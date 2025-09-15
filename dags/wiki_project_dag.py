import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

TZ = pendulum.timezone("UTC")
PAGENAMES = {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}
LAG_CANDIDATES = [2, 3, 4, 5, 6]  # hours behind to try, in order

def _download_pageviews(lag_candidates, **context):
    """
    Try a sequence of lagged hours (2..6h back) until one exists.
    Download to /tmp/wikipageviews_{KEY}.gz and return {"key": KEY}.
    KEY format: YYYYMMDDTHH0000 (UTC).
    """
    import requests, os
    base_dt = pendulum.instance(context["dag_run"].logical_date).in_timezone("UTC")
    for lag in lag_candidates:
        dt = base_dt - pendulum.duration(hours=lag)
        year = dt.strftime("%Y")
        year_month = dt.strftime("%Y-%m")
        day = dt.strftime("%Y%m%d")
        hour_chunk = dt.strftime("%H0000")
        key = dt.strftime("%Y%m%dT%H0000")

        url = (
            f"https://dumps.wikimedia.org/other/pageviews/"
            f"{year}/{year_month}/pageviews-{day}-{hour_chunk}.gz"
        )
        out_path = f"/tmp/wikipageviews_{key}.gz"

        try:
            r = requests.get(url, timeout=60, stream=True)
            if r.status_code == 200:
                with open(out_path, "wb") as f:
                    for ch in r.iter_content(8192):
                        if ch:
                            f.write(ch)
                return {"key": key}
        except requests.RequestException:
            pass  # try next lag

    raise AirflowException(
        f"No pageview file available for lags {lag_candidates} behind {base_dt}."
    )

def _gunzip_file(key: str, **_):
    import gzip, shutil, os
    gz = f"/tmp/wikipageviews_{key}.gz"
    out = f"/tmp/wikipageviews_{key}"
    if not os.path.exists(gz):
        raise AirflowException(f"Missing gz file: {gz}")
    with gzip.open(gz, "rb") as fin, open(out, "wb") as fout:
        shutil.copyfileobj(fin, fout)
    return out  # path (not used later, but handy for debugging)

def _fetch_pageviews(pagenames, key: str, **context):
    """
    Read /tmp/wikipageviews_{key}, aggregate views for PAGENAMES,
    and return SQL string to load MySQL. We store rows under the DAG's logical hour (context['ts']).
    """
    ts = context["ts"]  # logical run hour, ISO string
    input_path = f"/tmp/wikipageviews_{key}"
    result = dict.fromkeys(pagenames, 0)

    with open(input_path, "r") as f:
        for line in f:
            parts = line.rstrip("\n").split(" ")
            if len(parts) < 4:
                continue
            domain_code, page_title, view_counts = parts[0], parts[1], parts[2]
            if domain_code == "en" and page_title in pagenames:
                try:
                    result[page_title] = int(view_counts)
                except ValueError:
                    result[page_title] = 0

    stmts = [f"DELETE FROM pageview_counts WHERE datetime='{ts}';"]
    for pagename, views in result.items():
        stmts.append(
            "INSERT INTO pageview_counts(page, views, datetime) VALUES ("
            f"'{pagename}', {views}, '{ts}');"
        )
    return "\n".join(stmts)

with DAG(
    dag_id="wikipedia_pageviews_mysql",
    start_date=pendulum.datetime(2023, 1, 1, tz=TZ),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    description="Fetch Wikimedia hourly pageviews (lagged) and load into MySQL",
) as dag:

    # 0) Ensure table exists
    init_table = SQLExecuteQueryOperator(
        task_id="init_table",
        conn_id="mysql_default",
        database="my_db",
        autocommit=True,
        sql="""
        CREATE TABLE IF NOT EXISTS pageview_counts (
            page VARCHAR(255),
            views INT,
            datetime DATETIME
        );
        """,
    )

    # 1) Find a published hour (2..6 hours behind) & download it
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_download_pageviews,
        op_kwargs={"lag_candidates": LAG_CANDIDATES},
    )

    # 2) Gunzip it
    extract_gz = PythonOperator(
        task_id="extract_gz",
        python_callable=_gunzip_file,
        op_kwargs={
            "key": "{{ ti.xcom_pull(task_ids='get_data')['key'] }}",
        },
    )

    # 3) Build SQL to load into MySQL (from the decompressed file)
    fetch_pageviews = PythonOperator(
        task_id="fetch_pageviews",
        python_callable=_fetch_pageviews,
        op_kwargs={
            "pagenames": PAGENAMES,
            "key": "{{ ti.xcom_pull(task_ids='get_data')['key'] }}",
        },
    )

    # 4) Execute the SQL (no temp .sql file)
    write_to_mysql = SQLExecuteQueryOperator(
        task_id="write_to_mysql",
        conn_id="mysql_default",
        database="my_db",
        autocommit=True,
        sql="{{ ti.xcom_pull(task_ids='fetch_pageviews') }}",
    )

    init_table >> get_data >> extract_gz >> fetch_pageviews >> write_to_mysql
