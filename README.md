# Airflow-Wiki-Scraper
Starter project for airflow using astro-cli


# 📊 Wikipedia Pageviews ETL with Apache Airflow + MySQL

This project demonstrates an **ETL pipeline** built with [Apache Airflow](https://airflow.apache.org/) (via [Astronomer](https://www.astronomer.io/)) and MySQL running in containers (Podman).  
The DAG ingests hourly [Wikimedia pageview dumps](https://dumps.wikimedia.org/other/pageviews/), filters for selected pages, and writes results into a MySQL table for further analysis.

---

## 🚀 Features

- **Automated ETL with Airflow**  
  Orchestrates downloading, decompressing, transforming, and loading Wikipedia pageview data.

- **Resilient ingestion**  
  Handles Wikimedia’s publication lag by trying multiple past hours until a valid file is found.

- **Transformation step in Python**  
  Extracts hourly view counts for specific pages (Google, Amazon, Apple, Microsoft, Facebook).

- **Persistence layer**  
  Stores results in a MySQL database table `pageview_counts`.

- **Containerized setup**  
  Uses Podman to run both the Airflow (Astro) project and MySQL.

---

## 🏗 Project Structure
