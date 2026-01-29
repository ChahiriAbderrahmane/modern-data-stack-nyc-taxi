<div align="center">
  <h1>🚖 NYC Taxi Data Engineering Platform</h1>
  <h3>End-to-End ELT Pipeline | Data Warehouse | BI & API Microservices</h3>

  <p>
    An enterprise-grade Data Engineering project transforming raw NYC Taxi data into actionable insights 
    via a modern stack: <strong>Airflow, dbt, PostgreSQL, FastAPI, Power BI, and Slack</strong>.
  </p>

  <img src="https://img.shields.io/badge/Orchestration-Apache%20Airflow-blue?style=for-the-badge&logo=apacheairflow" />
  <img src="https://img.shields.io/badge/Transformation-dbt%20Core-FF694B?style=for-the-badge&logo=dbt" />
  <img src="https://img.shields.io/badge/Database-PostgreSQL-336791?style=for-the-badge&logo=postgresql" />
  <img src="https://img.shields.io/badge/API-FastAPI-009688?style=for-the-badge&logo=fastapi" />
  <img src="https://img.shields.io/badge/BI-Power%20BI-F2C811?style=for-the-badge&logo=powerbi" />
  <img src="https://img.shields.io/badge/Alerting-Slack-4A154B?style=for-the-badge&logo=slack&logoColor=white" />
  <img src="https://img.shields.io/badge/Container-Docker-2496ED?style=for-the-badge&logo=docker" />
</div>

# **Project Architecture**
![Project Architecture](assets/NYC%20Taxi%20Data%20Engineering%20Plateform.png)

<br>

## 📝 Table of Contents
1. [Project Overview](#overview)
2. [Architecture & Data Modeling](#architecture)
3. [Business Intelligence (Dashboards)](#bi)
4. [Orchestration (Airflow)](#airflow)
5. [Data Products (API)](#api)
6. [Observability & Alerting](#quality)
7. [Performance & Optimization](#perf)
8. [Installation](#install)
9. [ Contact ](#contact)

<hr>

<a name="overview"></a>
## 🔭 Project Overview

This project simulates a real-world data platform for a Taxi company. It ingests high-volume trip data, cleanses it, models it into a Star Schema, and serves it to different stakeholders (Executives, Operations, Finance) via Dashboards and APIs.

**Key Features:**
* **ELT Pipeline:** Ingestion of raw CSVs into Bronze/Silver/Gold layers using **dbt** and **Postgres**.
* **Data Quality:** Automated testing and "Revenue at Risk" calculation to detect anomalies (negative fares, time travel).
* **Microservice API:** A standalone **FastAPI** container serving Gold data to external apps.
* **Observability:** Slack alerting for data quality breaches.

<a name="architecture"></a>
## 🏗️ Architecture & Data Modeling

The project follows the Medallion Architecture (Bronze -> Silver -> Gold).

### The Star Schema (Gold Layer)
I transformed the data into a rigorous dimensional model optimized for BI performance.
<div align="center">
  <img src="assets/data_star_model.png" alt="Star Schema" width="800">
  <p><em>Entity Relationship Diagram (ERD) generated from the Gold Layer.</em></p>
</div>

### Aggregations for BI
To handle millions of rows efficiently in Power BI, specific Data Marts aggregate views were created with dbt.
The "_Key Measures" table was created in powerbi to gather the measures created with DAX code.
<img src="assets/agg_power_bi.png" alt="Aggregation Tables" width="800">

<a name="bi"></a>
## 📊 Business Intelligence (Power BI)

The final product is a comprehensive Power BI Report (`.pbit`) containing 4 specialized views.

### 1. Executive Pulse (C-Level)
*Focus: Year-over-Year growth, Total Revenue, and High-level trends.*
![Executive Dashboard](https://github.com/ChahiriAbderrahmane/modern-data-stack-nyc-taxi/blob/v1.0-stable/assets/Executive%20Pulse%20dashboard.png)

### 2. Operations & Traffic (Fleet Managers)
*Focus: Filled map, Borough-to-Borough flow, and RPM (Revenue Per Minute) optimization.*
![Ops Dashboard](https://github.com/ChahiriAbderrahmane/modern-data-stack-nyc-taxi/blob/v1.0-stable/assets/Op%C3%A9rations%20%26%20Trafic%20dashboard.png)

### 3. Financial Performance (Finance Depatement)
*Focus: Payment methods adoption (Cash vs Card), Tipping behavior, and Fare buckets.*
![Finance Dashboard](https://github.com/ChahiriAbderrahmane/modern-data-stack-nyc-taxi/blob/v1.0-stable/assets/Financial%20Performance%20%26%20Spending%20Patterns%20dashboard.png)

### 4. Data Quality Monitor (Data Engineering Team)
*Focus: Pipeline health, Invalid records tracking, and Revenue at Risk ($).*
![Quality Dashboard](https://github.com/ChahiriAbderrahmane/modern-data-stack-nyc-taxi/blob/v1.0-stable/assets/Data%20Quality%20Report%20dashboard.png)

### **Feature Highlight:** Tooltips allow users to hover over data points for granular details. It works only in the first dashboard, in the line chart. 
> ![Tooltip](https://github.com/ChahiriAbderrahmane/modern-data-stack-nyc-taxi/blob/v1.0-stable/assets/Tooltip%20dashboard.png)

<a name="airflow"></a>
## 🌪️ Orchestration (Apache Airflow)

The entire pipeline is orchestrated via **Astro CLI** (Airflow).

### The Main Pipeline
Handles the end-to-end flow: `dbt run` (Bronze/Silver/Gold), `dbt test`, and data freshness checks.
![Main DAG](assets/main_dag_graph.png)

### Static Dimensions & Utility DAGs
Separate DAG to manage static data to optimize runtime.

![static dimensions](assets/static_dimensions_dag.png)

<a name="api"></a>
## 🚀 Data Products: FastAPI Microservice

Beyond dashboards, this project exposes a REST API for application developers.
The API runs in an isolated Docker container but communicates with the same Data Warehouse.

* **Endpoint:** `/metrics/daily` (Supports date filtering)
* **Architecture:** Dockerized FastAPI service networked with Postgres.

![FastAPI Response](assets/fast_api_response.png)

<a name="quality"></a>
## 🚨 Observability & Alerting

I implemented a **Reverse ETL** logic to proactively notify the team when Data Quality degrades.
If the **Revenue at Risk** exceeds a threshold (e.g., $10k), a Slack alert is triggered automatically.

**Alerting DAG**
![Alerting DAG](assets/slack_dag.png)
**Slack Alert Message**
![Slack Alert](assets/slack_alert_message.png)

<a name="perf"></a>
## ⚡ Performance & Optimization

I optimized the pipeline architecture by **decoupling static data processing** from the daily workflow.

Initially, the DAG was monolithic, rebuilding all Dimensions and Facts on every run.
**Strategy:** I extracted static dimensions into a separate DAG (`static_dimensions_dag`) that runs only on-demand, leaving the main pipeline to process only new incoming trip data.

| Before Optimization | After Optimization |
| :---: | :---: |
| ![Before](assets/runing_duration_before.png) | ![After](assets/runing_duration_after.png) |
| **Monolithic DAG:**<br>Rebuilding static dimensions & facts every time.<br>*(High Latency)* | **Decoupled Architecture:**<br>Static dims separated.<br>Only processing new data.<br>*(Drastic reduction in runtime)* |

### **Airlfow ui dags**
![Airlfow ui dags](assets/airflow_dags_ui_airflow.png)

<a name="install"></a>
## 💻 How to Run

### Prerequisites
* Docker & Docker Compose
* Astro CLI
* Power BI Desktop (to view `.pbit`)

### Steps

1. **Clone the repository**
   ```bash
   git clone [https://github.com/ChahiriAbderrahmane/modern-data-stack-nyc-taxi.git)
   ````

2. **Start the Data Platform (Airflow + Postgres)** 
    ```bash
    astro dev start
    ```

3.  **Start the API Microservice**

    ```bash
    docker compose -f docker-compose-api.yml up --build
    ```

4.  **Access the Interfaces**

      * **Airflow:** `http://localhost:8080`
      * **FastAPI Docs:** `http://localhost:8000/docs`
      * **Power BI:** Open `assets/nyc_project_dashboard.pbit`


<a name="contact"></a>
## 📨 Contact Me

[LinkedIn](https://www.linkedin.com/in/abderrahmane-chahiri-151b26237/) •
[Gmail](mailto:chahiri.abderrahmane.eng@gmail.com)

<div align="center">
Made with ❤️ by <a href="https://www.linkedin.com/in/chahiri-abderrahmane/">Abderrahmane Chahiri</a>
</div>

