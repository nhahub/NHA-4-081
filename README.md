# 🎮 Steam Analytics Engine
### End-to-End Big Data ETL Pipeline with Machine Learning

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169e1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

> A live, automated Big Data pipeline that continuously extracts, transforms, and loads the Steam gaming marketplace into a Medallion Architecture data warehouse — extended with a Random Forest Game Success Prediction model.

> **Note:** This project is built as an **On-Premise Containerized Pipeline**, architected to allow a seamless migration to Microsoft Azure.

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Architecture & Pipeline Flow](#-architecture--pipeline-flow)
- [Technical Highlights](#-technical-highlights)
- [Data Model](#-data-model-star-schema)
- [Game Success Prediction Model](#-game-success-prediction-model)
- [Power BI Dashboard](#-power-bi-dashboard)
- [Project Deliverables](#-project-deliverables)
- [How to Run Locally](#-how-to-run-locally)
- [Future Roadmap](#-future-roadmap-azure-cloud-migration)
- [Team](#-team)

---

## 🌟 Project Overview

The Steam gaming marketplace hosts over **180,000 titles** generating a continuous stream of pricing, player counts, reviews, and sentiment data. This project builds a production-grade Big Data pipeline to make sense of it all in real time.

| Metric | Value |
|---|---|
| Total Games Processed | 42,980 |
| Total Reviews | 134,000,000 |
| Total Recommendations | 89,000,000 |
| Average Game Price | $7.11 USD |
| Star Schema Tables | 7 |
| Pipeline Schedule | Hourly (Automated) |

---

## 🏗️ Architecture & Pipeline Flow

The project strictly follows the **Medallion Architecture**, orchestrating batch processing via Apache Airflow.

```mermaid
flowchart LR
    A[Steam Web API] -- Incremental Fetch --> B[(Bronze: JSON)]
    B -- PySpark Explode & Dedupe --> C[(Silver: CSVs)]
    C -- PySpark JDBC Upsert --> D[(Gold: PostgreSQL)]
    D -- Direct Query --> E[Power BI Dashboard]
    D -- Feature Extraction --> F[Random Forest Model]

    subgraph Apache Airflow Orchestration
        A
        B
        C
        D
    end

    style A fill:#171a21,stroke:#66c0f4,stroke-width:2px,color:#fff
    style B fill:#cd7f32,stroke:#8b5a2b,stroke-width:2px,color:#fff
    style C fill:#c0c0c0,stroke:#808080,stroke-width:2px,color:#000
    style D fill:#ffd700,stroke:#daa520,stroke-width:2px,color:#000
    style E fill:#f2c811,stroke:#000,stroke-width:2px,color:#000
    style F fill:#2e7d32,stroke:#1b5e20,stroke-width:2px,color:#fff
```

### Bronze Layer — Raw Ingestion
- Connects to Steam Official Partner API (`GetAppList` & `AppDetails`)
- Uses an intelligent **Priority Queue** to download JSON metadata for fresh games
- Bypasses Steam rate limits and Cloudflare blocking via a local `game_registry.json`
- Raw JSON payloads persisted to local storage as timestamped batch files
- Asynchronous **Bronze Compaction** merges old batches into monthly archives every 3 days

### Silver Layer — Cleaning & Transformation
- **PySpark** reads all Bronze JSON files via wildcard pattern for distributed processing
- Dynamically explodes deeply nested JSON arrays (Achievements, DLCs, Genres, Categories)
- HTML tags stripped from all text fields using a registered PySpark UDF
- Duplicates dropped via composite-key logic and deduplication Window functions
- Produces **7 normalized tables** staged as CSV files

### Gold Layer — Business Ready
- **PySpark** runs JDBC operations to push Silver data into **PostgreSQL**
- Strict schema enforcement via `UnionByName` commits
- Reliable **Upserts** using Window deduplication — never wipes existing data
- Post-load validation compares Silver vs Gold row counts with 0.01% tolerance

---

## 🛠️ Technical Highlights

| Feature | Description |
|---|---|
| **Priority Queue Extraction** | Targets top-played games first, skips games updated within 24 hours, discovers new games from 180,000+ catalog |
| **Bronze Compaction** | Merges hourly batch files into monthly archives using streaming line-by-line writer — prevents OOM spikes |
| **Schema Enforcement** | Forces PySpark to align column types precisely against PostgreSQL schema before `UnionByName` commits |
| **Composite Key Upserts** | Each table uses its own composite key (e.g. AppID + Genre_ID) — prevents data collapse on repeated runs |
| **Airflow ShortCircuit** | `compact_gate` uses `ShortCircuitOperator` to decouple 3-day compaction from hourly extraction |
| **Pipeline Test Suite** | Standalone integrity tests validate Bronze JSON → Silver CSV → Gold PostgreSQL after every run |
| **Complete Dockerization** | Custom Airflow image installs Java 17 and PySpark at build time — single `docker-compose up` launches everything |

---

## 📊 Data Model (Star Schema)

The destination PostgreSQL database is organized into a Star Schema optimized for Power BI reporting.

- **`games_main`** (Fact Table) — 48 columns covering Financials, Player Counts, Reviews, Base Stats
- **`games_genres`** — Exploded genre bridge table
- **`games_categories`** — Exploded category bridge table
- **`games_achievements`** — Highlighted achievements per game
- **`games_dlc`** — DLC AppID references
- **`games_screenshots`** — Full and thumbnail screenshot URLs
- **`games_movies`** — Trailer and video metadata

![Database Schema](images/schema.png)

---

## 🤖 Game Success Prediction Model

An interactive **Random Forest** classification model that predicts whether a game will succeed on the Steam platform based on its specifications.

| Component | Detail |
|---|---|
| Algorithm | LightGBM Regression |
| Training Data | Gold layer `games_main` table |
| Input Features | Price, Discount %, Total Reviews, Recommendations, Achievements, DLC Count, Category Tags, Release Date, Genre |
| Target Variable | Binary — Successful / Not Successful |
| Output | Predicted review score + Sentiment bucket + Feature importance |

### How It Works
1. Enter the game's spec sheet — Price, Reviews, Recommendations, Achievements, Genre, and more
2. Click **Run Prediction**
3. The model returns the predicted review score, the sentiment tier, and which features are driving the result

### Feature Importance
The model ranks which inputs have the most impact on the prediction:
- **Reviews** and **Price** are the strongest predictors
- **Achievements** and **Recommendations** follow closely
- **Genre**, **Release Date**, and **Discount** have moderate influence

![Game Success Prediction Model](images/model1.png.jpeg)
---

## 📈 Power BI Dashboard

The reporting layer features a dark-mode Power BI dashboard designed for market analysis and sentiment tracking.

### Market Overview & Game Reception
![Page 1: Market Overview](images/dashboard_market_overview.png)

### Developer & Tag Performance
![Page 2: Developer & Tag Analysis](images/dashboard_developer_analysis.png)

### Airflow DAG Execution
![Airflow DAG](images/airflow_v2_dag.png)

---

## 📁 Project Deliverables

<table>
  <tr>
    <td align="center" width="50%">
      <b>📄 Project Documentation</b><br><br>
      <img src="images/doc_image.png" width="100%"/>
      <br>Full academic report covering all pipeline layers, data model, and ML model
    </td>
    <td align="center" width="50%">
      <b>📊 Project Presentation</b><br><br>
      <img src="images/persentaion_image.png" width="100%"/>
      <br>DEPI graduation project presentation slides
    </td>
  </tr>
</table>

---

## 🚀 How to Run Locally

**Prerequisites:** Docker Desktop, Steam API Key (free from [steamcommunity.com/dev/apikey](https://steamcommunity.com/dev/apikey))

**1. Clone the repository**
```bash
git clone https://github.com/your-repo/steam-etl-pipeline.git
cd steam-etl-pipeline
```

**2. Configure environment variables**
```bash
cp .env.example .env
# Edit .env and add your STEAM_API_KEY and database credentials
```

**3. Build and launch the containers**
```bash
docker-compose build airflow-webserver airflow-scheduler
docker-compose up -d
```

**4. Start the pipeline**
- Navigate to `localhost:8089` (Airflow UI)
- Unpause the DAG `steam_hourly_pipeline`
- Watch the tasks execute interactively

**5. Open the dashboard**
- Open `Dashboard.pbix` in Power BI Desktop
- Import `Steam_Dark_Theme.json` via View → Themes for dark-mode styling

---

## ☁️ Future Roadmap: Azure Cloud Migration

This pipeline was designed with cloud migration in mind. Local Docker components map directly to Azure services:

| Current (On-Premise) | Azure Equivalent |
|---|---|
| Local JSON Storage (Bronze/Silver) | Azure Data Lake Storage Gen2 (ADLS) |
| Apache Spark via Docker | Azure Databricks (PySpark) |
| PostgreSQL via Docker | Azure Database for PostgreSQL |
| Apache Airflow via Docker | Azure Data Factory (ADF) |

Additional planned enhancements:
- **Real Time Streaming** — Apache Kafka or Azure Event Hubs for live player count streaming
- **SCD Type 2 Price Tracking** — Delta Lake to track historical game price changes
- **SteamSpy API Integration** — Estimated revenue and player demographic enrichment
- **Expanded ML Model** — Additional features for higher prediction accuracy

---

## 👥 Team

**DEPI Data Engineering Graduation Project — July 2026**

| Name | Role |
|---|---|
| Mohamed Ahmed Arafa | Project Introduction & Architecture |
| Ahmed Samir Saad | Bronze & Silver Pipeline |
| Mohamed Abdelhakim | Gold Layer, Dashboard & Orchestration |
| Jamal Hany Jamal | ML Model — Design & Training | 
| Yousif Amin | ML Model — Evaluation & Results |

**Supervisor:** Mohamed Hamed

---


# Review Score Engine

An interactive web application and API that uses a LightGBM regression model to predict a game's Steam review score and sentiment bucket based on its specifications. 

The system provides live model estimates alongside a detailed breakdown of feature importance, highlighting exactly what variables drive the prediction[cite: 1, 2].

## Project Structure

```text
├── vercel.json           # Vercel routing and serverless function configurations
├── index.html            # Frontend user interface
├── requirements.txt      # Python dependencies[cite: 4]
└── api/
    ├── index.py          # FastAPI application backend[cite: 2]
    └── model/
        └── rating_predictor_v3.pkl  # Trained LightGBM model artifact[cite: 2]


<p align="center">
  <img src="https://img.shields.io/badge/DEPI-Data%20Engineering%20Track-blue?style=for-the-badge"/>
  <img src="https://img.shields.io/badge/Status-Completed-green?style=for-the-badge"/>
  <img src="https://img.shields.io/badge/July-2026-orange?style=for-the-badge"/>
</p>
