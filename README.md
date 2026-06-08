# ☄️ NASA Asteroid ETL Pipeline
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-green?logo=apache-airflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
## Introduction

This project builds an **ETL pipeline** to collect and process **Near-Earth Asteroids (NEOs)** data from NASA’s public API. The data is fetched daily in near real-time.

The pipeline performs the following steps:

- Extract data from NASA API  
- Process complex JSON data  
- Convert data into CSV format  
- Validate and clean data  
- Load data into PostgreSQL  
- Calculate asteroid risk scores  
- Automate the pipeline using Apache Airflow and run the environment with Docker
- Visualize the data using SQL query with Grafana

Data source:  
NASA Near Earth Object Web Service API  
https://api.nasa.gov/

---

# Project Structure

```
API provides information about asteroids approaching Earth.

nasa_asteroid_ETL_Project
│
├── dags/
│   └── Asteroid_DAG.py
│
├── scripts/
│   ├── Extract_API.py
│   ├── flatten_data.py
│   ├── transform_data.py
│   ├── validate.py
│   ├── load_postgres.py
│   └── danger_score.py
│   └── db_log.py
│
├── data/
│   ├── raw/
│   ├── flatten/
│   └── processed/
│   └── sql/
│        └── init.sql
│        └── ignored.sql
│        └── log.sql
│
├── logs/
│
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
└── .env
```

---

# Data Pipeline Flow

```
The pipeline processes data in the following order:

NASA API
   ↓
Extract JSON data
   ↓
Flatten data (nested JSON → tabular format)
   ↓
Transform → CSV
   ↓
Clean and transform data → CSV
   ↓
Validate before load → Database
   ↓
Load into PostgreSQL
   ↓
Calculate risk_score
```

---

# Risk Score Formula

The risk score is calculated based on:

- Maximum diameter  
- Velocity  
- Distance to Earth  

```
risk_score =
(diameter_max_m / 1000) * 4
+ (velocity_km_s / 30) * 3
+ (7500000 / miss_distance_km) * 3
```

### Danger Level Classification

| Risk Score | Danger Level |
|------------|-------------|
| ≥ 8        | EXTREME     |
| ≥ 6        | HIGH        |
| ≥ 4        | MEDIUM      |
| < 4        | LOW         |

# Technologies/Libraries Used

- Python
- Pandas
- PostgreSQL  
- Apache Airflow  
- Docker  
- Loguru
- Psycopg2 
- Grafana  

---

# How to Run the Project

Run the environment using Docker:

```bash
docker compose up
```

Access the Airflow UI:

```
http://localhost:8181
```

Access the Grafana UI:

```
http://localhost:3001
```

Default login credentials:

```
username: admin
password: admin
```

Trigger the DAG to run the entire pipeline:
<img width="1832" height="300" alt="Screenshot 2026-05-20 232049" src="https://github.com/user-attachments/assets/6432a0fd-c0ea-40c2-b398-facb1e162333" />

Visualize data using SQL at Grafana webserver:
<img width="1855" height="963" alt="Screenshot 2026-05-20 232450" src="https://github.com/user-attachments/assets/815fed10-b320-4a9c-933f-07b90f12e9fe" />



### Database Connection

```
Register Server:

- host: localhost
- port: 5433
- maintenance database: airflow
- username: airflow
- password: 123
```

---

# Results

Data is stored in PostgreSQL database **nasa_neo** with the following tables:

<img width="167" height="122" alt="Screenshot 2026-05-20 232904" src="https://github.com/user-attachments/assets/5abc9c5b-98df-43c5-8f5b-b2d12b073cb5" />


### Table Columns
**danger_level:**
```
asteroid_id
name
absolute_magnitude
diameter_min_m
diameter_max_m
velocity_km_s
miss_distance_km
risk_score
danger_level
date
```
```
**etl_log:**
```
run_id
step
status
records_processed
message
logged_at
```
**ignored:**
```
asteroid_id
name
diameter_max_m
velocity_km_s
miss_distance_km
risk_score
danger_level
reject_reason
```
---

# Logging

Pipeline logs are stored in:

```
log file: logs/
database: etl_log table at Postgres
```

API → Python → CSV → Postgres → Airflow (daily batch)
