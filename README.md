# 🔷 Airline Delay Analysis — Databricks Delta Live Tables

![Databricks](https://img.shields.io/badge/Databricks-FF3621.svg?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366.svg?style=flat)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C.svg?style=flat&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-blue.svg?style=flat&logo=python&logoColor=white)
![Status](https://img.shields.io/badge/Status-Production-green.svg?style=flat)

> **This is one half of a two-repo system.**
> This folder contains all **Databricks DLT transformation logic** (Bronze → Silver → Gold).
> The AWS infrastructure that triggers and orchestrates this pipeline lives in the companion repo →
> [`Airline-Delay-Analysis-AWS-Lambda-Ingestion`](https://github.com/EdIrfan/Airline-Delay-Analysis-AWS-Lambda-Ingestion)

---

## 🧭 What Does This Repo Contain?

This is the **data transformation and quality layer** of the Airline Delay Analysis pipeline. Once AWS Step Functions triggers the Databricks Job (via the companion repo), this DLT pipeline takes over — reading raw flight data from S3, cleaning and validating it, and producing 7 analytics-ready Gold tables. It is completely dependent on the parent repo and takes values from it for refrence.

This layer owns everything from raw bytes to business-ready Delta tables.

---

## 🏗️ Full System Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                    COMPANION REPO  (AWS Side)                        │
│   CSV Upload → S3 → EventBridge → Step Functions → Launcher Lambda  │
│                                        └──► triggers Databricks Job │
└──────────────────────────────────────────────────────────────────────┘
                              │
                              │  REST API call with run_id
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  THIS FOLDER  (Databricks DLT Side)                  │
│                                                                      │
│   S3 Landing Bucket                                                  │
│       └──► BRONZE  (Auto Loader streaming ingest)                   │
│                 └──► SILVER  (transform + 11 quality rules)         │
│                           └──► GOLD  (7 analytics tables)           │
│                                   ├── Daily Airline Performance      │
│                                   ├── Route Performance              │
│                                   ├── Airport Statistics             │
│                                   ├── Monthly Trends                 │
│                                   ├── Delay Cause Analysis           │
│                                   ├── Cancellation Analysis          │
│                                   └── Day of Week Performance        │
└──────────────────────────────────────────────────────────────────────┘
                              │
                              │  Checker Lambda polls status every 60s
                              ▼
                    ┌─────────────────┐
                    │  SUCCESS / FAIL │
                    │  → back to AWS  │
                    └─────────────────┘
```

---

## 🔗 Dependency on Companion Repo

This pipeline **cannot start without** the AWS orchestration defined in:
**[`Airline-Delay-Analysis-AWS-Lambda-Ingestion`](https://github.com/EdIrfan/Airline-Delay-Analysis-AWS-Lambda-Ingestion)**

| Companion Repo Does | This Folder Does |
|---|---|
| Detects CSV upload via EventBridge | — |
| Authenticates & triggers Databricks Job | Receives the job trigger |
| Polls job status every 60s (Checker Lambda) | Runs the DLT pipeline while being polled |
| Deletes S3 source file on success | — |
| Sends failure email via SNS | Reports status back via Databricks Jobs API |
| Manages all IAM, secrets, CI/CD | Uses the Databricks PAT stored in AWS Secrets Manager |

---

## 🥉🥈🥇 Medallion Architecture

### Bronze — Raw Ingest
- **Source:** S3 Landing Bucket (the CSV that triggered the pipeline)
- **Method:** Databricks Auto Loader (`cloudFiles`) — streaming ingest
- **Output:** `Flight_Delay_Bronze_dev` — immutable raw table, no transformations
- **Records:** ~6.5M per yearly CSV

### Silver — Transform + Validate
- **Source:** Bronze table via `dlt.read_stream()`
- **Method:** 22 column renames, type casting, derived fields
- **Quality:** `@dlt.expect_all_or_drop(Rules.rules)` — drops rows failing ANY rule
- **Output:** `Flight_Delay_Silver_dev` — materialized table, clean data only
- **Records:** ~5.8M pass (~700K dropped by quality rules)

### Gold — Analytics Tables
- **Source:** Silver table via `dlt.read()` (batch, not streaming)
- **Method:** Aggregations, groupBys, window functions in PySpark
- **Output:** 7 Delta tables — full refresh on every pipeline run
- **Ready for:** BI tools, dashboards, ML models

---

## 📋 Silver Quality Rules (11 Total)

All 11 rules live in `utilities/Rules.py` and are applied as a single `@dlt.expect_all_or_drop` decorator. A row must pass **all** rules to be kept.

| Rule | What It Checks |
|---|---|
| `BOTH_AIRPORTS_PRESENT` | Origin and destination airport codes are not null |
| `ORIGIN_IS_NOT_DESTINATION` | Origin != Destination (no same-airport flights) |
| `FLIGHT_IDENTITY_PRESENT` | Airline carrier code and flight number are not null |
| `SCHEDULED_DEP_TIME_VALID` | Scheduled departure time is not null |
| `SCHEDULED_ARR_TIME_VALID` | Scheduled arrival time is not null |
| `SCHEDULED_ELAPSED_TIME_VALID` | Scheduled elapsed time > 0 |
| `ACTUAL_ELAPSED_TIME_VALID` | Actual elapsed time > 0 (if flight completed) |
| `AIR_TIME_VALID` | Air time > 0 (if flight completed) |
| `DEP_TIME_NOT_NULL` | Actual departure time present |
| `ARR_TIME_NOT_NULL` | Actual arrival time present |
| `CANCELLED_THEN_NO_TAKEOFF` | If cancelled, actual departure time must be null |

> **Design note:** Rules are written as *keep* conditions (TRUE = keep the row).
> This is the correct DLT pattern for `expect_all_or_drop`.

---

## 📂 Folder Structure

```
DLT_Root_DEV/
│
├── transformations_SourceCode/
│   ├── bronze/
│   │   └── Bronze_Ingest.py          # Auto Loader streaming ingest from S3
│   ├── silver/
│   │   └── Transformation_silver.py  # Column renames, transforms, quality rules applied
│   └── gold/
│       └── gold_layer.py             # 7 analytics tables via dlt.read() batch
│
├── utilities/
│   └── Rules.py                      # 11 DLT quality rules (expect_all_or_drop)
│
├── explorations/                     # Scratch notebooks, ad-hoc analysis, prototypes
│
└── README.md
```

---

## 🥇 Gold Tables Reference

| Table Name | Description | Key Dimensions |
|---|---|---|
| `Gold_Daily_Airline_Performance` | Daily on-time stats per carrier | Date, Carrier, Avg Delay |
| `Gold_Route_Performance` | Performance by origin-destination pair | Route, Delay Cause |
| `Gold_Airport_Statistics` | Airport-level departure/arrival stats | Airport Code, Volume |
| `Gold_Monthly_Trends` | Month-over-month delay trends | Month, Year, Carrier |
| `Gold_Delay_Analysis` | Delay cause breakdown | Cause Category, Minutes |
| `Gold_Cancellation_Analysis` | Cancellation rates and reasons | Carrier, Reason Code |
| `Gold_Day_Of_Week_Performance` | Day-of-week patterns | DayOfWeek, Avg Delay |

> All Gold tables run as **full refresh** on every pipeline execution via `dlt.read()` batch mode.

---

## 🔧 DLT Pipeline Config

The Launcher Lambda in the companion repo updates these fields before each run via `PUT /api/2.0/pipelines/{id}`:

| Config | Value |
|---|---|
| Pipeline ID | `0a32c353-41d8-4458-8f45-ab05732f98e8` |
| Databricks Host | `https://dbc-0d07525d-6776.cloud.databricks.com` |
| Job ID | `965835917341782` |
| Source path | S3 Landing Bucket (injected per-run) |
| Mode | Triggered (not continuous) |

---

## 📊 Pipeline Stats

| Metric | Value |
|---|---|
| Raw records ingested (Bronze) | ~6.5 M per year |
| Records passing quality rules (Silver) | ~5.8 M |
| Records dropped by quality rules | ~700 K (~10%) |
| Quality rules enforced | 11 |
| Gold analytics tables | 7 |
| Silver column renames | 22 |
| Pipeline mode | Triggered (full refresh per run) |

---

## 🤝 Related

- **Companion repo (AWS orchestration):** [`Airline-Delay-Analysis-AWS-Lambda-Ingestion`](https://github.com/EdIrfan/Airline-Delay-Analysis-AWS-Lambda-Ingestion)
- **Databricks workspace:** `https://dbc-0d07525d-6776.cloud.databricks.com`
- **Pipeline type:** Delta Live Tables (DLT)
- **Storage format:** Delta Lake