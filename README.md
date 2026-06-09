# 🏛️ Project Sumé: Brazilian Legislative Data Infrastructure & Analytics Platform
### A Scalable Data Engineering Pipeline & BI Engine Built with PySpark, PostgreSQL, and Python

---

## 🚀 Project Overview
Project Sumé is an end-to-end data platform engineered to extract, clean, transform, and analyze legislative data from the Brazilian Chamber of Deputies (`dadosabertos.camara.leg.br`). 

The platform implements an automated ETL/ELT pipeline that moves data from raw public REST endpoints into a multi-tiered relational data warehouse framework. By leveraging distributed computing frameworks (PySpark) alongside relational databases (PostgreSQL), the project exposes deep analytical insights regarding political alignments, speech transcript keywords, ideological concentrations, and demographic diversity over time.

---

## 🛠️ Technical Architecture & Stack
* **Distributed Processing Engine:** Apache PySpark (Spark SQL & DataFrames)
* **Data Warehouse Cluster:** PostgreSQL (Multi-Schema Layout: Staging/Staging-Silver)
* **Programming & ETL Layer:** Python 3 (`pandas`, `requests`, `SQLAlchemy`, `psycopg2`)
* **Environment & Secret Security:** `python-dotenv` for configuration isolation
* **Project Packaging:** `setuptools` (Modular `src` package generation)

---

## 📐 Data Infrastructure Workflow

### 1. Extraction & Ingestion Layer (`/src` & Ingestion Scripts)
Automated Python modules map out parameters across historical legislative cycles, requesting nested JSON objects from public APIs and writing them as tables to a dedicated database:
* `get_legislaturas.py`: Seeds historical legislative timeline thresholds.
* `get_deputados.py` & `get_deputados_detalhes.py`: Extracts biographical records, party affiliations, and operational cabinet metrics.
* `project_utils.py`: Contains centralized database engine connection mapping and custom helper scripts.

### 2. PySpark Distributed Transcript Pipeline (`get_discursos.py`)
To prevent memory exhaustion when processing heavy text string fields (speech transcriptions and summary keywords), the pipeline scales out using PySpark:
* Initializes a local Spark Session loading the native PostgreSQL JDBC driver.
* Enforces strong type typing using explicit Spark schemas (`StructType`, `StructField`).
* Utilizes a custom recursive function `flatten_df_struct_columns_to_root()` to dynamically unpack nested JSON fields down to root-level columns.

### 3. Medallion Database Architecture
Data is processed through isolated database namespaces within PostgreSQL to ensure data lineage and integrity:
* **`deputados` Schema (Staging Layer):** Stores raw, unmodified API outputs, acting as an immutable land repository.
* **`silver` Schema (Enriched Layer - `pipeline_depurados.py`):** Runs optimized SQL queries executing complex multi-table `LEFT JOIN` operations across tracking data and structural political alignments (`alinhamento_partidos`), normalizing values into analytical models.

---

## 📊 Analytical Insights & Portfolio Findings
The processed data layers are analyzed using localized Jupyter notebooks to answer core questions regarding political landscapes:

### 1. Political Alignment & Ideological Concentration (`analise_cenario_camara_de_daputados.ipynb`)
* **Power Dilution:** Analysis indicates an active fragmentation of power within the Chamber of Deputies, highlighted by a steady rise in the number of active minor parties alongside a proportional decline in historical major party dominance.
* **The Rise of the "Centrão":** Documented how cross-party ideological coalitions (such as the "Centrão" and right-wing blocks) organize to gain majority control, compensating for the lack of single-party voting blocks.

### 2. Speech & Keyword Discourse Analysis (`analise discusso dos deputados.ipynb`)
* **Proportional Representation in Debates:** Discovered a direct correlation between total party size and speech frequency allocation, pointing to strict implicit constraints on individual debate times.
* **Crisis Reflection:** Text analysis of the 2019 legislative cycle caught an overwhelmingly dominant focus on pandemic-related keywords (COVID-19) alongside tactical procedural maneuvers.

### 3. Demographic Evolution Tracking (`analise_deputados.ipynb`)
* **Gender Disparity:** Evaluated female political representation over time. Despite an increase in the last two legislative cycles, historical tracking shows that women hold a small fraction of seats, contrasting significantly with national census metrics.

---

## 📦 Local Deployment & Setup Guide

### 1. Prerequisites
Ensure your machine has **Apache Spark** installed with the PostgreSQL JDBC driver jar configured, along with a running **PostgreSQL** instance.

### 2. Environment Configuration
Create a `.env` file in the project root folder to map your local connection strings:
```text
host=localhost
port=5432
username=postgres
password=your_secure_password