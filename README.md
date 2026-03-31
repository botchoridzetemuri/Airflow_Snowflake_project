# Snowflake & Airflow Automated Data Pipeline

## 📖 Project Overview
This project is an end-to-end Data Engineering pipeline that integrates **Apache Airflow** (running in Docker) with **Snowflake**. 

The pipeline automatically builds a Snowflake Data Warehouse from scratch, uploads local data directly to a Snowflake internal stage, and executes stored procedures to load and transform the data into Fact and Dimension tables.


## 🏗️ Pipeline Architecture & Workflow
The Airflow DAG (`snowflake_setup_dag`) is fully parameterized and generates tasks dynamically by reading `.sql` files from a structured directory. 

### Execution Variants (Parameterized Runs)
The DAG requires user input before running, acting as a defensive guardrail against accidental compute usage.

1. **`none` (Default):** The pipeline hits an `initial_validation` guardrail and **fails immediately**. This protects the Snowflake environment from blind executions.
2. **`yes`:** The full CI/CD-style deployment. 
    * Builds all Snowflake infrastructure (Databases, Schemas, Tables, Streams).
    * Uses a Snowflake `PUT` command via Airflow to upload `Airline Dataset.csv` to an internal stage.
    * Calls Stored Procedures to load the Raw, Clean, and Fact/Dimension tables.
3. **`no`:** Infrastructure only.
    * Safely rebuilds or verifies all Snowflake objects using idempotent SQL (`IF NOT EXISTS` / `CREATE OR REPLACE`).
    * Gracefully skips the data upload and stored procedures to save compute credits.

## 📂 Project Structure
```text
dags/set_up_database/
├── ddls/                      # Folder containing all SQL scripts
│   ├── 01_setup/              # Creates DB, Schema, Stages
│   ├── 02_tables/             # Creates Raw, Clean, Fact, and Dim tables
│   ├── 03_streams/            # Creates Snowflake Streams for CDC
│   ├── 04_procedures/         # Creates Stored Procedures for data loading
│   └── 05_security/           # Creates Roles and Row Access Policies
├── Airline Dataset.csv        # Source data file
├── snowflake_setup_dag.py     # Main Airflow DAG