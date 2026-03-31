import os
import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models.param import Param
from airflow.exceptions import AirflowFailException

logger = logging.getLogger("airflow.task")

DAGS_FOLDER = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
PROJECT_ROOT = os.path.join(DAGS_FOLDER, "dags", "set_up_database")
DDL_BASE_DIR = os.path.join(PROJECT_ROOT, "ddls")
CSV_FILE_PATH = os.path.join(PROJECT_ROOT, "Airline Dataset.csv")

@dag(
    dag_id='snowflake_setup',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "load_data": Param("none", type="string", enum=["none", "yes", "no"], 
                          description="Type 'none' to Fail | 'yes' to Build+Load | 'no' to Build only")
    }
)
def snowflake_pipeline():

    sql_tasks = []
    first_task_id = None

    @task
    def execute_sql(file_path: str):
        with open(file_path, 'r') as f:
            hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
            hook.run(f.read(), autocommit=True)
        logger.info(f"Executed: {os.path.basename(file_path)}")

    if os.path.exists(DDL_BASE_DIR):
        folders = sorted(os.listdir(DDL_BASE_DIR))
        previous_task = None
        
        for folder in folders:
            files = sorted([f for f in os.listdir(os.path.join(DDL_BASE_DIR, folder)) if f.endswith('.sql')])
            for f in files:
                t_id = f"run_{folder}_{f.replace('.sql','')}"
                if first_task_id is None:
                    first_task_id = t_id 
                
                exec_sql = execute_sql.override(task_id=t_id)(file_path=os.path.join(DDL_BASE_DIR, folder, f))
                
                if previous_task is not None:
                    previous_task >> exec_sql
                
                previous_task = exec_sql
                sql_tasks.append(exec_sql)

    @task.branch
    def initial_validation(**context):
        val = context['params'].get('load_data')
        if val == "none":
            raise AirflowFailException("Selection required: Choose 'yes' or 'no'.")
        return first_task_id

    @task
    def upload_csv_to_snowflake():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        if not os.path.exists(CSV_FILE_PATH):
            raise FileNotFoundError(f"CSV not found at {CSV_FILE_PATH}")
        with hook.get_conn() as conn:
            conn.cursor().execute(f"PUT 'file://{CSV_FILE_PATH}' @AIRLINE_DWH.RAW.STG_AIRLINE OVERWRITE = TRUE")

    @task
    def run_data_load_procedures():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        hook.run("CALL AIRLINE_DWH.RAW.SP_LOAD_TRAVEL_RAW();")
        hook.run("CALL AIRLINE_DWH.CLEAN.SP_LOAD_TRAVEL_CLEAN();")
        hook.run("CALL AIRLINE_DWH.DWH.SP_LOAD_FACT_FROM_STREAM();")

    @task
    def skip_data_load():
        logger.info("Infrastructure only. Skipping load.")

    @task.branch
    def data_decision(**context):
        if context['params'].get('load_data') == "yes":
            return "upload_csv_to_snowflake"
        return "skip_data_load"


    validate = initial_validation()
    decision = data_decision()
    upload = upload_csv_to_snowflake()
    load_proc = run_data_load_procedures()
    skip = skip_data_load()


    validate >> sql_tasks[0]
    

    sql_tasks[-1] >> decision
    

    decision >> [upload, skip]
    upload >> load_proc

snowflake_pipeline()