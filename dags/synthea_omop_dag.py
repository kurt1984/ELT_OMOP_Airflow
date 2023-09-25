"""

## Run ELT on energy capacity data with Cosmos and dbt Core

Shows how to use the Cosmos, to create an Airflow task group from a dbt project.
The data is loaded into a database and analyzed using the Astro Python SDK. 
"""

from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
import logging
import os
import duckdb
from pendulum import datetime
import matplotlib.pyplot as plt
import numpy as np


task_logger = logging.getLogger("airflow.task")


CONNECTION_ID = "db_conn"
DB_NAME = "duckdb"
# SCHEMA_NAME = "postgres"
# The path to the dbt project
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/ETL-Synthea-dbt"
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"



profile_config = ProfileConfig(
    profile_name = "etl_synthea_dbt",
    target_name = "omop",

    profiles_yml_filepath = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/ETL-Synthea-dbt/profiles.yml"
)



 
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


@dag(
    start_date=datetime(2023, 3, 26),
    schedule=None,
    catchup=False,
)
def synthea_omop_dag():

    @task(task_id="load_csv_to_duckdb")
    def load_csv_to_duckdb():

        con = duckdb.connect(f'{DBT_PROJECT_PATH}/dbt.duckdb')

        synthea_tables = ["patients", "encounters", "providers", "conditions", "allergies", "observations", "procedures", "devices", "medications", "immunizations"]
        vocab_tables = ["concept", "concept_relationship", "concept_ancestor", "vocabulary"]

        for table in synthea_tables:
            con.sql(f'create table IF NOT EXISTS source.{table} as FROM "{DBT_PROJECT_PATH}/data/synthea/{table}.csv"')

        for table in vocab_tables:
            con.sql(f'create table IF NOT EXISTS cdm.{table} as FROM "{DBT_PROJECT_PATH}/data/vocab/{table.upper()}.csv"')

        con.table("source.encounters").show()

        con.close()

    load_csv_to_duckdb_task = load_csv_to_duckdb()
    

        # use the DbtTaskGroup class to create a task group containing task created
        # from dbt models
    dbt_tg = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "vars": '{"country_code": "CH"}',
        }
    )
 
    make_quarto_report = BashOperator(
    task_id="make_quarto_report",
    bash_command=f"quarto render {os.environ['AIRFLOW_HOME']}/report/hello.qmd  --to html")

    
    load_csv_to_duckdb_task >> dbt_tg >> make_quarto_report
 


synthea_omop_dag()
