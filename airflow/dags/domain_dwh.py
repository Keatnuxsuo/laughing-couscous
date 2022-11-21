import datetime
import pendulum
import itertools

from airflow import DAG
from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

with DAG(
    dag_id='domain_dwh_pipeline',
    schedule_interval='0 6 * * *',
    start_date=pendulum.datetime(2022, 11, 11, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['domain']
) as workflow:

    # region Connections/variables
    # Init Airbyte/Databricks connections
    # For some reason, using the Jinja templated way of grabbing variables can't find these.
    # Use Variable.get instead
    
    # Airbyte
    airbyte_airflow_conn_id = Variable.get("ajp_airbyte_connection")
    airbyte_domain_sync_id = Variable.get("airbyte_ajp_01_domain_api_sync")

    # The ABS notebook has a few datasets of interest that are on different endpoints.
    # This dictionary is used to dynamically generate the tasks required.
    airbyte_abs_endpoints = dict(
            med_age_persons=Variable.get("airbyte_ajp_01_abs_med_age_persons"),
            count_households=Variable.get("airbyte_ajp_01_abs_count_households"),
            med_mortgage_repymt_mthly=Variable.get("airbyte_ajp_01_abs_med_mortgage_repymt_mthly"),
            med_rent_weekly=Variable.get("airbyte_ajp_01_abs_med_rent_weekly"),
            med_ttl_fam_income_weekly=Variable.get("airbyte_ajp_01_abs_med_ttl_fam_income_weekly")
        )

    databricks_airflow_conn = Variable.get("ajp_databricks_connection")
    databricks_domain_bronze_job_name = Variable.get("databricks_domain_bronze_job_name")
    databricks_abs_bronze_job_name = Variable.get("databricks_abs_bronze_job_name")
    databricks_domain_silver_job_name = Variable.get("databricks_domain_silver_job_name")
    databricks_abs_silver_job_name = Variable.get("databricks_abs_silver_job_name")
    databricks_combined_gold_job_name = Variable.get("databricks_abs_domain_gold_job_name")

    # endregion Connections/variables

    # region Tasks

    # Task setup for Airbyte - Domain. Just one operator needed.
    airbyte_task_domain = AirbyteTriggerSyncOperator(
        task_id='airbyte_domain',
        airbyte_conn_id=airbyte_airflow_conn_id,
        connection_id=airbyte_domain_sync_id,
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    # ABS tasks - dynamically created from airbyte_abs_endpoints dictionary
    airbyte_abs_tasks = [
        AirbyteTriggerSyncOperator(
            task_id=f'airbyte_abs_{next_key}',
            airbyte_conn_id=airbyte_airflow_conn_id,
            connection_id=next_val,
            asynchronous=False,
            timeout=3600,
            wait_seconds=3
        ) for next_key, next_val in airbyte_abs_endpoints.items()
    ]

    # Task setup for Databricks - ABS. Just one per layer needed.
    databricks_task_abs_bronze = DatabricksRunNowOperator(
        task_id='databricks_abs_bronze',
        job_name=databricks_abs_bronze_job_name,
        databricks_conn_id=databricks_airflow_conn
    )

    databricks_task_abs_silver = DatabricksRunNowOperator(
        task_id='databricks_abs_silver',
        job_name=databricks_abs_silver_job_name,
        databricks_conn_id=databricks_airflow_conn
    )

    # Task setup for Databricks - Domain. Just one per layer needed.
    databricks_task_domain_bronze = DatabricksRunNowOperator(
        task_id='databricks_domain_bronze',
        job_name=databricks_domain_bronze_job_name,
        databricks_conn_id=databricks_airflow_conn
    )

    databricks_task_domain_silver = DatabricksRunNowOperator(
        task_id='databricks_domain_silver',
        job_name=databricks_domain_silver_job_name,
        databricks_conn_id=databricks_airflow_conn
    )

    # Final combined dataset
    databricks_task_combined_gold = DatabricksRunNowOperator(
        task_id='databricks_combined_gold',
        job_name=databricks_combined_gold_job_name,
        databricks_conn_id=databricks_airflow_conn
    )

    # endregion Tasks

    # region DAG
    airbyte_abs_tasks >> databricks_task_abs_bronze >> databricks_task_abs_silver
    airbyte_task_domain >> databricks_task_domain_bronze >> databricks_task_domain_silver
    [databricks_task_abs_silver, databricks_task_domain_silver] >> databricks_task_combined_gold
    # endregion DAG