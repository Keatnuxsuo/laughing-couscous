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
    dagrun_timeout=datetime.timedelta(minutes=15),
    tags=['domain']
) as workflow:

    # Init Airbyte/Databricks connections
    # For some reason, using the Jinja templated way of grabbing variables can't find these.
    # Use Variable.get instead
    airbyte_airflow_conn_id = Variable.get("ajp_airbyte_connection")
    airbyte_domain_sync_id = Variable.get("airbyte_ajp_01_domain_api_sync")

    databricks_abs_job_id = Variable.get("databricks_abs_job_id")
    databricks_abs_job_name = Variable.get("databricks_abs_job_name")
    databricks_domain_bronze_job_name = Variable.get("databricks_domain_bronze_job_name")
    databricks_domain_bronze_job_id = Variable.get("databricks_domain_bronze_job_id")
    databricks_domain_silver_job_name = Variable.get("databricks_domain_silver_job_name")
    databricks_domain_silver_job_id = Variable.get("databricks_domain_silver_job_id")
    databricks_airflow_conn = Variable.get("ajp_databricks_connection")

    # The ABS notebook has a few datasets of interest that are set up as inputs
    # to a widget in a common notebook.
    # Set up tasks to call these once each, plus REGION and STATE dimensions.
    airbyte_abs_endpoints = dict(
            med_age_persons=Variable.get("airbyte_ajp_01_abs_med_age_persons"),
            #count_households=Variable.get("airbyte_ajp_01_abs_count_households"),
            med_mortgage_repymt_mthly=Variable.get("airbyte_ajp_01_abs_med_mortgage_repymt_mthly"),
            med_rent_weekly=Variable.get("airbyte_ajp_01_abs_med_rent_weekly"),
            med_ttl_fam_income_weekly=Variable.get("airbyte_ajp_01_abs_med_ttl_fam_income_weekly")
        )

    # databricks_abs_datasets = ['hhld_size', 'med_age', 'med_income', 'mortgage_pymt', 'rent_pymt']

    # databricks_abs_params = [dict(
    #     dataset = x,
    #     dimension_1 = 'REGION',
    #     dimension_2 = 'STATE'
    # ) for x in databricks_abs_datasets]

    # Task setup for Airbyte - Domain. Just one operator needed.
    airbyte_task_domain = AirbyteTriggerSyncOperator(
        task_id='airbyte_domain',
        airbyte_conn_id=airbyte_airflow_conn_id,
        connection_id=airbyte_domain_sync_id,
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

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

    # # Task setup for Databricks - ABS. Dynamic creation of tasks based on datasets above.
    # databricks_abs_tasks = [
    # DatabricksRunNowOperator(
    #         task_id=f'databricks_task_abs_{next_param["dataset"]}',
    #         job_name=databricks_abs_job_name,
    #         notebook_params = next_param,
    #         timeout=300,
    #         databricks_conn_id=databricks_airflow_conn
    #     ) for next_param in databricks_abs_params]

    # # Task setup for Databricks - Domain. Just one needed.
    databricks_task_domain_bronze = DatabricksRunNowOperator(
        task_id='databricks_domain_bronze',
        job_name = databricks_domain_bronze_job_name,
        databricks_conn_id=databricks_airflow_conn
    )

    databricks_task_domain_silver = DatabricksRunNowOperator(
        task_id='databricks_domain_silver',
        job_name = databricks_domain_silver_job_name,
        databricks_conn_id=databricks_airflow_conn
    )

    airbyte_abs_tasks >> airbyte_task_domain >> databricks_task_domain_bronze >> databricks_task_domain_silver