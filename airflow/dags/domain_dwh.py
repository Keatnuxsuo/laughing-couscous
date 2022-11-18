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
    airbyte_sync_id = Variable.get("airbyte_ajp_01_domain_api_sync")
    databricks_abs_job_id = Variable.get("databricks_abs_job_id")
    databricks_abs_job_name = Variable.get("databricks_abs_job_name")
    databricks_domain_job_name = Variable.get("databricks_domain_job_name")
    databricks_domain_job_id = Variable.get("databricks_domain_job_id")
    databricks_airflow_conn = Variable.get("ajp_databricks_connection")

    # The ABS notebook has a few datasets of interest that are set up as inputs
    # to a widget in a common notebook.
    # Set up tasks to call these once each, plus REGION and STATE dimensions.
    datasets = ['hhld_size', 'med_age', 'med_income', 'mortgage_pymt', 'rent_pymt']

    abs_params = [dict(
        dataset = x,
        dimension_1 = 'REGION',
        dimension_2 = 'STATE'
    ) for x in datasets]

    # Task setup for Airbyte - Domain. Just one operator needed.
    airbyte_task_domain = AirbyteTriggerSyncOperator(
        task_id='domain_sync',
        airbyte_conn_id=airbyte_airflow_conn_id,
        connection_id=airbyte_sync_id,
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    # Task setup for Databricks - ABS. Dynamic creation of tasks based on datasets above.
    databricks_abs_tasks = [
    DatabricksRunNowOperator(
            task_id=f'databricks_task_abs_{next_param["dataset"]}',
            job_name=databricks_abs_job_name,
            notebook_params = next_param,
            timeout=300,
            databricks_conn_id=databricks_airflow_conn
        ) for next_param in abs_params]

    # Task setup for Databricks - Domain. Just one needed.
    databricks_task_domain = DatabricksRunNowOperator(
        job_name = databricks_domain_job_name,
        timeout=300,
        databricks_conn_id=databricks_airflow_conn
    )

    # Airflow should run the list in the middle?
    airbyte_task_domain >> databricks_task_domain
    databricks_abs_tasks