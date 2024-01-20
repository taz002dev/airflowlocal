from airflow.decorators import task, dag
from airflow.providers.docker.operators.docker import DockerOperator
# from airflow.providers.google.cloud.operators.bigquery import bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from docker.types import Mount
from airflow.utils.email import send_email
from airflow.models.xcom import XCom

from datetime import datetime

def email_function(context):
    dag_run = context.get("dag_run")
    msg = "DAG ran successfully"
    subject = f"DAG {dag_run} has completed"
    send_email(to="andreicanache@gmail.com", subject=subject, html_content=msg)

# def query_build(table_name:str):
#     return f'select * from {table_name} limit 1;'

def build_query(ti):
    table_name = ti.xcom_pull('table_name')
    return f'select * from {table_name} limit 1;'


@dag (start_date=datetime(2021, 1, 1), schedule_interval='@daily', catchup=False , on_failure_callback=email_function)
def bq_query():

    @task()
    def compose_query():
        return '{table:"inlaid-keyword-311405.moncrip_raw.message_gateway"}'

    BigQueryExecuteQueryOperator_task = BigQueryExecuteQueryOperator(
        task_id='bq_task',
        # sql= 'select * from {{task_instance.xcom_pull(task_ids="compose_query", key="return_value")}} limit 1',
        sql=build_query,
        write_disposition='WRITE_EMPTY',
        allow_large_results=False,
        gcp_conn_id='gcp_bq',
        use_legacy_sql=False,
        create_disposition='CREATE_IF_NEEDED',
        priority='INTERACTIVE',
        location='EU'
        )
  
    compose_query() >> BigQueryExecuteQueryOperator_task

dag = bq_query()