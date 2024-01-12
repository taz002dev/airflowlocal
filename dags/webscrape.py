from airflow.decorators import task, dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.utils.email import send_email

from datetime import datetime


def email_function(context):
    dag_run = context.get("dag_run")
    msg = "DAG ran successfully"
    subject = f"DAG {dag_run} has completed"
    send_email(to="andreicanache@gmail.com", subject=subject, html_content=msg)


@dag (start_date=datetime(2021, 1, 1), schedule_interval='@daily', catchup=False , on_failure_callback=email_function)
def webscrape():

    @task()
    def t1():
        return '{message:"test"}'

    t2 = DockerOperator(            
        task_id='t2',
        image = 'taz002dev/scrape:amd64',
        container_name='task_t2',
        api_version='auto',
        # command = 'echo "Yo no soi marinero"',
        auto_remove=True,
        # docker_url='container:lucid_lederberg',
        docker_url="unix://var/run/docker.sock",
        environment={"BQ_CREDS_FILE":"/app/bq_creds.json"},
        network_mode='bridge',
        mounts = [Mount(source = "/home/andrei/.bq/inlaid-keyword-311405-033a1e9fcf98.json",
                       target = "/app/bq_creds.json", 
                       type ="bind")]
    )
    # t2 = DockerOperator(
    #        task_id='t2',
    #        image = 'hello-world',
    #        docker_url='unix://var/run/docker.sock',
    #        network_mode='bridge', 
    #        auto_remove=True       
    #   )
    # t2 = DockerOperator ( 
    #                     task_id='t2',
    #                     api_version='auto',
    #                     container_name='task_t2'
    #                     image=' stock_image: v1.0.0', 
    #                     command='bash /†mp/scripts/output.sh ', 
    #                     docker_url='unix://var/run/docker.sock', 
    #                     network_mode='bridge', 
    #                     xcom_all=True, 
    #                     retrieve_output=True, 
    #                     retrieve_output_path='/tmp/script.out',
    #                     auto_remove=True)
    t1() >> t2

dag = webscrape()