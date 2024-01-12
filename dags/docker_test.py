from airflow.decorators import task, dag
from airflow.providers.docker.operators.docker import DockerOperator

from datetime import datetime

@dag (start_date=datetime(2021, 1, 1), schedule_interval='@daily', catchup=False)
def docker_dag():

    @task()
    def t1():
        pass

    t2 = DockerOperator(            
        task_id='t2',
        image = 'hello-world',
        container_name='task_t2',
        api_version='auto',
        # command = 'echo "Yo no soi marinero"',
        auto_remove=True,
        # docker_url='container:lucid_lederberg',
        docker_url="unix://var/run/docker.sock",
        network_mode='bridge'
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

dag = docker_dag()