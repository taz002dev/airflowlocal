from airflow.decorators import task, dag
from airflow.operators.bash import BashOperator

from datetime import datetime

@dag (start_date=datetime(2021, 1, 1), schedule_interval='@daily', catchup=False)
def webscrape_bash():

    @task()
    def t1():
        pass

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "bubamara"'
        # env: Optional[Dict[str, str]] = None,
        # output_encoding: str = 'utf-8',
        # skip_exit_code: int = 99,
    )
    t1() >> bash_task

dag = webscrape_bash()