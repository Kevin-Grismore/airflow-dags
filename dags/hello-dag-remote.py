import pendulum
from airflow.decorators import dag, task

@dag(
    dag_id="hello-dag-remote",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def hello():
    @task
    def hello_task():
        print('Hello world!')

    hello_task()

dag = hello()