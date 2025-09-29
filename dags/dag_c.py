from airflow.sdk import DAG, task

from datetime import datetime, timedelta

with DAG(
        dag_id="dag_c",
        start_date=datetime(2025, 1, 1),
        schedule="* * * * *"
) as dag:
    @task
    def task_1():
        print("task_1")


    @task
    def task_2():
        print("task_2")


    @task
    def task_3():
        print("task_3")


    task_1() >> task_2() >> task_3()
