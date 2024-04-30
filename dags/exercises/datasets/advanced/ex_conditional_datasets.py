"""
Exercise: Conditional Datasets

Schedule this DAG to run as soon as the following dataset conditions are met:

- Either Dataset("file://a") or Dataset("file://b") must have been updated 
PLUS
- Dataset("file://c") must have been updated
PLUS
- Either Dataset("file://d") or Dataset("file://e") must have been updated
"""


from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from pendulum import datetime


@dag(
    start_date=datetime(2024, 4, 1),
    schedule=None,
    catchup=False,
    tags=["Datasets", "advanced", "exercises"],
)
def ex_conditional_datasets():

    @task
    def placeholder_task():
        return "Hi!"

    placeholder_task()


ex_conditional_datasets()
