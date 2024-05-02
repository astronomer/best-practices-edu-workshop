from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from pendulum import datetime


@dag(
    start_date=datetime(2024, 4, 1),
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 0 * * *", timezone="UTC"),
        datasets=(Dataset("file://a") | Dataset("file://b")),
        # Use () instead of [] to be able to use conditional dataset scheduling!
    ),
    catchup=False,
    tags=["Datasets", "example DAG"],
)
def sol_time_datasets():

    @task
    def placeholder_task():
        return "Hi!"

    placeholder_task()


"""
Solution: Time & Dataset scheduling

This DAG runs as soon as the following conditions are met:

EITHER:
- it is 3am on a Weekday
OR
- The Dataset("file://a") OR Dataset("file://b") has been updated


Help: https://crontab.guru/
"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from pendulum import datetime


@dag(
    start_date=datetime(2024, 4, 1),
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 0 * * 4", timezone="UTC"),
        datasets=(Dataset("file://a") | Dataset("file://b")),
        # Use () instead of [] to be able to use conditional dataset scheduling!
    ),
    catchup=False,
    tags=["Datasets", "example DAG"],
)
def time_and_dataset_example():

    @task
    def placeholder_task():
        return "Hi!"

    placeholder_task()


time_and_dataset_example()
