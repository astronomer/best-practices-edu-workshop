"""
Exercise: Time & Dataset scheduling

Schedule this DAG to run as soon as the following conditions are met:

EITHER:
- it is 3am on a Weekday
OR
- The Dataset("file://a") OR Dataset("file://b") has been updated
"""


from airflow.decorators import dag, task
# from airflow.models.dataset import Dataset
# from airflow.timetables.datasets import DatasetOrTimeSchedule
# from airflow.timetables.trigger import CronTriggerTimetable
from pendulum import datetime


@dag(
    start_date=datetime(2024, 4, 1),
    schedule=None,
    catchup=False,
    tags=["Datasets", "advanced", "exercises"],
)
def ex_time_datasets():

    @task
    def placeholder_task():
        return "Hi!"

    placeholder_task()


ex_time_datasets()
