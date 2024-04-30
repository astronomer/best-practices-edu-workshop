from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.models.baseoperator import chain
from pendulum import datetime


@dag(
    start_date=datetime(2024, 4, 1),
    schedule=[Dataset("s3://a")],  # use [] to schedule based on one dataset!
    catchup=False,
    tags=["Datasets", "example DAG"],
)
def datasets_simple_upstream():

    @task(outlets=[Dataset("s3://b")])
    def write_to_bucket_b():
        import time

        # code that updates a file in S3
        # does not actually need to update the file
        time.sleep(5)
        return "File updated"

    @task(
        outlets=[
            Dataset("s3://c"),
            Dataset("s3://d"),
        ]  # you can update multiple datasets at once!
    )
    def write_to_bucket_c_and_d():
        import time

        # code that updates a file in S3
        # does not actually need to update the file
        time.sleep(15)
        return "Files updated"

    chain(write_to_bucket_b(), write_to_bucket_c_and_d())


datasets_simple_upstream()
