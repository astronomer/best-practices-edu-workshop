from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from pendulum import datetime


@dag(
    start_date=datetime(2024, 4, 1),
    schedule=(
        Dataset("s3://b") & Dataset("s3://c")
    ),  # use () and conditional expressions to schedule based on multiple datasets!
    # AND = &, OR = |
    catchup=False,
    tags=["Datasets", "example DAG"],
)
def datasets_simple_downstream():

    @task
    def placeholder_task():
        import time
        # some code
        time.sleep(20)
        return "Hi"

    placeholder_task()


datasets_simple_downstream()
