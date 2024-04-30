"""
## The DAG from the Airflow in 1 Slide slide 

This is a very simple DAG that fetches a random cat fact (not idempotent!)
from an API using a @task decorated function and prints it to the logs using a
BashOperator.
The cat fact is passed from one task to the other by using XComs.
The last line in the DAG sets the dependency between the two tasks.
"""

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from pendulum import datetime
import requests


@dag(
    start_date=datetime(2023, 8, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 3},
    tags=["101 example DAG"],
)
def in_cat_fact():
    @task
    def get_cat_fact():
        r = requests.get("https://catfact.ninja/fact")
        return r.json()["fact"]

    get_cat_fact_obj = get_cat_fact()

    print_cat_fact = BashOperator(
        task_id="print_cat_fact",
        bash_command=f"echo {get_cat_fact_obj}",
    )

    get_cat_fact_obj >> print_cat_fact
    # chain(get_cat_fact_obj, print_cat_fact)


in_cat_fact()
