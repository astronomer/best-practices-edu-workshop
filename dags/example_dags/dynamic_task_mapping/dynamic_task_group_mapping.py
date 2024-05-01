"""
### Dynamically map over a task group and pull XComs from specific tasks in the task group

This DAG shows three examples of dynamically mapping over a task group. Both, 
mapping positional arguments and keyword arguments are shown. A downstream task
demonstrates how to pull XComs from specific tasks in the task group.
"""

from airflow.decorators import dag, task_group, task
from pendulum import datetime


@dag(
    start_date=datetime(2024, 4, 1),
    schedule=None,
    catchup=False,
    tags=["Dynamic Task Mapping", "Example DAGs"],
)
def dynamic_task_group_mapping():
    # TASK GROUP 1: mapping over 1 positional argument
    @task_group(group_id="group1")
    def tg1(my_num):
        @task(map_index_template="{{ my_custom_map_index }}")
        def print_num(num):
            from airflow.operators.python import get_current_context

            context = get_current_context()
            context["my_custom_map_index"] = f"My number is: {num}"
            return num

        @task(map_index_template="{{ my_custom_map_index }}")
        def add_42(num):
            result = num + 42
            from airflow.operators.python import get_current_context

            context = get_current_context()
            context["my_custom_map_index"] = f"{num} + 42 = {result}"
            return result

        print_num(my_num) >> add_42(my_num)

    # a downstream task to print out resulting XComs
    @task
    def pull_xcom(**context):
        pulled_xcom = context["ti"].xcom_pull(
            task_ids=[
                "group1.add_42"
            ],  # reference a task in a taskgroup with task_group_id.task_id
            map_indexes=[
                2,
                3,
            ],  # only pull xcom from specific mapped task instances (2.5 feature)
            key="return_value",
        )

        # will print out a list of results from map index 2 and 3 of the add_42 task
        print(pulled_xcom)

    # creating 6 mapped task instances of the TaskGroup tg2 (2.5 feature) mapping over one positional argument
    tg1_object = tg1.expand(my_num=[19, 23, 42, 8, 7, 108])

    # setting dependencies
    tg1_object >> pull_xcom()


dynamic_task_group_mapping()
