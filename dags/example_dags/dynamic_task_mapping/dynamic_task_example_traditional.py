from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain
from pendulum import datetime


@dag(
    start_date=datetime(2023, 10, 18),
    schedule=None,
    tags=["example DAG", "Dynamic Task Mapping"],
    catchup=False,
)
def dynamic_task_example_traditional():

    @task
    def get_file_paths() -> str:
        # logic to get file paths. (potentially)
        # results in different number of files each run
        import random

        num_files = random.randint(40, 100)

        return [
            {"FILENAME": f"folder/file{i}", "CONSTANT": "42"}
            for i in random.sample(range(1000), num_files)
        ]  # return a list of dictionaries because the env parameter of the BashOperator takes a dict

    file_paths = get_file_paths()

    process_file = BashOperator.partial(
        task_id="process_file",
        bash_command="echo $FILENAME && echo $CONSTANT",
        map_index_template="Processing file: {{ task.env.FILENAME }} with constant: {{ task.env.CONSTANT }}",
    ).expand(env=file_paths)

    # since not all tasks are TaskFlow tasks, the dependency needs to be set manually
    chain(file_paths, process_file)


dynamic_task_example_traditional()
