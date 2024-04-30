from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2023, 10, 18),
    schedule=None,
    tags=["example DAG", "Dynamic Task Mapping"],
    catchup=False,
)
def dynamic_task_example_taskflow():

    @task
    def get_file_paths() -> str:
        # logic to get file paths. (potentially)
        # results in different number of files each run
        import random

        num_files = random.randint(40, 100)

        return [f"folder/file{i}" for i in random.sample(range(1000), num_files)]

    @task(map_index_template="{{ my_custom_map_index }}")
    def process_file(constant: int, file: str) -> None:
        # logic to process file

        # create the custom map index
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = f"Processed {file} with constant: {constant}"

    file_paths = get_file_paths()
    processed_files = process_file.partial(constant=42).expand(
        file=file_paths
    )  # the mapping happens here .partial takes the constant argument and .expand takes the changing argument


dynamic_task_example_taskflow()
