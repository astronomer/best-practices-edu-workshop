from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2023, 10, 18),
    schedule=None,
    tags=["example DAG", "Dynamic Task Mapping"],
    catchup=False,
)
def dynamic_task_expand_kwargs():

    @task
    def get_file_paths() -> str:
        # logic to get file paths. (potentially)
        # results in different number of files each run
        import random

        num_sets = random.randint(40, 100)
        random_noise = random.randint(1, 1000)

        return [
            {"my_file": f"folder/file{i}", "num": i + random_noise}
            for i in random.sample(range(1000), num_sets)
        ]  # this is preparing downstream to map over sets of keyword arguments

    @task(map_index_template="{{ my_custom_map_index }}")
    def process_file(constant: int, my_file: str, num: int) -> None:
        # logic to process file

        # create the custom map index
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = (
            f"Processed {my_file} with num: {num} and constant: {constant}"
        )

    file_paths = get_file_paths()
    processed_files = process_file.partial(constant=42).expand_kwargs(
        file_paths
    )  # the mapping happens here .partial takes the constant argument and .expand_kwargs takes the changing argument
    # expand_kwargs is used to expand over sets of keyword arguments, and takes a list of dictionaries


dynamic_task_expand_kwargs()
