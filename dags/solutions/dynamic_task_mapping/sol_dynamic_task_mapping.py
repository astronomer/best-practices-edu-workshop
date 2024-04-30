"""
Solution: Dynamic Task Mapping
"""

from airflow.decorators import dag, task
import requests


@dag(
    start_date=None,
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["Dynamic Task Mapping", "solution"],
)
def sol_dynamic_task_mapping():
    @task
    def get_fruits() -> list[dict]:
        import random

        rand_int = random.randint(10, 49)

        r = requests.get(f"https://www.fruityvice.com/api/fruit/all").json()
        r = random.sample(r, rand_int)

        return r

    # NEW in Airflow 2.9: Define custom names for the map index
    @task(map_index_template="{{ my_mapping_variable }}")
    def print_fruit_info(fruit_info: dict):

        fruit_name = fruit_info["name"]
        sugar_content = fruit_info["nutritions"]["sugar"]
        calories = fruit_info["nutritions"]["calories"]
        carbs = fruit_info["nutritions"]["carbohydrates"]
        protein = fruit_info["nutritions"]["protein"]
        fat = fruit_info["nutritions"]["fat"]

        print(f"{fruit_name} sugar content: {sugar_content}")
        print(f"{fruit_name} calories: {calories}")
        print(f"{fruit_name} carbs: {carbs}")
        print(f"{fruit_name} protein: {protein}")
        print(f"{fruit_name} fat: {fat}")

        # create custom map index
        from airflow.operators.python import get_current_context

        context = get_current_context()
        # The map index is added after the task has run, so it can include any computed values
        # from within the task
        context["my_mapping_variable"] = f"{fruit_name} - {sugar_content}g sugar."

    print_fruit_info.expand(
        fruit_info=get_fruits()
    )  # use expand to map over a list of inputs


sol_dynamic_task_mapping()
