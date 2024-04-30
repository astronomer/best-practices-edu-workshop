"""
Exercise: Dynamic Task Mapping

Use dynamic task mapping to create one mapped task for each fruit returned by the get_fruits task. 
This will avoid the loop in the print_fruit_info task and allow for parallel execution of the mapped tasks and better observability.
Bonus: create a custom map index that includes the sugar content of the fruit.
"""

from airflow.decorators import dag, task
import requests


@dag(
    start_date=None,
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["Dynamic Task Mapping", "exercise"],
)
def ex_dynamic_task_mapping():
    @task
    def get_fruits() -> list[dict]:
        import random

        rand_int = random.randint(10, 49)

        r = requests.get(f"https://www.fruityvice.com/api/fruit/all").json()
        r = random.sample(r, rand_int)

        return r

    @task
    def print_fruit_info(fruit_info_list: list[dict]):

        for fruit_info in fruit_info_list:

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

    print_fruit_info(fruit_info_list=get_fruits())


ex_dynamic_task_mapping()
