from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from pendulum import datetime
import pandas as pd

DATASET_URI = "file://include/example_data/my_data.csv"
# in a production environment, this variable should be defined only in one place!


@dag(
    schedule=None,
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=["Datasets", "upstream", "solutions"],
)
def my_etl_dag():

    @task
    def extract() -> dict:
        "Mock data extraction."
        return {
            "a": [1, 2, 3, 4, 5, 6, 7],
            "b": [14, 7, 89, 56, 7, 2, 3],
            "target": [37, 58, 65, 72, 81, 98, 101],
        }

    @task
    def transform(data: dict) -> pd.DataFrame:
        """
        Normalize the data.
        Arg:
            data: dict
        Return:
            df: pd.DataFrame
        """

        df = pd.DataFrame(data)

        for col in df.columns:
            if col != "target":
                df[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())

        return df

    @task(
        outlets=[Dataset(DATASET_URI)],
    )
    def load(df: pd.DataFrame) -> None:
        """
        Save the data to a CSV file.
        Arg:
            data: pd.DataFrame
        """
        df.to_csv("include/example_data/my_data.csv", index=False)

    # using the TaskFlow API, dependencies are inferred
    extract_obj = extract()
    transform_obj = transform(extract_obj)
    load(transform_obj)


my_etl_dag()
