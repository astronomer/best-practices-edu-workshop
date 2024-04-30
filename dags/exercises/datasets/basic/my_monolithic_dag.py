"""
Exercise: Break this DAG out into (at least) two DAGs that are scheduled using Datasets!
"""

from airflow.decorators import dag, task

# from airflow.models.dataset import Dataset
from airflow.models.baseoperator import chain
from pendulum import datetime
import pandas as pd


@dag(
    schedule=None,
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=["Datasets", "exercises"],
)
def my_monolithic_dag():

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

    @task()
    def load(df: pd.DataFrame) -> None:
        """
        Save the data to a CSV file.
        Arg:
            data: pd.DataFrame
        """
        df.to_csv("include/example_data/my_data.csv", index=False)

    @task
    def load_data() -> pd.DataFrame:
        import pandas as pd

        df = pd.read_csv("include/example_data/my_data.csv")
        return df

    @task
    def create_train_test_split(df) -> dict:
        """
        Create a train test split of the data.
        Arg:
            df: pd.DataFrame
        Return:
            dict: dict with keys "X_train", "X_test", "y_train", "y_test"
        """
        from sklearn.model_selection import train_test_split

        X = df[["a", "b"]]
        y = df["target"]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        return {
            "X_train": X_train,
            "X_test": X_test,
            "y_train": pd.DataFrame(y_train),
            "y_test": pd.DataFrame(y_test),
        }

    @task
    def train_linear_regression_model(x_train, y_train) -> None:
        from sklearn.linear_model import LinearRegression
        import pickle

        model = LinearRegression()
        model.fit(x_train, y_train)

        with open("include/example_data/my_lr_model.pkl", "wb") as f:
            pickle.dump(model, f)

    @task
    def test_linear_regression_model(x_test, y_test) -> float:
        import pickle

        with open("include/example_data/my_lr_model.pkl", "rb") as f:
            model = pickle.load(f)

        return model.score(x_test, y_test)

    extract_obj = extract()
    transform_obj = transform(extract_obj)
    load_obj = load(transform_obj)

    load_data_obj = load_data()

    chain(load_obj, load_data_obj)

    create_train_test_split_obj = create_train_test_split(load_data_obj)
    train_linear_regression_model_obj = train_linear_regression_model(
        create_train_test_split_obj["X_train"], create_train_test_split_obj["y_train"]
    )
    test_linear_regression_model_obj = test_linear_regression_model(
        create_train_test_split_obj["X_test"], create_train_test_split_obj["y_test"]
    )

    chain(train_linear_regression_model_obj, test_linear_regression_model_obj)


my_monolithic_dag()
