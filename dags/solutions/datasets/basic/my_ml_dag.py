from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.models.baseoperator import chain
from pendulum import datetime
import pandas as pd

DATASET_URI = "file://include/example_data/my_data.csv"
# in a production environment, this variable should be defined only in one place!


@dag(
    schedule=[Dataset(DATASET_URI)],
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=["Datasets", "upstream", "solutions"],
)
def my_ml_dag():

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

    load_data_obj = load_data()
    create_train_test_split_obj = create_train_test_split(load_data_obj)
    train_linear_regression_model_obj = train_linear_regression_model(
        create_train_test_split_obj["X_train"], create_train_test_split_obj["y_train"]
    )
    test_linear_regression_model_obj = test_linear_regression_model(
        create_train_test_split_obj["X_test"], create_train_test_split_obj["y_test"]
    )

    chain(train_linear_regression_model_obj, test_linear_regression_model_obj)


my_ml_dag()
