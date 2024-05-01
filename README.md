## Apache Airflow DAG Authoring & Best Practices Workshop

*Date: 2024-05-02*

This repository contains example code, exercises and solutions for the Apache Airflow DAG Authoring & Best Practices workshop.

## How to use this repository

While the DAGs in this repository can be used in any Airflow 2.9 setup, the easiest way to try them out locally is by either using the Astro CLI or GitHub Codespaces.

### Using the Astro CLI

1. Install the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) on your local machine
2. Clone this repository
3. Run `astro dev start` in the repository root, which will start a local Airflow using 4 Docker containers:

    - Postgres: Airflow's Metadata Database
    - Webserver: The Airflow component responsible for rendering the Airflow UI
    - Scheduler: The Airflow component responsible for monitoring and triggering tasks
    - Triggerer: The Airflow component responsible for triggering deferred tasks

4. You can access the Airflow UI at [http://localhost:8080](http://localhost:8080) with the login credentials `admin` and `admin`.
5. Run DAGs by toggling them on in the Airflow UI and clicking the Run arrow.

### Using GitHub Codespaces

1. Fork this repository.
2. Create a new GitHub codespaces project on your fork by clicking **...** and **New with options**. Make sure it uses at least 4 cores!
3. Wait for the codespaces project to start. Once it has started, open a new terminal and run the following command:

    ```bash
    astro dev start
    ```

4. Once the Airflow project has started access the Airflow UI by clicking on the **Ports** tab and opening the forward URL for port 8080.
5. Log in to the Airflow UI using the credentials `admin` and `admin`.
6. Run DAGs by toggling them on in the Airflow UI and clicking the Run arrow.

## Content

### DAGs

#### Example DAGs:

- `bad_dag.py`: A DAG with common issues and anti-patterns
- `good_dag.py`: The same DAG but with best practices applied
- `in_cat_fact.py`: Very simple beginner DAG from the `Airflow in 1 Slide` Slide.
- `datasets/`
    - `datasets_simple_upstream.py`: A DAG that is scheduled on 1 Dataset and produces updates to 3 other Datasets.
    - `datasets_simple_downstream.py`: A DAG that is scheduled on 2 Datasets.
- `dynamic_task_mapping/`
    - `dynamic_task_example_traditional.py`: A DAG that demonstrates how to use dynamic task mapping with traditional Airflow operators.
    - `dynamic_task_example_taskflow.py`: A DAG that demonstrates how to use dynamic task mapping with the TaskFlow API (`@task` decorator).
    - `dynamic_task_group_mapping.py`: A DAG that demonstrates how to dynamically map a task group.
    - `dynamic_task_expand_kwargs.py`: A DAG that demonstrates mapping over sets of keyword arguments using `.expand_kwargs()`.


#### Exercise DAGs:

- `/datasets/`
    - `basic/`
        - `my_monolithic_dag.py`: A longer DAG. Exercise: Split this DAG into multiple smaller DAGs and schedule them using datasets.
    - `advanced/`
        - `ex_time_datasets.py`: Schedule this DAG on a time and dataset schedule as described in the docstring.
        - `ex_conditional_datasets.py`: Schedule this DAG on several datasets using conditional logic as described in the docstring.
- `/dynamic_task_mapping/`
    - `ex_dynamic_task_mapping.py`: Exercise: Refactor this DAG to use dynamic task mapping.


#### Solution DAGs:

- `/datasets/`
    - `basic/`
        - `my_etl_dag.py` and `my_ml_dag.py`: A possible solution to the `my_monolithic_dag.py` exercise.
    - `advanced/`
        - `sol_time_datasets_solution.py`: The solution to the `ex_time_datasets.py` exercise.
        - `sol_conditional_datasets_solution.py`: The solution to the `ex_conditional_datasets.py` exercise.
- `/dynamic_task_mapping/`
    - `sol_dynamic_task_mapping.py`: The solution to the `ex_dynamic_task_mapping.py` exercise.


### Other

The `include/dynamic_dag_generation_multifile_method/` folder contains an example of generating DAG files from a JSON config.
The `tests` folder contains examples for DAG validation tests, unit tests and integration tests. You can run all tests with the Astro CLI by using `astro dev pytest`.