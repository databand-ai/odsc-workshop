import pandas as pd
import numpy as np
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from pandas import DataFrame
from dbnd import task, pipeline, output
from sklearn.linear_model import ElasticNet
from dbnd import log_dataframe, log_metric
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from typing import Tuple
from sklearn.model_selection import train_test_split
import os

logging.basicConfig(level=logging.INFO)
WORKDIR = os.path.dirname(os.path.abspath(__file__))


def _p(*path):
    return os.path.join(WORKDIR, *path)


def _output(*path):
    return _p("output", *path)


INPUT_FILE = _p("wine.csv")
COLUMNS = [
    "fixed acidity",
    "volatile acidity",
    "citric acid",
    "residual sugar",
    "chlorides",
    "free sulfur dioxide",
    "total sulfur dioxide",
    "density",
    "pH",
    "sulphates",
    "alcohol",
    "quality",
]
DATA = [
    [7.2, 0.31, 0.5, 13.3, 0.056, 68, 195, 0.9982, 3.01, 0.47, 9.2, 5],
    [6.7, 0.41, 0.34, 9.2, 0.049, 29, 150, 0.9968, 3.22, 0.51, 9.1, 5],
    [6.7, 0.41, 0.34, 9.2, 0.049, 29, 150, 0.9968, 3.22, 0.51, 9.1, 5],
    [5.5, 0.485, 0, 1.5, 0.065, 8, 103, 0.994, 3.63, 0.4, 9.7, 4],
]


def fetch_prod_data_func(output_path):
    # bring data from your production source i.e. database
    data = pd.DataFrame(DATA, columns=COLUMNS)
    data.to_csv(output_path)


@task(result="training_set, validation_set")
def prepare_data(raw_data: DataFrame) -> Tuple[DataFrame, DataFrame]:
    train_df, validation_df = train_test_split(raw_data)

    return train_df, validation_df


@task
def train_model(
    training_set: DataFrame, alpha: float = 0.5, l1_ratio: float = 0.5,
) -> ElasticNet:
    lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio)
    lr.fit(training_set.drop(["quality"], 1), training_set[["quality"]])

    return lr


@task
def validate_model(model: ElasticNet, validation_dataset: DataFrame) -> str:
    log_dataframe("validation", validation_dataset)

    validation_x = validation_dataset.drop(["quality"], 1)
    validation_y = validation_dataset[["quality"]]

    prediction = model.predict(validation_x)
    rmse = np.sqrt(mean_squared_error(validation_y, prediction))
    mae = mean_absolute_error(validation_y, prediction)
    r2 = r2_score(validation_y, prediction)

    log_metric("rmse", rmse)
    log_metric("mae", rmse)
    log_metric("r2", r2)

    return "%s,%s,%s" % (rmse, mae, r2)


# task
@pipeline(result=("model", "validation"))
def predict_wine_quality(
    raw_data: DataFrame, alpha: float = 0.5, l1_ratio: float = 0.5,
):
    training_set, validation_set = prepare_data(raw_data=raw_data)

    model = train_model(training_set=training_set, alpha=alpha, l1_ratio=l1_ratio)
    validation = validate_model(model=model, validation_dataset=validation_set)

    return model, validation


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
}

with DAG(dag_id="predict_wine_quality_dag", default_args=default_args, schedule_interval="0 */6 * * *") as dag:
    alpha = 0.2
    l1_ratio = 0.3

    fetch_data_prod = _output("task1_out_{{ts_nodash}}.csv")
    task1 = PythonOperator(
        task_id="fetch_production_data",
        python_callable=fetch_prod_data_func,
        op_kwargs={"output_path": fetch_data_prod},
    )

    training_set, validation_set = prepare_data(fetch_data_prod)
    model = train_model(
        training_set=training_set, alpha=(alpha or 0.5), l1_ratio=(l1_ratio or 0.5)
    )
    validation = validate_model(model=model, validation_dataset=validation_set)
    task1 >> training_set.op
