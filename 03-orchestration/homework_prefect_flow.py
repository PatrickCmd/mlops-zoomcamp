import argparse
import os
import pickle
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner


def dump_pickle(obj, filename):
    with open(filename, "wb") as f_out:
        return pickle.dump(obj, f_out)


@task(name="Get data paths")
def get_paths(date):
    logger = get_run_logger()
    url_prefix = "https://nyc-tlc.s3.amazonaws.com/trip+data/"

    # 2 months back and previous month
    prev_month = (date - relativedelta(months=1)).strftime("%Y-%m")
    prev_2_month = (date - relativedelta(months=2)).strftime("%Y-%m")
    logger.info(f"Previous month: {prev_month}")
    logger.info(f"Previous second month: {prev_2_month}")

    train_filename = f"fhv_tripdata_{prev_2_month}.parquet"
    val_filename = f"fhv_tripdata_{prev_month}.parquet"

    if os.path.exists(f"./data/{train_filename}") and os.path.exists(
        f"./data/{val_filename}"
    ):
        logger.info(f"Train path: ./data/{train_filename} already exists")
        logger.info(f"Val path: ./data/{val_filename} already exists")
    else:
        logger.info("Downloading training and validation datasets")
        status1 = os.system(
            f"wget {url_prefix}{train_filename} -O ./data/{train_filename}"
        )
        status2 = os.system(f"wget {url_prefix}{val_filename} -O ./data/{val_filename}")
        if status1 != 0 and status2 != 0:
            return "ERROR 404: File Not Found"

    train_path = f"./data/{train_filename}"
    val_path = f"./data/{val_filename}"

    return train_path, val_path


@task(name="Read data")
def read_data(path):
    df = pd.read_parquet(path)
    return df


@task(name="Prepare features")
def prepare_features(df, categorical, train=True):
    logger = get_run_logger()

    df["duration"] = df.dropOff_datetime - df.pickup_datetime
    df["duration"] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    mean_duration = df.duration.mean()
    if train:
        logger.info(f"The mean duration of training is {mean_duration}")
    else:
        logger.info(f"The mean duration of validation is {mean_duration}")

    df[categorical] = df[categorical].fillna(-1).astype("int").astype("str")
    return df


@task(name="Train Model")
def train_model(df, categorical):
    logger = get_run_logger()

    train_dicts = df[categorical].to_dict(orient="records")
    dv = DictVectorizer()
    X_train = dv.fit_transform(train_dicts)
    y_train = df.duration.values

    logger.info(f"The shape of X_train is {X_train.shape}")
    logger.info(f"The DictVectorizer has {len(dv.feature_names_)} features")

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    y_pred = lr.predict(X_train)
    mse = mean_squared_error(y_train, y_pred, squared=False)
    logger.info(f"The MSE of training is: {mse}")
    return lr, dv


@task(name="Run model")
def run_model(df, categorical, dv, lr):
    logger = get_run_logger()

    val_dicts = df[categorical].to_dict(orient="records")
    X_val = dv.transform(val_dicts)
    y_pred = lr.predict(X_val)
    y_val = df.duration.values

    mse = mean_squared_error(y_val, y_pred, squared=False)
    logger.info(f"The MSE of validation is: {mse}")
    return


@flow(task_runner=SequentialTaskRunner(), name="Model training hw")
def main(date=None):
    logger = get_run_logger()

    categorical = ["PUlocationID", "DOlocationID"]

    # Get train and validation file paths
    if date is None:
        date = datetime.today()
    else:
        date = datetime.strptime(date, "%Y-%m-%d")

    train_path, val_path = get_paths(date).result()

    df_train = read_data(train_path)
    df_train_processed = prepare_features(df_train, categorical)

    df_val = read_data(val_path)
    df_val_processed = prepare_features(df_val, categorical, train=False)

    # train the model
    lr, dv = train_model(df_train_processed, categorical).result()
    run_model(df_val_processed, categorical, dv, lr)

    # Saving dictvectorizer and model for given date
    logger.info("Saving dictvectorizer and model for given date.")
    date_str = date.strftime("%Y-%m-%d")
    dest_path = "./models/output"
    dump_pickle(dv, os.path.join(dest_path, f"dv-{date_str}.pkl"))
    dump_pickle(lr, os.path.join(dest_path, f"model-{date_str}.pkl"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="the date the model is trained at.")
    args = parser.parse_args()
    main(args.date)
