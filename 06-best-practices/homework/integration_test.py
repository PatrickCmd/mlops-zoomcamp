import argparse
import os
from datetime import datetime

import pandas as pd

import batch

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
categorical = ["PUlocationID", "DOlocationID"]
columns = ["PUlocationID", "DOlocationID", "pickup_datetime", "dropOff_datetime"]
options = {"client_kwargs": {"endpoint_url": S3_ENDPOINT_URL}}


def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)


def generate_df():
    data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (1, 1, dt(1, 2, 0), dt(2, 2, 1)),
    ]
    df = pd.DataFrame(data, columns=columns)

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="the year of the dataset for model predictions.")
    parser.add_argument(
        "--month", help="the month of the dataset for model predictions."
    )
    args = parser.parse_args()

    year, month = int(args.year), int(args.month)
    batch.main(year, month)

    result_file = batch.get_output_path(year, month)
    df_result = batch.read_data(result_file)
    durations_prediction_sum = df_result["predicted_duration"].sum()

    assert round(durations_prediction_sum, 1) == 69.3
    print(
        f"sum of predicted durations for the test dataframe: {durations_prediction_sum}"
    )
