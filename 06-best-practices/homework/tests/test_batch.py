from datetime import datetime
from pathlib import Path

import pandas as pd

import batch

categorical = ["PUlocationID", "DOlocationID"]
columns = ["PUlocationID", "DOlocationID", "pickup_datetime", "dropOff_datetime"]


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


def test_prepare_data():
    df = generate_df()
    actual_df = batch.prepare_data(df, categorical)
    actual_df["duration"] = actual_df["duration"].round(decimals=1)

    data = [
        ("-1", "-1", dt(1, 2), dt(1, 10), 8.0),
        ("1", "1", dt(1, 2), dt(1, 10), 8.0),
    ]
    columns.append("duration")
    expected_df = pd.DataFrame(data, columns=columns)

    assert actual_df.to_dict() == expected_df.to_dict()
