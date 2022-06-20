#!/usr/bin/env python
# coding: utf-8

import argparse
import pickle
import pandas as pd


categorical = ["PUlocationID", "DOlocationID"]


with open("model.bin", "rb") as f_in:
    dv, lr = pickle.load(f_in)


def read_data(filename):
    df = pd.read_parquet(filename, engine="pyarrow")
    df["duration"] = df.dropOff_datetime - df.pickup_datetime
    df["duration"] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    df[categorical] = df[categorical].fillna(-1).astype("int").astype("str")
    return df


def apply_model(df, year, month):
    dicts = df[categorical].to_dict(orient="records")
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)
    print(f"mean predicted duration for the dataset: {y_pred.mean()}")
    df["ride_id"] = f"{year:04d}/{month:02d}_" + df.index.astype("str")

    output_file = f"output/fhv_tripdata_result/{year:04d}-{month:02d}.parquet"
    df.to_parquet(output_file, engine="pyarrow", compression=None, index=False)


def main(year, month):
    df = read_data(
        f"https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_{year}-{month:02d}.parquet"
    )

    apply_model(df, year, month)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--year",
        help="the year the model is used for predictions."
    )
    parser.add_argument(
        "--month",
        help="the month the model is used for predictions."
    )
    args = parser.parse_args()
    main(int(args.year), int(args.month))
