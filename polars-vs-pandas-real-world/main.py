import time

import numpy as np
import pandas as pd
import polars as pl


def main_pd():
    start_time = time.time()
    df = pd.read_csv("large_sales_data.csv")

    # Fix data types up front
    df["region"] = df["region"].astype("category")
    df["category"] = df["category"].astype("category")
    df["status"] = df["status"].astype("category")

    # Vectorized revenue calculation
    df["net_revenue"] = df["quantity"] * df["sales"] * (1 - 0.075)

    # Vectorized Flagging
    df["order_flag"] = np.where(df["net_revenue"] > 50000, "high", "low")

    # Aggregation
    result = df.groupby("region")["net_revenue"].sum()

    end_time = time.time()
    print(f"Pandas took {end_time - start_time} seconds")
    print(result)


def main_polars_eager():
    start_time = time.time()

    result = (
        pl.read_csv("large_sales_data.csv")
        .with_columns(
            [(pl.col("sales") * pl.col("quantity") * (1 - 0.075)).alias("net_revenue")]
        )
        .with_columns(
            [
                pl.when(pl.col("net_revenue") > 50000)
                .then(pl.lit("high"))
                .otherwise(pl.lit("low"))
                .alias("order_flag")
            ]
        )
        .group_by("region")
        .agg(pl.col("net_revenue").sum())
    )

    end_time = time.time()
    print(f"Polars Eager mode took {end_time - start_time} seconds")
    print(result)


def main_polars_lazy():
    start_time = time.time()

    result = (
        pl.scan_csv("large_sales_data.csv")
        .with_columns(
            [(pl.col("sales") * pl.col("quantity") * (1 - 0.075)).alias("net_revenue")]
        )
        .with_columns(
            [
                pl.when(pl.col("net_revenue") > 50000)
                .then(pl.lit("high"))
                .otherwise(pl.lit("low"))
                .alias("order_flag")
            ]
        )
        .group_by("region")
        .agg(pl.col("net_revenue").sum())
        .collect()
    )

    end_time = time.time()
    print(f"Polars Lazy mode took {end_time - start_time} seconds")
    print(result)


if __name__ == "__main__":
    main_pd()
    main_polars_eager()
    main_polars_lazy()
