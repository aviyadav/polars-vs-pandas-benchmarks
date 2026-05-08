import time

import polars as pl

CSV_FILE = "large_sales_data.csv"


# Shared query builder — keeps transformation logic in one place
def build_query(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf
        # Only read the columns we actually need — skip order_id, order_date, discount
        .select(["region", "sales", "quantity", "status", "category"])
        .with_columns(
            (pl.col("sales") * pl.col("quantity") * (1 - 0.075)).alias("net_revenue")
        )
        .with_columns(
            pl.when(pl.col("net_revenue") > 50000)
            .then(pl.lit("high"))
            .otherwise(pl.lit("low"))
            .alias("order_flag")
        )
        .group_by("region")
        .agg(pl.col("net_revenue").sum())
        .sort("region")
    )


def main_polars_lazy():
    """
    Polars lazy mode — builds an optimised query plan then collects.
    Polars will push down the column projection so only the needed
    columns are read from disk, keeping memory well under control.
    """
    print("=" * 60)
    print("Polars — Lazy mode")
    print("=" * 60)
    try:
        start = time.time()
        result = build_query(pl.scan_csv(CSV_FILE)).collect()
        print(f"Time : {time.time() - start:.2f}s")
        print(result)
        return result
    except FileNotFoundError:
        print(f"Error: {CSV_FILE} not found. Run generate-dataset.py first.")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None


def main_polars_streaming():
    """
    Polars streaming mode — executes the query in fixed-size chunks so
    the entire dataset never lives in RAM at once.  The final aggregated
    result is tiny (one row per region) regardless of input size.
    """
    print("\n" + "=" * 60)
    print("Polars — Streaming mode  (constant-memory)")
    print("=" * 60)
    try:
        start = time.time()
        result = build_query(pl.scan_csv(CSV_FILE)).collect(engine="streaming")
        print(f"Time : {time.time() - start:.2f}s")
        print(result)
        return result
    except FileNotFoundError:
        print(f"Error: {CSV_FILE} not found. Run generate-dataset.py first.")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None


if __name__ == "__main__":
    lazy_result = main_polars_lazy()
    streaming_result = main_polars_streaming()

    if lazy_result is not None and streaming_result is not None:
        match = lazy_result.sort("region").equals(streaming_result.sort("region"))
        print("\n" + "=" * 60)
        print(f"Results match: {match}")
        print("=" * 60)
