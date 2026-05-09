import time
import pandas as pd
import polars as pl
import duckdb
import numpy as np
from datafusion import SessionContext, col
from datafusion import functions as F

"""
Performance Benchmark: Pandas vs Polars vs DuckDB vs datafusion

This script generates a synthetic dataset with 10 million rows and runs
an identical aggregation across Pandas, Polars, and DuckDB to compare
execution speed on a single machine.

Run this script from the command line:
    python performance_benchmark.py
"""


def generate_data(n_rows: int = 100_000_000, output_path: str = "benchmark_data.csv") -> None:
    """Generate synthetic sales data and write to CSV."""
    data = {
        "category": np.random.choice(["A", "B", "C", "D"], n_rows),
        "region": np.random.choice(["NA", "EU", "APAC"], n_rows),
        "sales": np.random.randn(n_rows) * 100 + 500,
    }
    pd.DataFrame(data).to_csv(output_path, index=False)
    print(f"Generated {n_rows:,} rows -> {output_path}\n")


def benchmark_pandas(csv_path: str) -> float:
    """Run aggregation using Pandas and return elapsed time."""
    start = time.time()
    df = pd.read_csv(csv_path)
    result = df.groupby("category").agg({"sales": "mean"}).reset_index()
    elapsed = time.time() - start
    print(f"Pandas: {elapsed:.2f}s")
    return elapsed


def benchmark_polars(csv_path: str) -> float:
    """Run aggregation using Polars and return elapsed time."""
    start = time.time()
    # scan_csv uses the LazyFrame / streaming engine and avoids the
    # parallel chunk-boundary miscount that triggers:
    #   ComputeError: CSV malformed: expected N rows, actual M rows
    # on very large files with Polars' eager multi-threaded CSV reader.
    result = (
        pl.scan_csv(csv_path)
        .group_by("category")
        .agg(pl.col("sales").mean())
        .collect()
    )
    elapsed = time.time() - start
    print(f"Polars: {elapsed:.2f}s")
    return elapsed


def benchmark_duckdb(csv_path: str) -> float:
    """Run aggregation using DuckDB and return elapsed time."""
    start = time.time()
    result = duckdb.sql(f"""
        SELECT category, AVG(sales) AS avg_sales
        FROM '{csv_path}'
        GROUP BY category
    """).df()
    elapsed = time.time() - start
    print(f"DuckDB: {elapsed:.2f}s")
    return elapsed


def benchmark_datafusion(csv_path: str) -> float:
    """Run aggregation using datafusion and return elapsed time."""
    start = time.time()
    
    ctx = SessionContext()
    df_fusion = ctx.read_csv(csv_path, has_header=True)
    result_fusion = (
        df_fusion
        .aggregate(
            [col("category")],
            [F.avg(col("sales")).alias("sales")],
        )
        .collect()  # materialise the lazy plan — required for accurate timing
    )

    elapsed = time.time() - start
    print(f"DataFusion: {elapsed:.2f}s")
    return elapsed


def main() -> None:
    csv_path = "benchmark_data.csv"
    n_rows = 100_000_000

    # Generate data
    # generate_data(n_rows, csv_path)

    # Run benchmarks
    pandas_time = benchmark_pandas(csv_path)
    polars_time = benchmark_polars(csv_path)
    duckdb_time = benchmark_duckdb(csv_path)
    datafusion_time = benchmark_datafusion(csv_path)

    # Summary
    print("\n--- Speedup Summary ---")
    print(f"Polars is {pandas_time / polars_time:.1f}x faster than Pandas")
    print(f"DuckDB is {pandas_time / duckdb_time:.1f}x faster than Pandas")
    print(f"DataFusion is {pandas_time / datafusion_time:.1f}x faster than Pandas")


    # Polars is 18.0x faster than Pandas
    # DuckDB is 14.4x faster than Pandas
    # DataFusion is 43.4x faster than Pandas

if __name__ == "__main__":  
    main()