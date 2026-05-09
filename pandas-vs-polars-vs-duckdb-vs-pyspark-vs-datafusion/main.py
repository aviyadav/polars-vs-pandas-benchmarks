import time

# Pandas
import pandas as pd

# Polars
import polars as pl

# DuckDB
import duckdb

# DataFusion
from datafusion import SessionContext, col
from datafusion import functions as F

# PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

def panads_dataframe():
    start = time.time()    
    df_pd = pd.read_csv("large_sales_data.csv")
    result_pd = (
        df_pd.groupby("category")
        .agg({"sales": "mean"})
        .reset_index()
    )

    end = time.time()

    print(f"Pandas: {end - start}")
    print(result_pd.head())


def polars_dataframe():
    start = time.time()    
    df_pl = pl.read_csv("large_sales_data.csv")
    result_pl = (
        df_pl.group_by("category")
        .agg(pl.col("sales").mean())
    )

    end = time.time()

    print(f"Polars: {end - start}")
    print(result_pl.head())


def duckdb_dataframe():
    start = time.time()    
    result_duck = duckdb.sql("""
        SELECT category, avg(sales) AS sales
        FROM "large_sales_data.csv"
        GROUP BY category
    """).df()

    end = time.time()

    print(f"DuckDB: {end - start}")
    print(result_duck.head())


def pyspark_dataframe():
    import os
    import sys
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # PySpark 4.1 / Spark 4.x only supports Java 17 or 21.
    # OpenJDK 25 (shipped with Fedora 44) removed javax.security.auth.Subject.getSubject()
    # which Hadoop still depends on, causing UnsupportedOperationException at runtime.
    # Point JAVA_HOME to a compatible Java 21 installation.
    java21_home = "/usr/lib/jvm/jdk-21.0.7+6"
    if os.path.isdir(java21_home):
        os.environ["JAVA_HOME"] = java21_home

    start = time.time()    
    
    spark = SparkSession.builder.getOrCreate()
    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "large_sales_data.csv")
    df_spark = spark.read.csv(csv_path, header=True, inferSchema=True)
    result_spark = (
        df_spark.groupBy("category")
        .agg(avg("sales").alias("sales"))
    )
    result_spark.show(5)

    end = time.time()

    print(f"PySpark: {end - start}")

def datafusion_dataframe():
    start = time.time()

    ctx = SessionContext()
    df_fusion = ctx.read_csv("large_sales_data.csv", has_header=True)
    result_fusion = df_fusion.aggregate(
        [col("category")],
        [F.avg(col("sales")).alias("sales")],
    )

    end = time.time()

    print(f"DataFusion: {end - start}")
    print(result_fusion.to_pandas().head())



def main():
    panads_dataframe()
    polars_dataframe()
    duckdb_dataframe()
    datafusion_dataframe()
    pyspark_dataframe()


if __name__ == "__main__":
    main()