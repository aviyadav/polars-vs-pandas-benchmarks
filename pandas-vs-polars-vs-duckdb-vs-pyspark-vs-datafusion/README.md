# Pandas vs Polars vs DuckDB vs DataFusion vs PySpark
A performance benchmark comparing five popular Python DataFrame / query-engine libraries on a common `GROUP BY` + `AVG` aggregation task over large CSV datasets.

## Benchmark Results

> **Task:** read a CSV file → `GROUP BY category` → `AVG(sales)`  
> **Dataset:** 100 million rows, 3 columns (`category`, `region`, `sales`)  
> **Machine:** Fedora Linux 44, Python 3.13, single node (no cluster)

| Library      | Time (s) | vs Pandas  |
| ------------ | --------:| ----------:|
| **Pandas**   | 19.42 s  | 1.0× (baseline) |
| **PySpark**  | ~5.8 s\* | ~3.3×      |
| **DuckDB**   | 1.25 s   | **15.5×**  |
| **DataFusion** | 0.53 s | **36.6×**  |
| **Polars**   | 0.92 s   | **21.2×**  |

\* PySpark timing is from `main.py` on the 1M-row dataset; the `benchmark.py` run (100M rows) excludes PySpark due to JVM startup overhead.

### Key Takeaways

- **Pandas** is the slowest by a wide margin — it reads the entire file into a single-threaded in-memory DataFrame with no query optimisation.
- **Polars** uses a lazy streaming engine (`scan_csv`) and Rust-backed columnar execution — **21× faster** than Pandas.
- **DuckDB** queries the CSV directly via SQL with vectorised execution — **15.5× faster**, no DataFrame load required.
- **DataFusion** (Apache Arrow / Rust) is the fastest at **36.6×**, using a fully lazy query plan that only scans what the aggregation needs.
- **PySpark** incurs JVM startup cost (~3–5 s) making it slower for single-node work, but it shines on distributed clusters.

## Benchmark Methodology

`benchmark.py` runs the same aggregation across all libraries and reports elapsed wall-clock time end-to-end (including CSV I/O).

| Library    | API used                                      |
| ---------- | --------------------------------------------- |
| Pandas     | `read_csv()` → `groupby().agg()`              |
| Polars     | `scan_csv()` → `group_by().agg().collect()`   |
| DuckDB     | `duckdb.sql("SELECT … GROUP BY …").df()`      |
| DataFusion | `ctx.read_csv()` → `.aggregate().collect()`   |
| PySpark    | `SparkSession` → `groupBy().agg()` (main.py)  |

> **Note on Polars `scan_csv`:** Polars' eager `read_csv()` raises `ComputeError: CSV malformed` on files larger than ~500 MB due to a parallel-chunk boundary misalignment. `scan_csv()` (LazyFrame) avoids this entirely and is the recommended API for large files.

> **Note on DataFusion timing:** DataFusion's `aggregate()` returns a lazy plan — `.collect()` is required to materialise results; without it the timer only captures query-planning time (~0.01 s), not execution.

## Prerequisites

- **Python** ≥ 3.13
- **Java 17 or 21** (required by PySpark / Spark 4.x — see [note below](#pyspark--java-compatibility))
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Setup

### 1. Install dependencies

```bash
uv sync
```

Or with pip:

```bash
pip install pandas polars duckdb datafusion pyspark
```

### 2. Generate the dataset

```bash
uv run python generate_dataset.py
```

This creates `large_sales_data.csv` (~60 MB, 1 million rows) with columns: `order_id`, `order_date`, `region`, `category`, `sales`, `quantity`, `discount`, `status`.

### 3. Run the benchmark

```bash
uv run python main.py
```

By default all four benchmarks run. You can comment/uncomment individual calls in the `main()` function to run specific ones.

## PySpark & Java Compatibility

PySpark 4.x (Spark 4) only supports **Java 17** and **Java 21**. If your system ships with a newer JDK (e.g. OpenJDK 25 on Fedora 44), PySpark will fail with:

```
UnsupportedOperationException: getSubject is not supported
```

This happens because Java 25 removed `javax.security.auth.Subject.getSubject()`, which Hadoop (a PySpark dependency) still relies on.

### Fix — install Java 21 alongside your system JDK

```bash
# Download Eclipse Temurin JDK 21
curl -sL "https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.7%2B6/OpenJDK21U-jdk_x64_linux_hotspot_21.0.7_6.tar.gz" \
  -o /tmp/openjdk21.tar.gz

# Extract into /usr/lib/jvm
sudo tar -xzf /tmp/openjdk21.tar.gz -C /usr/lib/jvm/
```

The `pyspark_dataframe()` function in `main.py` automatically sets `JAVA_HOME` to `/usr/lib/jvm/jdk-21.0.7+6` if that directory exists. No system-wide Java configuration changes are needed — your default Java 25 remains untouched.

## Project Structure

```
├── generate_dataset.py   # Creates the 1M-row CSV dataset (large_sales_data.csv)
├── benchmark.py          # Full benchmark: generates 100M-row CSV, times all libraries
├── main.py               # Quick runner: all five libraries on large_sales_data.csv
├── pyproject.toml        # Project metadata & dependencies
├── uv.lock               # Locked dependency versions
└── README.md
```

## License

This project is for educational and benchmarking purposes.
