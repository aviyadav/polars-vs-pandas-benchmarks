# Polars vs Pandas Benchmarks

A comprehensive performance comparison between Polars and Pandas on various data manipulation operations using 5 million rows of synthetic data.

## Overview

This project benchmarks common data operations to compare the performance of [Polars](https://www.pola.rs/) (a blazingly fast DataFrame library written in Rust) against [Pandas](https://pandas.pydata.org/) (the traditional Python data manipulation library).

## Features

The benchmark suite tests the following operations:

1. **CSV Read** - Loading data from CSV files
2. **Arrow/Parquet** - Loading data from Parquet (Pandas) and Arrow IPC (Polars) files
3. **Filter+Compute** - Filtering rows and computing new columns
4. **GroupBy** - Aggregating data with multiple statistics
5. **Join** - Left join between main dataset and dimension table
6. **Windows** - Rolling windows and ranking operations
7. **Strings** - String manipulation and pattern matching
8. **Datetime** - Time-based resampling and grouping
9. **Pivot** - Pivot table operations

## Dataset

The synthetic dataset contains **5 million rows** with the following columns:

- `id`: Random integers (0-200,000) representing entity IDs
- `cat`: Random categories (a-f)
- `val`: Normal distribution values (mean=100, std=15)
- `ts`: Random timestamps over a 10-day period
- `txt`: Random text values (alpha, beta, gamma, delta, epsilon)

## Benchmark Results

Results from a typical run on 5 million rows:

| Operation      | Pandas Time | Polars Time | Speedup |
|----------------|-------------|-------------|---------|
| CSV read       | 6.266s      | 0.204s      | **30.8x** |
| Arrow/Parquet  | 0.198s      | 0.001s      | **207.5x** |
| Filter+Compute | 0.113s      | 0.079s      | **1.4x** |
| GroupBy        | 0.422s      | 0.206s      | **2.0x** |
| Join           | 0.674s      | 0.060s      | **11.2x** |
| Windows        | 5.139s      | 1.077s      | **4.8x** |
| Strings        | 0.249s      | 0.283s      | **0.9x** |
| Datetime       | 2.563s      | 0.688s      | **3.7x** |
| Pivot          | 0.297s      | 0.229s      | **1.3x** |

### Key Takeaways

- **Polars dominates in I/O operations**: 30-200x faster for reading files
- **Joins are significantly faster**: 11x speedup
- **Window operations**: Nearly 5x faster
- **Time-series operations**: 3.7x faster
- **String operations**: Comparable performance (Pandas slightly faster in this case)
- **Overall**: Polars is faster in 8 out of 9 benchmark categories

## Installation

### Prerequisites

- Python 3.8+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

### Using uv (recommended)

```bash
# Clone the repository
git clone <repository-url>
cd polars-vs-pandas-benchmarks-reproduced

# Install dependencies
uv sync

# Run benchmarks
uv run python main.py
```

### Using pip

```bash
# Clone the repository
git clone <repository-url>
cd polars-vs-pandas-benchmarks-reproduced

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install pandas polars numpy

# Run benchmarks
python main.py
```

## Project Structure

```
.
├── main.py           # Benchmark suite implementation
├── README.md         # This file
├── pyproject.toml    # Project dependencies
├── data.csv          # Generated CSV file (~212MB)
├── data.parquet      # Generated Parquet file (~70MB)
└── data.arrow        # Generated Arrow IPC file (~229MB)
```

## Customization

### Adjusting Dataset Size

You can modify the dataset size by changing the `N` variable in `main.py`:

```python
N = 5_000_000  # Change this value (e.g., 1_000_000 for 1M rows)
```

### Adjusting Benchmark Runs

Modify the `bench()` function parameters:

```python
def bench(fn, warmups=1, runs=3):  # Adjust warmups and runs
    # ...
```

## Technical Details

### Key Polars Features Used

- **Lazy evaluation**: Using `.lazy()` for query optimization
- **Expression API**: Composable operations with `pl.col()`
- **Time-based rolling windows**: `rolling_mean_by()` for temporal aggregations
- **Dynamic grouping**: `group_by_dynamic()` for time-series resampling
- **Zero-copy operations**: Arrow memory format for efficient data transfer

### Benchmark Methodology

Each benchmark:
1. Performs 1 warmup run to ensure JIT compilation and caching
2. Executes 3 timed runs
3. Reports the median time to reduce variance
4. Compares equivalent operations between Pandas and Polars

## Dependencies

- **pandas**: Traditional DataFrame library for Python
- **polars**: Fast DataFrame library with Rust backend
- **numpy**: Numerical computing for data generation
- **pyarrow** (optional): Enhanced Parquet support

## Contributing

Feel free to:
- Add new benchmark scenarios
- Optimize existing implementations
- Report issues or inconsistencies
- Suggest improvements

## License

This project is available for educational and benchmarking purposes.

## Resources

- [Polars Documentation](https://pola-rs.github.io/polars/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Polars vs Pandas Comparison](https://pola-rs.github.io/polars/py-polars/html/reference/api/comparison-with-pandas.html)

## Notes

- Results may vary based on hardware, OS, and Python/library versions
- Polars benefits from parallel execution on multi-core systems
- Memory usage is not measured in this benchmark suite
- Some operations are inherently sequential and may not show dramatic speedups