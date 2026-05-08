# Pandas vs Polars — Real-World Performance Comparison

A hands-on benchmark comparing **Pandas** and **Polars** running the same data pipeline across dataset sizes from small (100K rows) all the way to large (100M+ rows).

---

## What is being benchmarked?

Every run executes the same three-step pipeline against a synthetic sales CSV:

1. **Compute** `net_revenue = quantity × sales × (1 - 0.075)`
2. **Flag** each order as `"high"` or `"low"` based on a revenue threshold
3. **Aggregate** total net revenue grouped by `region`

The same logic is expressed four ways:

| Approach | File | How it works |
|---|---|---|
| **Pandas** | `main.py` | Loads the full CSV into a DataFrame, applies NumPy vectorised ops |
| **Polars — Eager** | `main.py` | Loads the full CSV, executes immediately like Pandas |
| **Polars — Lazy** | `main.py` | Builds a query plan first; only reads needed columns (projection pushdown) |
| **Polars — Streaming** | `main_2.py` | Executes in fixed-size chunks; the full file never lives in RAM |

---

## Dataset

The synthetic dataset represents sales orders with these columns:

| Column | Type | Description |
|---|---|---|
| `order_id` | int | Unique order identifier |
| `order_date` | datetime | One order per minute from 2022-01-01 |
| `region` | str | North / South / East / West |
| `category` | str | Electronics / Clothing / Furniture / Food / Sports |
| `sales` | int | Unit price (100 – 10,000) |
| `quantity` | int | Units sold (1 – 20) |
| `discount` | float | Applied discount (0 – 0.5) |
| `status` | str | Completed / Returned / Pending / Cancelled |

---

## Quickstart

### 1 — Generate a dataset

```bash
python generate-dataset.py
```

Datasets of different sizes expose different performance characteristics. Edit the call at the bottom of `generate-dataset.py` to control the scale:

```python
gen_large_dataset_parallel(
    total_rows=1_000_000,    # change this to vary the dataset size
    batch_size=500_000,       # rows per worker batch — tune to available RAM
    num_workers=4,            # parallel workers — tune to CPU core count
)
```

Suggested sizes to run the benchmark at:

| Label | `total_rows` | Approx CSV size |
|---|---|---|
| Small | `1_000_000` | ~70 MB |
| Medium | `10_000_000` | ~700 MB |
| Large | `100_000_000` | ~7 GB |

> **RAM guidance** — keep `batch_size × num_workers` comfortably below your free RAM.
> A `batch_size` of `5_000_000` uses ~400 MB per worker.

### 2 — Run the benchmark

```bash
python main.py      # Pandas  vs  Polars Eager  vs  Polars Lazy
python main_2.py    # Polars Lazy  vs  Polars Streaming
```

---

## How the data generation works

Generating hundreds of millions of rows in one shot would itself crash the system, so `generate-dataset.py` uses **batched multiprocessing**:

1. The target row count is split into equal-sized batches.
2. A `ProcessPoolExecutor` assigns each batch to a worker process.
3. Every worker generates its batch with Polars and writes it to a temporary CSV.
4. Once all workers finish, Polars lazy I/O concatenates the batch files into a single `large_sales_data.csv` and deletes the temporaries.

```
total_rows = 100_000_000,  batch_size = 5_000_000,  num_workers = 4

 Worker 0 → batch_0.csv (5 M rows)  ┐
 Worker 1 → batch_1.csv (5 M rows)  ├─ run in parallel
 Worker 2 → batch_2.csv (5 M rows)  │
 Worker 3 → batch_3.csv (5 M rows)  ┘
 ... (20 batches total)
 → combine → large_sales_data.csv
```

---

## What to expect at each scale

Performance characteristics shift significantly as the dataset grows.

### Small (~1 M rows)

At this scale everything is fast and the differences are minor. Pandas is familiar and perfectly capable.

| Approach | Expected time |
|---|---|
| Pandas | < 1 s |
| Polars Eager | < 1 s |
| Polars Lazy | < 1 s |
| Polars Streaming | < 1 s |

### Medium (~10 M rows)

Polars' multi-threaded execution and columnar layout start to pull ahead of Pandas.

| Approach | Expected time |
|---|---|
| Pandas | 5 – 15 s |
| Polars Eager | 2 – 5 s |
| Polars Lazy | 1 – 3 s |
| Polars Streaming | 1 – 3 s |

### Large (~100 M rows)

At this scale Pandas risks running out of memory. Polars Lazy and Streaming handle it comfortably due to projection pushdown and chunked execution.

| Approach | Expected time | Memory risk |
|---|---|---|
| Pandas | 60 – 120 s | ⚠️ High — loads full CSV |
| Polars Eager | 20 – 40 s | ⚠️ High — loads full CSV |
| Polars Lazy | 8 – 15 s | ✅ Low — reads only needed columns |
| Polars Streaming | 2 – 5 s | ✅ Very low — fixed chunk size |

> Times measured on a 4-core / 16 GB machine. Your numbers will differ.

---

## Key concepts explained

### Projection pushdown
Polars Lazy inspects the query plan and avoids reading columns that are never used. For this benchmark only `region`, `sales`, and `quantity` are needed — `order_id`, `order_date`, `discount`, `status`, and `category` are skipped entirely when reading from disk.

### Streaming execution (`engine="streaming"`)
Instead of loading the whole file into a single in-memory DataFrame, the streaming engine processes a fixed-size chunk at a time, merges partial aggregations, and discards each chunk before loading the next. Peak memory is proportional to the chunk size, not the file size.

### Why Polars is faster than Pandas
- Written in Rust — no Python overhead in the hot path
- Columnar, Arrow-based memory layout — SIMD-friendly
- Fully multi-threaded by default
- Query optimiser rewrites the plan before touching data

---

## Files

| File | Purpose |
|---|---|
| `generate-dataset.py` | Batched, parallel dataset generation using Polars |
| `main.py` | Pandas vs Polars Eager vs Polars Lazy |
| `main_2.py` | Polars Lazy vs Polars Streaming |
| `pyproject.toml` | Project dependencies |

---

## When to use what

| Scenario | Recommendation |
|---|---|
| Dataset fits easily in RAM, quick exploration | Pandas or Polars Eager |
| Dataset fits in RAM, need speed | Polars Lazy |
| Dataset approaches or exceeds available RAM | Polars Streaming |
| Hundreds of millions of rows or more | Polars Streaming + Parquet instead of CSV |
