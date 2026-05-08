import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl


def gen_batch(
    batch_num: int,
    batch_size: int,
    total_rows: int,
    start_order_id: int,
    start_date: str,
):
    """Generate a batch of data using Polars."""
    np.random.seed(42 + batch_num)

    regions = ["North", "South", "East", "West"]
    categories = ["electronics", "clothing", "furniture", "food", "sports"]
    statuses = ["completed", "returned", "pending", "cancelled"]

    # Calculate the actual number of rows in this batch
    actual_batch_size = min(batch_size, total_rows - (batch_num * batch_size))

    # Calculate start and end datetime objects for this batch
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    # Offset start by the batch's position to get continuous dates across batches
    start_dt = start_dt + timedelta(minutes=batch_num * batch_size)
    end_dt = start_dt + timedelta(minutes=actual_batch_size - 1)

    # Generate data in-memory for this batch
    df = pl.DataFrame(
        {
            "order_id": np.arange(start_order_id, start_order_id + actual_batch_size),
            "order_date": pl.datetime_range(
                start=start_dt,
                end=end_dt,
                interval="1m",
                eager=True,
            ),
            "region": np.random.choice(regions, size=actual_batch_size),
            "category": np.random.choice(categories, size=actual_batch_size),
            "sales": np.random.randint(100, 10000, size=actual_batch_size),
            "quantity": np.random.randint(1, 20, size=actual_batch_size),
            "discount": np.round(np.random.uniform(0, 0.5, size=actual_batch_size), 2),
            "status": np.random.choice(statuses, size=actual_batch_size),
        }
    )

    # Write batch to file
    batch_filename = f"large_sales_data_batch_{batch_num}.csv"
    df.write_csv(batch_filename)
    return batch_filename, len(df)


def gen_large_dataset_parallel(
    total_rows: int = 100_000_000,
    batch_size: int = 5_000_000,
    num_workers: int | None = None,
    output_file: str = "large_sales_data.csv",
):
    """
    Generate a large dataset in parallel batches.

    Args:
        total_rows: Total number of rows to generate
        batch_size: Number of rows per batch (keep this reasonable for memory)
        num_workers: Number of parallel processes (default: CPU count)
        output_file: Final output filename (combined batches)
    """
    if num_workers is None:
        import os

        num_workers = os.cpu_count() or 4
        num_workers = int(num_workers)  # Ensure it's an int

    num_batches = (total_rows + batch_size - 1) // batch_size  # Ceiling division

    print(f"Generating {total_rows:,} rows in {num_batches} batches")
    print(f"Using {num_workers} parallel workers")
    print(f"Batch size: {batch_size:,} rows")

    batch_files = []

    # Generate batches in parallel
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for batch_num in range(num_batches):
            start_order_id = 100000 + (batch_num * batch_size)
            future = executor.submit(
                gen_batch,
                batch_num=batch_num,
                batch_size=batch_size,
                total_rows=total_rows,
                start_order_id=start_order_id,
                start_date="2022-01-01",
            )
            futures.append(future)

        # Collect results as they complete
        for i, future in enumerate(as_completed(futures), 1):
            batch_file, rows_written = future.result()
            batch_files.append(batch_file)
            print(
                f"Completed batch {i}/{num_batches}: {rows_written:,} rows written to {batch_file}"
            )

    # Combine all batch files into one
    print("\nCombining batch files...")
    start_time = time.time()

    # Read all batch files lazily and write to final file
    dfs = [pl.scan_csv(f) for f in batch_files]
    combined_df = pl.concat(dfs)
    combined_df.sink_csv(output_file)

    # Clean up batch files
    for batch_file in batch_files:
        Path(batch_file).unlink()

    end_time = time.time()
    print(f"Combined file created: {output_file}")
    print(f"Combination took {end_time - start_time:.2f} seconds")


def gen_large_dataset_simple(
    total_rows: int = 100_000_000,
    batch_size: int = 1_000_000,
    output_file: str = "large_sales_data.csv",
):
    """
    Simple batched version without multiprocessing.
    Use this if multiprocessing causes issues on your system.
    """
    print(f"Generating {total_rows:,} rows in batches of {batch_size:,}")

    batch_files = []
    num_batches = (total_rows + batch_size - 1) // batch_size

    for batch_num in range(num_batches):
        batch_file, rows_written = gen_batch(
            batch_num=batch_num,
            batch_size=batch_size,
            total_rows=total_rows,
            start_order_id=100000 + (batch_num * batch_size),
            start_date="2022-01-01",
        )
        batch_files.append(batch_file)
        print(f"Batch {batch_num + 1}/{num_batches}: {rows_written:,} rows")

    # Combine batch files
    print("\nCombining batch files...")
    start_time = time.time()

    dfs = [pl.scan_csv(f) for f in batch_files]
    combined_df = pl.concat(dfs)
    combined_df.sink_csv(output_file)

    for batch_file in batch_files:
        Path(batch_file).unlink()

    end_time = time.time()
    print(f"Combined file created: {output_file}")
    print(f"Combination took {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    # For testing with smaller dataset
    # gen_large_dataset_simple(total_rows=10_000_000, batch_size=1_000_000)

    # For production with full dataset (1 billion rows)
    # Note: This will take a while and create a ~100GB+ CSV file
    # Adjust batch_size based on your available RAM
    start_time = time.time()

    # Use multiprocessing version for speed
    gen_large_dataset_parallel(
        total_rows=100_000_000,  # Start with 100M for testing, increase to 1B when ready
        batch_size=5_000_000,  # Adjust based on your RAM (5M rows ~ 400MB in memory)
        num_workers=16,  # Adjust based on your CPU cores
    )

    end_time = time.time()
    print(f"\nTotal time: {end_time - start_time:.2f} seconds")
