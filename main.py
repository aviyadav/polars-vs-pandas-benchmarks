import statistics as stats
import time

import numpy as np
import pandas as pd
import polars as pl


def bench(fn, warmups=1, runs=3):
    # warmup
    for _ in range(warmups):
        fn()
    # measure
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return stats.median(times)


N = 5_000_000  # scale knob; start at 2–5M rows
rng = np.random.default_rng(42)


def make_data():
    ids = rng.integers(0, 200_000, size=N, dtype=np.int32)
    cat = rng.choice(["a", "b", "c", "d", "e", "f"], size=N)
    val = rng.normal(loc=100, scale=15, size=N).astype(np.float32)
    ts = pd.to_datetime(1_725_000_000 + rng.integers(0, 86_400 * 10, N), unit="s")
    txt = rng.choice(["alpha", "beta", "gamma", "delta", "epsilon"], size=N)
    pdf = pd.DataFrame({"id": ids, "cat": cat, "val": val, "ts": ts, "txt": txt})
    plf = pl.from_pandas(pdf)  # Arrow zero-copy when possible
    return pdf, plf


pdf, plf = make_data()

# write files
pdf.to_csv("data.csv", index=False)
pdf.to_parquet("data.parquet")
plf.write_ipc("data.arrow")  # Arrow stream


def pandas_csv_read():
    pd.read_csv("data.csv")


def polars_csv_read():
    pl.read_csv("data.csv")


def pandas_parquet_read():
    pd.read_parquet("data.parquet")


def polars_ipc_read():
    pl.read_ipc("data.arrow")


def pandas_filter_compute():
    df = pdf
    out = df.loc[(df["val"] > 110) & (df["cat"].isin(["c", "d"]))].copy()
    out["score"] = (out["val"] - 100) * 1.2


def polars_filter_compute_eager():
    df = plf
    out = df.filter(
        (pl.col("val") > 110) & (pl.col("cat").is_in(["c", "d"]))
    ).with_columns(score=(pl.col("val") - 100) * 1.2)


def polars_filter_compute_lazy():
    df = plf.lazy()
    out = df.filter(
        (pl.col("val") > 110) & (pl.col("cat").is_in(["c", "d"]))
    ).with_columns(score=(pl.col("val") - 100) * 1.2)
    out.collect()


def pandas_groupby():
    (
        pdf.groupby(["cat"], as_index=False).agg(
            mean_val=("val", "mean"),
            std_val=("val", "std"),
            n=("id", "size"),
            nuniq=("id", "nunique"),
        )
    )


def polars_groupby_eager():
    (
        plf.group_by("cat").agg(
            pl.col("val").mean().alias("mean_val"),
            pl.col("val").std().alias("std_val"),
            pl.len().alias("n"),
            pl.col("id").n_unique().alias("nuniq"),
        )
    )


def polars_groupby_lazy():
    (
        plf.lazy()
        .group_by("cat")
        .agg(
            pl.col("val").mean().alias("mean_val"),
            pl.col("val").std().alias("std_val"),
            pl.len().alias("n"),
            pl.col("id").n_unique().alias("nuniq"),
        )
        .collect()
    )


dim = pd.DataFrame({"id": np.arange(200_000, dtype=np.int32)}).assign(
    weight=lambda d: (d["id"] % 17).astype(np.float32)
)
dim_pl = pl.from_pandas(dim)


def pandas_join():
    pdf.merge(dim, on="id", how="left", validate="many_to_one")


def polars_join_eager():
    plf.join(dim_pl, on="id", how="left", coalesce=True)


def polars_join_lazy():
    plf.lazy().join(dim_pl.lazy(), on="id", how="left").collect()


def pandas_window():
    df = pdf.copy()
    df["roll"] = df.sort_values("ts").set_index("ts")["val"].rolling("2h").mean().values
    df["rank_in_id"] = df.groupby("id")["val"].rank(method="first")


def polars_window_lazy():
    (
        plf.lazy()
        .with_columns(
            [
                pl.col("val").rolling_mean_by("ts", window_size="2h").alias("roll"),
                pl.col("val").rank("ordinal").over("id").alias("rank_in_id"),
            ]
        )
        .collect()
    )


def pandas_strings():
    s = pdf["txt"]
    _ = s.str.upper().str.contains("A", regex=False).astype("int8").to_frame()


def polars_strings():
    s = plf["txt"]
    _ = pl.DataFrame({"txt": s}).with_columns(
        pl.col("txt").str.to_uppercase().str.contains("A").cast(pl.Int8)
    )


def pandas_time_resample():
    df = pdf[["ts", "val"]].set_index("ts")
    _ = df.resample("5min").val.mean()


def polars_time_groupby():
    (
        plf.sort("ts")
        .group_by_dynamic(index_column="ts", every="5m", period="5m")
        .agg(pl.col("val").mean())
    )


def pandas_pivot():
    _ = pd.pivot_table(
        pdf, index="cat", columns=(pdf["id"] % 8), values="val", aggfunc="mean"
    )


def polars_pivot():
    df = plf.with_columns(bucket=(pl.col("id") % 8))
    _ = df.pivot(values="val", index="cat", on="bucket", aggregate_function="mean")


tests = [
    ("CSV read", lambda: pandas_csv_read(), lambda: polars_csv_read()),
    ("Arrow/Parquet", lambda: pandas_parquet_read(), lambda: polars_ipc_read()),
    (
        "Filter+Compute",
        lambda: pandas_filter_compute(),
        lambda: polars_filter_compute_lazy(),
    ),
    ("GroupBy", lambda: pandas_groupby(), lambda: polars_groupby_lazy()),
    ("Join", lambda: pandas_join(), lambda: polars_join_lazy()),
    ("Windows", lambda: pandas_window(), lambda: polars_window_lazy()),
    ("Strings", lambda: pandas_strings(), lambda: polars_strings()),
    ("Datetime", lambda: pandas_time_resample(), lambda: polars_time_groupby()),
    ("Pivot", lambda: pandas_pivot(), lambda: polars_pivot()),
]

for name, pd_fn, pl_fn in tests:
    t_pd = bench(pd_fn)
    t_pl = bench(pl_fn)
    print(
        f"{name:12s} | pandas {t_pd:6.3f}s  | polars {t_pl:6.3f}s  | speedup x{t_pd / max(t_pl, 1e-9):.1f}"
    )
