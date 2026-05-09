import time

import numpy as np
import pandas as pd

np.random.seed(42)

n = 1_000_000
regions = ["north", "south", "east", "west"]
categories = ["electronics", "clothing", "furniture", "food", "sports"]
statuses = ["completed", "returned", "pending", "cancelled"]

df = pd.DataFrame(
    {
        "order_id": np.arange(1000, 1000 + n),
        "order_date": pd.date_range(start="2022-01-01", periods=n, freq="1min"),
        "region": np.random.choice(regions, size=n),
        "category": np.random.choice(categories, size=n),
        "sales": np.random.randint(100, 10000, size=n),
        "quantity": np.random.randint(1, 20, size=n),
        "discount": np.round(np.random.uniform(0.0, 0.5, size=n), 2),
        "status": np.random.choice(statuses, size=n),
    }
)

df.to_csv("large_sales_data.csv", index=False)
df.to_csv('large_sales_data.csv', index=False)
