# Databricks notebook source
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

# COMMAND ----------

df = spark.sql("SELECT * from lakehouse_development.ml_features.analysis_job_lead_prices").toPandas()
len(df)

# COMMAND ----------

len(df), len(df["job_id"].unique()), len(df["lead_id"].unique())

# COMMAND ----------

num_claims = df.groupby("job_id")["job_lead_claimed"].sum()
num_claims.hist(range=(0, 10))
num_claims.describe()

# COMMAND ----------

np.percentile(num_claims, 99), np.percentile(num_claims, 95)

# COMMAND ----------

# Compute discounts for each job having the first-round-first-batch and first-round-other-batches
discount_rows = []
for idx, group in df.groupby(by="job_id"):
    if len(group) > 1 and "first-round-first-batch" in list(group["claimed_in"]) and "first-round-other-batches" in list(group["claimed_in"]):
        first_batch = group[group["claimed_in"] == "first-round-first-batch"]
        other_batches = group[group["claimed_in"] == "first-round-other-batches"]
        mean_price = group["job_lead_price"].mean()
        first_batch_price = first_batch["job_lead_price"].mean()
        absolute_discount = first_batch_price - mean_price
        discount_percentage = absolute_discount / first_batch_price * 100
        discount_rows.append(dict(first_batch_price=first_batch_price, mean_price=mean_price, absolute_discount=absolute_discount, discount_percentage=discount_percentage))
discount_df = pd.DataFrame(discount_rows)

# COMMAND ----------

fig, ax = plt.subplots()
ax.hist(discount_df["absolute_discount"], bins=range(-40, 60, 10))
ax.set_xlabel("Absolute discount")
ax.set_ylabel("Number of jobs")
ax.grid("both")

# COMMAND ----------

fig, ax = plt.subplots()
ax.hist(discount_df["discount_percentage"], range=(-60, 60), bins=range(-60, 60, 10))
ax.set_xlabel("Discount percentage")
ax.set_ylabel("Number of jobs")
ax.grid("both")

# COMMAND ----------


