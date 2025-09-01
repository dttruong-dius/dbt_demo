# Databricks notebook source
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from collections import defaultdict
from tqdm import tqdm
import json
import yaml

# COMMAND ----------

num_leads = spark.sql("SELECT count(*) from lakehouse_development.ml_features.analysis_job_lead_prices").toPandas()
num_leads = num_leads.iloc[0][0]

# COMMAND ----------

# Count the number jobs
num_jobs = spark.sql("SELECT count(DISTINCT job_id) from lakehouse_development.ml_features.analysis_job_lead_prices").toPandas()
num_jobs = num_jobs.iloc[0][0]

# COMMAND ----------

print(num_leads, num_jobs, num_leads / num_jobs)

# COMMAND ----------

# Fetch the claimed leads
claimed_leads = spark.sql("SELECT * from lakehouse_development.ml_features.analysis_job_lead_prices where job_lead_claimed = true").toPandas()
len(claimed_leads)

# COMMAND ----------

num_claimed_leads = len(claimed_leads)
num_clained_jobs = len(claimed_leads["job_id"].unique())
print(num_claimed_leads, num_clained_jobs, num_claimed_leads / num_clained_jobs)

# COMMAND ----------

zero_claims = num_jobs - num_clained_jobs
print(zero_claims, zero_claims / num_jobs)

# COMMAND ----------

num_claims = claimed_leads.groupby("job_id")["job_lead_claimed"].sum()
num_claims.hist(range=(0, 10))
num_claims.describe()

# COMMAND ----------

np.percentile(num_claims, 99), np.percentile(num_claims, 95)

# COMMAND ----------

# Count number of cases
# case 1: all claims in first round first batch
# case 2: at least one claim in the first round first batch and at least one claim in other batches of the first round
# case 3: claims in the first round first batch and other rounds
# case 4: claims in other rounds
cases = defaultdict(int)
for job_id, group in tqdm(claimed_leads.groupby(by="job_id")):
    claimed_in = (group["claimed_in"]).unique()
    if len(claimed_in) == 1 and "first-round-first-batch" in claimed_in:
        cases["case_1"] += 1
    elif "first-round-first-batch" in claimed_in and "first-round-other-batches" in claimed_in:
        cases["case_2"] += 1
    elif "first-round-first-batch" in claimed_in:
        cases["case_3"] += 1
    else:
        cases["case_4"] += 1
print(cases)


# COMMAND ----------

 len(claimed_leads["job_id"].unique()), len(claimed_leads.groupby(by="job_id")), sum(cases.values())

# COMMAND ----------

for case in sorted(cases.keys()):
    print(case, cases[case], cases[case] /  sum(cases.values()))

# COMMAND ----------

with open("price_settings.yml", "r") as f:
    price_settings = yaml.safe_load(f)
category_to_upper_bound = {d["master_category"]:d["price_higher"] for d in price_settings["pricing_info"]}
claimed_leads["upper_bound_price"] = claimed_leads["master_category"].map(category_to_upper_bound)

# COMMAND ----------

# Compute discounts for each job having the first-round-first-batch and first-round-other-batches
discount_rows = []
for job_id, group in claimed_leads.groupby(by="job_id"):
    claimed_in = list((group["claimed_in"]).unique())
    if "first-round-first-batch" in claimed_in and "first-round-other-batches" in claimed_in:
        first_batch = group[group["claimed_in"] == "first-round-first-batch"]
        other_batches = group[group["claimed_in"] == "first-round-other-batches"]
        mean_price = group["job_lead_price"].mean()
        other_batches_price = other_batches["job_lead_price"].mean()
        first_batch_price = first_batch["job_lead_price"].mean()
        prediction_over_upper_bound_ratio = first_batch_price / group["upper_bound_price"].iloc[0]
        discount_value_to_other_batches = first_batch_price - other_batches_price
        discount_percentage_to_other_batches = discount_value_to_other_batches / first_batch_price * 100
        discount_rows.append(dict(
            job_id=job_id,
            upper_bound_price=group["upper_bound_price"].iloc[0],
            first_batch_price=first_batch_price,
            mean_price=mean_price,
            other_batches_price=other_batches_price,
            discount_value_to_other_batches=discount_value_to_other_batches,
            discount_percentage_to_other_batches=discount_percentage_to_other_batches,
            prediction_over_upper_bound_ratio=prediction_over_upper_bound_ratio,
        ))
discount_df = pd.DataFrame(discount_rows)
discount_df.head()

# COMMAND ----------

len(discount_df), len(discount_df) / len(claimed_leads["job_id"].unique())

# COMMAND ----------

fig, ax = plt.subplots()
ax.hist(discount_df["prediction_over_upper_bound_ratio"].replace([np.inf, -np.inf], np.nan).dropna(), bins=np.arange(0, 1.4, 0.1))
ax.set_xlabel("prediction_over_upper_bound_ratio")
ax.set_ylabel("Number of jobs")
ax.grid("both")

# COMMAND ----------

fig, ax = plt.subplots()
ax.hist(discount_df["discount_value_to_other_batches"], bins=range(-40, 60, 10))
ax.set_xlabel("Discount value to other batches")
ax.set_ylabel("Number of jobs")
ax.grid("both")

# COMMAND ----------

fig, ax = plt.subplots()
ax.hist(discount_df["discount_percentage_to_other_batches"], range=(-60, 60), bins=range(-60, 60, 10))
ax.set_xlabel("Discount percentage to other batches")
ax.set_ylabel("Number of jobs")
ax.grid("both")

# COMMAND ----------

# job_types = spark.sql("SELECT * FROM lakehouse_production.ml_features.over_under_claim_raticate_payload where date >= '2025-01-01'").toPandas()
# len(job_types)

# COMMAND ----------

# price_predictions = spark.sql("select date, get_json_object(request, '$.dataframe_split.data[0][0]') as job_id, get_json_object(response, '$.predictions.final_price') as predicted_price from lakehouse_production.ml_features.persian_payload where date >= '2025-01-01' and status_code = '200'").toPandas()
# print(len(price_predictions))
# price_predictions.head()

# COMMAND ----------


