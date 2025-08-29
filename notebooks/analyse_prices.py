# Databricks notebook source
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from collections import defaultdict
from tqdm import tqdm

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
# case 2: all claims in the first round
# case 3: some claims in the first round first batch and other batches or rounds
# case 4: others
cases = defaultdict(int)
for job_id, group in tqdm(claimed_leads.groupby(by="job_id")):
    claimed_in = (group["claimed_in"]).unique()
    if len(claimed_in) == 1 and "first-round-first-batch" in claimed_in:
        cases["case_1"] += 1
    if len(claimed_in) == 2 and "first-round-first-batch" in claimed_in and "first-round-other-batches" in claimed_in:
        cases["case_2"] += 1
    elif "first-round-first-batch" in claimed_in:
        cases["case_3"] += 1
    else:
        cases["case_4"] += 1
print(cases)


# COMMAND ----------

for case in sorted(cases.keys()):
    print(case, cases[case], cases[case] / sum(cases.values()))

# COMMAND ----------

# Compute discounts for each job having the first-round-first-batch and first-round-other-batches
discount_rows = []
for idx, group in claimed_leads.groupby(by="job_id"):
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

len(discount_df), len(discount_df) / len(claimed_leads["job_id"].unique())

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

job_types = spark.sql("SELECT * FROM lakehouse_production.ml_features.over_under_claim_raticate_payload where date >= '2025-01-01'").toPandas()

# COMMAND ----------

len(job_types)

# COMMAND ----------


