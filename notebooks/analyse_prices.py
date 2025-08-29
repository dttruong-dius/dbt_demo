# Databricks notebook source
from matplotlib import pyplot as plt

# COMMAND ----------

df = spark.sql("SELECT * from lakehouse_development.ml_features.analysis_job_lead_prices").toPandas()
len(df)

# COMMAND ----------

len(df), len(df["job_id"].unique()), len(df["lead_id"].unique())

# COMMAND ----------

count = 0
for idx, group in df.groupby(by="job_id"):
    if len(group) > 1 and group["is_other_rounds"].sum() > 0:
        print(idx)
        display(group)
        count += 1
    if count > 3:
        break

# COMMAND ----------


