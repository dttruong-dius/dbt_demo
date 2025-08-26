from databricks.sdk import WorkspaceClient

# Authenticate (relies on DATABRICKS_HOST and DATABRICKS_TOKEN env vars)
w = WorkspaceClient()

# Get your warehouse ID (pick one from your workspace)
# You can list warehouses like this:
warehouses = list(w.warehouses.list())
for wh in warehouses:
    print(wh.name, wh.id)

# Replace with your SQL Warehouse ID (string)
WAREHOUSE_ID = "8b5bb45ca0974c4c"

# Create a Lakeflow Job with dbt tasks
job = w.jobs.create(
    name="dbt_sql_warehouse_job",
    tasks=[
        {
            "task_key": "build_table_1",
            "description": "Run dbt build for table_1",
            "dbt_task": {
                "commands": ["dbt build --select table_1"],
                "project_directory": "/Workspace/Repos/duytintruong@hipagesgroup.com.au/dbt_demo",
                "profiles_directory": "/Workspace/Repos/duytintruong@hipagesgroup.com.au/dbt_demo",
            },
            "warehouse_id": WAREHOUSE_ID,
        },
        {
            "task_key": "build_table_2",
            "description": "Run dbt build for table_2",
            "dbt_task": {
                "commands": ["dbt build --select table_2"],
                "project_directory": "/Workspace/Repos/duytintruong@hipagesgroup.com.au/dbt_demo",
                "profiles_directory": "/Workspace/Repos/duytintruong@hipagesgroup.com.au/dbt_demo",
            },
            "warehouse_id": WAREHOUSE_ID,
        },
        {
            "task_key": "build_table_3",
            "description": "Run dbt build for table_3 (depends on 1 & 2)",
            "depends_on": [
                {"task_key": "build_table_1"},
                {"task_key": "build_table_2"},
            ],
            "dbt_task": {
                "commands": ["dbt build --select table_3"],
                "project_directory": "/Workspace/Repos/duytintruong@hipagesgroup.com.au/dbt_demo",
                "profiles_directory": "/Workspace/Repos/duytintruong@hipagesgroup.com.au/dbt_demo",
            },
            "warehouse_id": WAREHOUSE_ID,
        },
    ],
)

print(f"✅ Created job with ID: {job.job_id}")

run = w.jobs.run_now(job_id=job.job_id)
print(f"▶️ Job started: run_id={run.run_id}")
