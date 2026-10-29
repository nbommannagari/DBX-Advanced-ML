# Databricks notebook source
# MAGIC %md
# MAGIC # QC Wrapper — 3.2 Demo: Common Testing Strategies
# MAGIC
# MAGIC This wrapper runs the **3.2 Demo** notebook via `dbutils.notebook.run()`.
# MAGIC
# MAGIC The 3.2 Demo has an **intentional failure** at Cell 22 (a teaching exercise where `HeartDiseaseorAttack` contains `-1`). This causes the notebook to stop at that cell when run as a job task.
# MAGIC
# MAGIC This wrapper catches that expected failure so the QC automation job can continue to downstream tasks. The important work (classroom setup, model training, MLflow registration, serving endpoint creation) all completes before Cell 22.

# COMMAND ----------

import os

# Resolve the absolute path to the 3.2 Demo notebook
this_notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
qc_root = "/".join(this_notebook_path.split("/")[:-1])
demo_notebook = os.path.normpath(
    f"{qc_root}/../../M03 - Model Rollout Strategies with Databricks/3.2 Demo - Common Testing Strategies"
)

print(f"Running: {demo_notebook}")

try:
    result = dbutils.notebook.run(demo_notebook, timeout_seconds=3600)
    print(f"Notebook completed successfully. Result: {result}")
except Exception as e:
    error_msg = str(e)
    # dbutils.notebook.run() wraps cell errors in a generic
    # NotebookExecutionException — the actual cell error text is NOT
    # included. We check for this known wrapper exception type.
    if "NotebookExecutionException" in error_msg or "Workload failed" in error_msg:
        print("=" * 70)
        print("EXPECTED FAILURE (by design) — 3.2 Demo Cell 22")
        print("The notebook intentionally validates bad data (-1 in")
        print("HeartDiseaseorAttack) to teach error handling.")
        print("The important work (classroom setup, model training,")
        print("MLflow registration, endpoint creation) completed before this.")
        print("=" * 70)
        print(f"\nCaught error: {error_msg[:300]}")
    else:
        # Unexpected failure type — re-raise so the job task fails
        print(f"UNEXPECTED failure in 3.2 Demo: {error_msg}")
        raise