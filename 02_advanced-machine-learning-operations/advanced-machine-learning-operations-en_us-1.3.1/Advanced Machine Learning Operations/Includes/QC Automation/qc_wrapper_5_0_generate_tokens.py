# Databricks notebook source
# MAGIC %md
# MAGIC # QC Wrapper — M05: 0 - Generate Tokens
# MAGIC
# MAGIC The `0 - Generate Tokens` notebook collects a PAT through an interactive widget, which cannot be filled in during a job run, so `var/credentials.cfg` is never written. The 5.2 Demo then fails with `KeyError: 'db_token'`.
# MAGIC
# MAGIC This wrapper writes `var/credentials.cfg` (in the same `[DEFAULT]` format the course uses) for the M02 and M05 folders using the job run's own notebook-context token and workspace URL, and then runs the M05 `0 - Generate Tokens` notebook so it is still validated. The token is never printed.

# COMMAND ----------

import os

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
qc_root = "/Workspace" + "/".join(ctx.notebookPath().get().split("/")[:-1])
course_root = os.path.normpath(f"{qc_root}/../..")

# Runtime credentials of the job run (not hard-coded)
db_token = ctx.apiToken().get()
db_instance = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"

contents = f"""
[DEFAULT]
db_token = {db_token}
db_instance = {db_instance}
"""

module_folders = [
    "M02 - Streamlining MLOps with Databricks",
    "M05 - Build ML Assets as Code",
]

for folder in module_folders:
    var_dir = f"{course_root}/{folder}/var"
    os.makedirs(var_dir, exist_ok=True)
    cfg_path = f"{var_dir}/credentials.cfg"
    with open(cfg_path, "w") as f:
        f.write(contents)
    print(f"Credentials file written: {cfg_path}")

# Validate the M05 Generate Tokens notebook itself
gen_tokens_nb = f"{course_root}/M05 - Build ML Assets as Code/0 - Generate Tokens".replace("/Workspace", "", 1)
print(f"Running: {gen_tokens_nb}")
result = dbutils.notebook.run(gen_tokens_nb, timeout_seconds=1800)
print(f"0 - Generate Tokens completed. Result: {result}")