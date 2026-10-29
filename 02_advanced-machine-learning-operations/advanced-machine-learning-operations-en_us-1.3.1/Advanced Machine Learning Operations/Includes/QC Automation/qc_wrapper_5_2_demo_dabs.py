# Databricks notebook source
# MAGIC %md
# MAGIC # QC Wrapper — M05: 5.2 Demo - Working with Declarative Automation Bundles
# MAGIC
# MAGIC In a job run, the 5.2 Demo cells execute back-to-back. `databricks bundle init default-python` reports success, but the generated `my_project` folder is not yet visible to the next cells (`cd my_project` fails, then `DA.update_bundle()` raises `FileNotFoundError: my_project/resources/my_project.job.yml`).
# MAGIC
# MAGIC This wrapper leaves the course notebook untouched. It pre-creates the `my_project` bundle from the same template (CLI v0.298.0), **waits until the files are visible** on the workspace filesystem, and then runs the 5.2 Demo via `dbutils.notebook.run()`. The demo's own `bundle init` then finds the project already in place, and the remaining cells (validate, `update_bundle`, deploy, run, destroy) work on it. The token is never printed.

# COMMAND ----------

import os
import subprocess
import time

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
qc_root = "/Workspace" + "/".join(ctx.notebookPath().get().split("/")[:-1])
m05_dir = os.path.normpath(f"{qc_root}/../../M05 - Build ML Assets as Code")
bundle_dir = f"{m05_dir}/my_project"
demo_notebook = f"{m05_dir}/5.2 Demo - Working with Declarative Automation Bundles".replace("/Workspace", "", 1)

# CLI auth for the subprocesses only, using the job run's own credentials (not printed)
cli_env = os.environ.copy()
cli_env["DATABRICKS_HOST"] = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
cli_env["DATABRICKS_TOKEN"] = ctx.apiToken().get()

CLI_VERSION = "0.298.0"  # same version the 5.2 Demo installs


def sh(cmd, check=True):
    """Run a bash command in the M05 folder and echo its output."""
    r = subprocess.run(["bash", "-c", cmd], cwd=m05_dir, env=cli_env, capture_output=True, text=True)
    if r.stdout.strip():
        print(r.stdout.strip())
    if r.stderr.strip():
        print(r.stderr.strip())
    if check and r.returncode != 0:
        raise RuntimeError(f"Command failed (exit {r.returncode}): {cmd}")
    return r


# 1. Ensure the Databricks CLI (same version as the demo) is installed
r = sh("databricks version", check=False)
if r.returncode != 0 or CLI_VERSION not in r.stdout:
    print(f"Installing Databricks CLI v{CLI_VERSION} ...")
    sh(
        "which databricks && rm $(which databricks); "
        "[ -e $HOME/bin/databricks ] && rm $HOME/bin/databricks; "
        f"curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v{CLI_VERSION}/install.sh | sh; "
        "ln -sf $HOME/bin/databricks /usr/local/bin/databricks",
        check=False,
    )
    sh("databricks version")

# 2. Pre-create the bundle from the template (skip if it already exists)
if os.path.exists(f"{bundle_dir}/databricks.yml"):
    print("my_project already exists - skipping bundle init.")
else:
    sh("databricks bundle init default-python")

# 3. Wait until the generated files are visible on the workspace filesystem
TIMEOUT_S, INTERVAL_S = 180, 3
start = time.time()
while not (os.path.exists(f"{bundle_dir}/databricks.yml") and os.path.isdir(f"{bundle_dir}/resources")):
    if time.time() - start > TIMEOUT_S:
        raise FileNotFoundError(f"{bundle_dir}/databricks.yml not visible after {TIMEOUT_S}s")
    time.sleep(INTERVAL_S)
print(f"Bundle files visible after {time.time() - start:.0f}s: {sorted(os.listdir(bundle_dir))}")

# 4. Run the 5.2 Demo notebook (failures propagate so the job task fails on real errors)
print(f"Running: {demo_notebook}")
result = dbutils.notebook.run(demo_notebook, timeout_seconds=3600)
print(f"5.2 Demo completed. Result: {result}")