# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

class Token:
    def __init__(self, config_path='../M01 - Streamlining MLOps with Databricks/var/credentials.cfg'):
        self.config_path = config_path
        self.token = self.get_credentials()

    def get_credentials(self):
        import configparser
        import os

        # Check if the file exists

        if os.path.exists(self.config_path):
            print("The configuration file was found in the specified path: M02 - Streamlining MLOps with Databricks/var/credentials.cfg")

        elif not os.path.exists(self.config_path):
            print("The configuration file was not found in the specified path. Trying secondary path...")
            
            config_path = './var/credentials.cfg'
            self.config_path = config_path

            print(f"The configuration file was found in the specified path: M05/var/credentials.cfg")
        else:
            raise FileNotFoundError(f"The configuration file was not found at {self.config_path}")

        config = configparser.ConfigParser()
        config.read(self.config_path)

        if 'DEFAULT' not in config:
            raise KeyError("The section 'DEFAULT' was not found in the configuration file.")

        if 'db_token' not in config['DEFAULT']:
            raise KeyError("The key 'db_token' was not found in the 'DEFAULT' section of the configuration file.")

        token = config['DEFAULT']['db_token']
        
        # Print the token for debugging purposes
        print(f"db_token: {token}")

        return token

# COMMAND ----------

# Use the Token class to get the token
token_obj = Token()
token = token_obj.token

# COMMAND ----------

def init_DA(name, reset=True, pipeline=False):
    DA = DBAcademyHelper()
    DA.init()

    if pipeline:
        DA.paths.stream_source = f"{DA.paths.working_dir}/stream_source"
        DA.paths.storage_location = f"{DA.paths.working_dir}/storage_location"
        DA.pipeline_name = f"{DA.unique_name(sep='-')}: ETL Pipeline - {name.upper()}"
        DA.lookup_db = f"{DA.unique_name(sep='_')}_lookup"

        if reset:  # create lookup tables
            import pandas as pd

            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DA.lookup_db}")
            DA.clone_source_table(f"{DA.lookup_db}.date_lookup", source_name="date-lookup")
            DA.clone_source_table(f"{DA.lookup_db}.user_lookup", source_name="user-lookup")

            df = spark.createDataFrame(pd.DataFrame([
                ("device_id_not_null", "device_id IS NOT NULL", "validity", "bpm"),
                ("device_id_valid_range", "device_id > 110000", "validity", "bpm"),
                ("heartrate_not_null", "heartrate IS NOT NULL", "validity", "bpm"),
                ("user_id_not_null", "user_id IS NOT NULL", "validity", "workout"),
                ("workout_id_not_null", "workout_id IS NOT NULL", "validity", "workout"),
                ("action_not_null","action IS NOT NULL","validity","workout"),
                ("user_id_not_null","user_id IS NOT NULL","validity","user_info"),
                ("update_type_not_null","update_type IS NOT NULL","validity","user_info")
            ], columns=["name", "condition", "tag", "topic"]))
            df.write.mode("overwrite").saveAsTable(f"{DA.lookup_db}.rules")
        
    return DA

None

# COMMAND ----------

import os
from urllib.parse import urlparse, urlunsplit
import ipywidgets as widgets

# set CWD to folder containing notebook; this can go away for DBR14.x+ as its default behavior
os.chdir('/Workspace' + os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()) + '/')

# Create Credentials File Method
@DBAcademyHelper.add_method
def load_credentials(self):
    import configparser
    c = configparser.ConfigParser()
    c.read(filenames=f"var/credentials.cfg")
    try:
        token = c.get('DEFAULT', 'db_token')
        host = c.get('DEFAULT', 'db_instance')
        os.environ["DATABRICKS_HOST"] = host
        os.environ["DATABRICKS_TOKEN"] = token
    except:
        token = ''
        host = f'https://{spark.conf.get("spark.databricks.workspaceUrl")}'

    return token, host    
@DBAcademyHelper.add_method
def create_credentials_file(self, db_token, db_instance):
    contents = f"""
db_token = "{db_token}"
db_instance = "{db_instance}"
    """
    self.write_to_file(contents, "credentials.py")
None

# Get Credentials Method
@DBAcademyHelper.add_method
def get_credentials(self):
    (current_token, current_host) = self.load_credentials()

    @widgets.interact(host=widgets.Text(description='Host:',
                                        placeholder='Paste workspace URL here',
                                        value=current_host,
                                        continuous_update=False),
                      token=widgets.Password(description='Token:',
                                             placeholder='Paste PAT here',
                                             value=current_token,
                                             continuous_update=False)
    )
    def _f(host='', token=''):
        u = urlparse(host)
        host = urlunsplit((u.scheme, u.netloc, '', '', ''))

        if host and token:
            os.environ["DATABRICKS_HOST"] = host
            os.environ["DATABRICKS_TOKEN"] = token

            contents = f"""
[DEFAULT]
db_token = {token}
db_instance = {host}
            """
            # Save credentials in the new location
            os.makedirs('./var', exist_ok=True)
            with open("./var/credentials.cfg", "w") as f:
                print(f"Credentials stored ({f.write(contents)} bytes written).")

None

# COMMAND ----------

@DBAcademyHelper.add_method
def update_bundle(self):
    import ipywidgets as widgets
    import os
    import shutil
    import subprocess
    import json

    def try_remove(file_path):
        """Attempt to remove a file, ignoring errors if it doesn't exist."""
        try:
            os.remove(file_path)
            print(f"Removed: {file_path}")
        except FileNotFoundError:
            print(f"File not found, skipping: {file_path}")
        except PermissionError:
            print(f"Permission denied, could not remove: {file_path}")
        except Exception as e:
            print(f"Error removing {file_path}: {e}")

    def try_rmdir(dir_path):
        """Attempt to remove a directory and all its contents."""
        try:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
                print(f"Removed directory: {dir_path}")
            else:
                print(f"Directory not found, skipping: {dir_path}")
        except Exception as e:
            print(f"Error removing directory {dir_path}: {e}")

    # List of files to remove (updated for CLI v0.298.0 default-python template)
    files_to_remove = [
        "my_project/resources/sample_job.job.yml",
        "my_project/resources/my_project_etl.pipeline.yml",
        "my_project/src/sample_notebook.ipynb",
    ]

    # List of directories to remove (updated for CLI v0.298.0 template)
    dirs_to_remove = [
        "my_project/src/my_project_etl",
        "my_project/.vscode",
        "my_project/fixtures",
        "my_project/tests",
    ]

    # Try to remove each file
    for file_path in files_to_remove:
        try_remove(file_path)

    # Try to remove each directory
    for dir_path in dirs_to_remove:
        try_rmdir(dir_path)

    # --- Rewrite databricks.yml: remove artifacts and variables sections ---
    import yaml
    databricks_yml_path = "my_project/databricks.yml"
    if os.path.exists(databricks_yml_path):
        with open(databricks_yml_path, "r") as f:
            bundle_config = yaml.safe_load(f)
        # Remove artifacts section (uses 'uv build --wheel' which is not installed)
        bundle_config.pop("artifacts", None)
        # Remove top-level variables section (not needed for our custom job/pipeline)
        bundle_config.pop("variables", None)
        # Remove variables from each target too
        if "targets" in bundle_config:
            for target_name, target_config in bundle_config["targets"].items():
                if isinstance(target_config, dict):
                    target_config.pop("variables", None)
        with open(databricks_yml_path, "w") as f:
            yaml.dump(bundle_config, f, default_flow_style=False, sort_keys=False)
        print(f"Updated databricks.yml (removed artifacts and variables sections).")

    # --- Rewrite main.py as self-contained script (no wheel/package import needed) ---
    main_py_path = "my_project/src/my_project/main.py"
    if os.path.exists(main_py_path):
        main_py_content = '''from databricks.sdk.runtime import spark


def main():
    # Read taxi data from samples catalog
    df = spark.read.table("samples.nyctaxi.trips")
    df.show(5)
    print("Executing job with modified script")
    print(f"Total rows: {df.count()}")


if __name__ == "__main__":
    main()
'''
        with open(main_py_path, "w") as f:
            f.write(main_py_content)
        print(f"Updated main.py (self-contained, no package import needed).")

    # Get the current cluster ID
    current_cluster = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

    def get_policy_id(policy_name):
        """Fetches the policy ID for a given cluster policy name."""
        result = subprocess.run(
            ["databricks", "cluster-policies", "list", "--output", "JSON"],
            capture_output=True, text=True
        )

        if result.returncode != 0:
            print("Error executing command:", result.stderr)
            return None

        try:
            policies = json.loads(result.stdout)
        except json.JSONDecodeError:
            print("Failed to decode JSON from command output")
            return None

        for policy in policies:
            if policy['name'] == policy_name:
                return policy['policy_id']

        return None

    # Specify the policy name
    policy_name = "DBAcademy DLT UC"

    # Get the policy ID
    policy_id = get_policy_id(policy_name)

    if policy_id:
        print(f"Found policy ID: {policy_id}")
    else:
        print(f"Policy named '{policy_name}' not found")

    contents = f"""
# The main job for my_project.
resources:
  jobs:
    my_project_job:
      name: my_project_job

      schedule:
        # Run every day at 8:37 AM
        quartz_cron_expression: '44 37 8 * * ?'
        timezone_id: Europe/Amsterdam

      tasks:
        - task_key: notebook_task
          existing_cluster_id: {current_cluster}
          notebook_task:
            notebook_path: ../src/notebook.ipynb
        
        - task_key: refresh_pipeline
          depends_on:
            - task_key: notebook_task
          pipeline_task:
            pipeline_id: ${{resources.pipelines.my_project_pipeline.id}}
        
        - task_key: main_task
          depends_on:
            - task_key: refresh_pipeline
          existing_cluster_id: {current_cluster}
          spark_python_task:
            python_file: "../src/my_project/main.py"
            """

    with open("my_project/resources/my_project.job.yml", "w") as f:
        print(f"Updated the bundle yml file ({f.write(contents)} bytes written).")

    pipelinecontents = f"""
# The SDP pipeline for my_project.
resources:
  pipelines:
    my_project_pipeline:
      name: my_project_pipeline
      target: my_project_${{bundle.environment}}
      clusters:
        - label: default
          policy_id: {policy_id}
          num_workers: 1
        - label: maintenance
          policy_id: {policy_id}
      libraries:
        - notebook:
            path: ../src/dlt_pipeline.ipynb

      configuration:
        bundle.sourcePath: ${{workspace.file_path}}/src
            """

    with open("my_project/resources/my_project.pipeline.yml", "w") as f:
        print(f"Updated the bundle yml file ({f.write(pipelinecontents)} bytes written).")

    # Create notebook.ipynb placeholder in src
    import nbformat
    nb = nbformat.v4.new_notebook()
    nb.cells = [nbformat.v4.new_code_cell("print('Hello from notebook task')")]
    with open("my_project/src/notebook.ipynb", "w") as f:
        nbformat.write(nb, f)
        print("Created src/notebook.ipynb")

    # Create a valid SDP pipeline notebook in src
    nb_dlt = nbformat.v4.new_notebook()
    nb_dlt.cells = [nbformat.v4.new_code_cell("import pyspark.pipelines as dp\nfrom pyspark.sql.functions import *\n\n@dp.table(comment=\"A simple demo table created by the declarative automation bundle\")\ndef demo_table():\n    return spark.range(100).toDF(\"id\").withColumn(\"value\", col(\"id\") * 2)")]
    with open("my_project/src/dlt_pipeline.ipynb", "w") as f:
        nbformat.write(nb_dlt, f)
        print("Created src/dlt_pipeline.ipynb (with valid SDP code)")

    print("\nBundle updated successfully.")

# COMMAND ----------

LESSON = "generate_tokens"
DA = init_DA(LESSON, reset=False)

@DBAcademyHelper.add_method
def create_credentials_file(self, db_token, db_instance):
    contents = f"""
db_token = "{db_token}"
db_instance = "{db_instance}"
    """
    self.write_to_file(contents, "credentials.py")

None

# COMMAND ----------

import os
import yaml

# Get the current cluster ID from Spark configuration
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
print("Cluster ID:", cluster_id)

# Path to the variables.yml file
variables_file_path = "./ml_project/resources/variables.yml"

# Read the existing YAML file
with open(variables_file_path, "r") as file:
    variables_data = yaml.safe_load(file)

# Update the cluster_id_var.default with the actual cluster ID
variables_data["variables"]["cluster_id_var"]["default"] = cluster_id

# Write the updated data back to the YAML file
with open(variables_file_path, "w") as file:
    yaml.dump(variables_data, file, default_flow_style=False)

#print(f"Updated cluster_id_var in {variables_file_path} with Cluster ID: {cluster_id}")

# COMMAND ----------

# @DBAcademyHelper.add_method
# def update_ml_project_bundle(self):
#     import shutil
#     import yaml

#     # Define the source and destination file paths
#     source_file = "./my_project/databricks.yml"
#     destination_folder = "./ml_project/"
#     destination_file = f"{destination_folder}/databricks.yml"

#     # Copy the file from the source to the destination
#     try:
#         shutil.copy(source_file, destination_file)
#         #print(f"File copied successfully from {source_file} to {destination_file}")
#     except FileNotFoundError:
#         print(f"Source file {source_file} not found. Please check the path.")
#         return
#     except Exception as e:
#         print(f"An error occurred while copying the file: {e}")
#         return

#     # Load the YAML file
#     try:
#         with open(destination_file, "r") as file:
#             data = yaml.safe_load(file)
#     except FileNotFoundError:
#         print(f"File not found: {destination_file}")
#         return
#     except Exception as e:
#         print(f"An error occurred while reading the file: {e}")
#         return

#     # Update the 'bundle' name
#     if "bundle" in data and "name" in data["bundle"]:
#         old_name = data["bundle"]["name"]
#         data["bundle"]["name"] = "ml_project"
#         #print(f"Updated bundle name from '{old_name}' to 'ml_project'")
#     else:
#         print("The 'bundle' or 'name' field was not found in the YAML file.")
#         return

#     # Save the updated YAML back to the file
#     try:
#         with open(destination_file, "w") as file:
#             yaml.safe_dump(data, file)
#         #print(f"Updated YAML file saved to: {destination_file}")
#     except Exception as e:
#         print(f"An error occurred while saving the file: {e}")
# None

# COMMAND ----------

import os
import yaml

# Load the base path from the bundle validation output
notebook_path_full = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# Extract path up to the course folder
base_path = "/Workspace" + "/".join(notebook_path_full.split("/")[:-2])
# Define the workspace path of the notebook
#notebook_path = base_path + "/M05 - Build ML Assets as Code/my_project/scratch/exploration"
bundle_workspace_path = base_path + "/M05 - Build ML Assets as Code/ml_project"
src_relative_path = "src"  # This is the folder inside your project

# Full absolute path to src in workspace
src_workspace_path = f"{bundle_workspace_path}/{src_relative_path}"

# Load the YAML file
yaml_file = base_path + "/M05 - Build ML Assets as Code/ml_project/resources/ml_project_job.job.yml"

with open(yaml_file, "r") as f:
    data = yaml.safe_load(f)

# Update all notebook_task and spark_python_task paths
for task in data["resources"]["jobs"]["ml_project_job"]["tasks"]:
    task_key = task.get("task_key", "unknown")
    if "notebook_task" in task:
        notebook_name = os.path.basename(task["notebook_task"]["notebook_path"])
        task["notebook_task"]["notebook_path"] = f"{src_workspace_path}/{notebook_name}"
    elif "spark_python_task" in task:
        python_file = os.path.basename(task["spark_python_task"]["python_file"])
        task["spark_python_task"]["python_file"] = f"{src_workspace_path}/{python_file}"

# Save the updated YAML
with open(yaml_file, "w") as f:
    yaml.safe_dump(data, f, default_flow_style=False)

print("Updated databricks.yml with correct notebook and script paths.")