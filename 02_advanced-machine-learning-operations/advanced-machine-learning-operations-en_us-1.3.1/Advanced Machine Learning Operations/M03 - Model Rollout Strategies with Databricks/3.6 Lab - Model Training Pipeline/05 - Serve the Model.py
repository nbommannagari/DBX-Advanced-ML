# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %pip install -U databricks-feature-engineering

# COMMAND ----------

catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')

# COMMAND ----------

from mlflow import MlflowClient

# Initialize the MLflow Client
client = <FILL_IN>

# Define the model name and alias
model_name = <FILL_IN>  # Replace with your actual model name
alias_a = "a" 
alias_b = "b"

# Get the model version by alias
model_a_version= <FILL_IN>
model_b_version = <FILL_IN>

# Print the model version
print(f"Version for model a: {model_a_version}")
print(f"Version for model b: {model_b_version}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Look up model versions by alias (a and b)
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC from mlflow import MlflowClient
# MAGIC
# MAGIC # Initialize the MLflow Client
# MAGIC client = MlflowClient()
# MAGIC
# MAGIC # Define the model name and alias
# MAGIC model_name = f"{catalog}.{schema}.my_model_{schema}"  # Replace with your actual model name
# MAGIC alias_a = "a" 
# MAGIC alias_b = "b"
# MAGIC
# MAGIC # Get the model version by alias
# MAGIC model_a_version= client.get_model_version_by_alias(model_name, alias_a).version
# MAGIC model_b_version = client.get_model_version_by_alias(model_name, alias_b).version
# MAGIC
# MAGIC # Print the model version
# MAGIC print(f"Version for model a: {model_a_version}")
# MAGIC print(f"Version for model b: {model_b_version}")
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

from databricks.sdk import WorkspaceClient

try:
    # Initialize the workspace client
    workspace = WorkspaceClient()

    # Delete the serving endpoint
    workspace.serving_endpoints.delete(name=f"M02-endpoint_{schema}")
    print('Deleted Endpoint M02-endpoint')
except:
    print("Endpoint does not exist.")

# COMMAND ----------

# MAGIC %md
# MAGIC Setup the model serving to include both model versions with 40% of traffic going towards serving model A and 60% going towards model B.

# COMMAND ----------

from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")
endpoint_name = f"M02-endpoint_{schema}"
spark.sql(f'use catalog {catalog}')
spark.sql(f'use schema {schema}')
# Check if the endpoint already exists
try:
    # Attempt to get the endpoint
    existing_endpoint = client.get_endpoint(endpoint_name)
    print(f"Endpoint '{endpoint_name}' already exists.")
except Exception as e:
    # If not found, create the endpoint
    if "RESOURCE_DOES_NOT_EXIST" in str(e):
        print(f"Creating a new endpoint: {endpoint_name}")
        endpoint = client.create_endpoint(
            name=endpoint_name,
            config={
                "served_entities": [
                    {
                        "name": "my-model-a",
                        "entity_name": <FILL_IN>,
                        "entity_version": <FILL_IN>,
                        "workload_size": "Small",
                        "scale_to_zero_enabled": True
                    },
                    {
                        "name": "my-model-b",
                        "entity_name": <FILL_IN>,
                        "entity_version": <FILL_IN>,
                        "workload_size": "Small",
                        "scale_to_zero_enabled": True
                    }
                ],
                "traffic_config": {
                    "routes": [
                        {
                            "served_model_name": <FILL_IN>,
                            "traffic_percentage": 30
                        },
                        {
                            "served_model_name": <FILL_IN>,
                            "traffic_percentage": 60
                        }
                    ]
                }
            }
        )
    else:
        print(f"An error occurred: {e}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Create or reuse the model serving endpoint
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC from mlflow.deployments import get_deploy_client
# MAGIC
# MAGIC client = get_deploy_client("databricks")
# MAGIC endpoint_name = f"M02-endpoint_{schema}"
# MAGIC spark.sql(f'use catalog {catalog}')
# MAGIC spark.sql(f'use schema {schema}')
# MAGIC # Check if the endpoint already exists
# MAGIC try:
# MAGIC     # Attempt to get the endpoint
# MAGIC     existing_endpoint = client.get_endpoint(endpoint_name)
# MAGIC     print(f"Endpoint '{endpoint_name}' already exists.")
# MAGIC except Exception as e:
# MAGIC     # If not found, create the endpoint
# MAGIC     if "RESOURCE_DOES_NOT_EXIST" in str(e):
# MAGIC         print(f"Creating a new endpoint: {endpoint_name}")
# MAGIC         endpoint = client.create_endpoint(
# MAGIC             name=endpoint_name,
# MAGIC             config={
# MAGIC                 "served_entities": [
# MAGIC                     {
# MAGIC                         "name": "my-model-a",
# MAGIC                         "entity_name": model_name,
# MAGIC                         "entity_version": model_a_version,
# MAGIC                         "workload_size": "Small",
# MAGIC                         "scale_to_zero_enabled": True
# MAGIC                     },
# MAGIC                     {
# MAGIC                         "name": "my-model-b",
# MAGIC                         "entity_name": model_name,
# MAGIC                         "entity_version": model_b_version,
# MAGIC                         "workload_size": "Small",
# MAGIC                         "scale_to_zero_enabled": True
# MAGIC                     }
# MAGIC                 ],
# MAGIC                 "traffic_config": {
# MAGIC                     "routes": [
# MAGIC                         {
# MAGIC                             "served_model_name": "my-model-a",
# MAGIC                             "traffic_percentage": 40
# MAGIC                         },
# MAGIC                         {
# MAGIC                             "served_model_name": "my-model-b",
# MAGIC                             "traffic_percentage": 60
# MAGIC                         }
# MAGIC                     ]
# MAGIC                 }
# MAGIC             }
# MAGIC         )
# MAGIC     else:
# MAGIC         print(f"An error occurred: {e}")
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>