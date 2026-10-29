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

# MAGIC %md
# MAGIC ## Workflow Notebook - Silver to Feature Store
# MAGIC
# MAGIC 1. **Widgets at the Top**:
# MAGIC    - In this notebook, you will find several parameterized widgets:
# MAGIC      - **catalog**
# MAGIC      - **column**
# MAGIC      - **primary_key**
# MAGIC      - **schema**
# MAGIC      - **silver_table_name**
# MAGIC      - **target_column**
# MAGIC
# MAGIC 2. **Purpose of Parameterization**:
# MAGIC    - These widgets allow you to configure parameters dynamically when setting up workflows.
# MAGIC    - Instead of modifying hard-coded values in the notebook, you can edit the parameters directly in the Lakeflow Jobs UI.
# MAGIC
# MAGIC 3. **Notebook Functionality**:
# MAGIC    - This notebook focuses on **feature engineering**.
# MAGIC    - Specifically, it normalizes the **Age** column and generates a feature table.
# MAGIC    - The resulting feature table is stored in the **Feature Store** for use in downstream tasks like model training or evaluation.

# COMMAND ----------

# MAGIC %md
# MAGIC Read in silver-layer data.

# COMMAND ----------

# MAGIC %pip install -U databricks-feature-engineering

# COMMAND ----------

catalog = dbutils.widgets.get(<FILL_IN>)
schema = dbutils.widgets.get(<FILL_IN>)
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Read catalog/schema widgets and set the active schema
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC catalog = dbutils.widgets.get("catalog")
# MAGIC schema = dbutils.widgets.get("schema")
# MAGIC spark.sql(f"USE {catalog}.{schema}")
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

silver_table_name = dbutils.widgets.get(<FILL_IN>)
df = spark.read.format('delta').table(<FILL_IN>).toPandas()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Load selected feature columns from the silver table
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC silver_table_name = dbutils.widgets.get("silver_table_name")
# MAGIC df = spark.read.format('delta').table(silver_table_name).select('id', 'Diabetes_binary', 'HighBP', 'BMI', 'Smoker', 'Stroke', 'HeartDiseaseorAttack', 'Age'). toPandas()
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC Perform feature engineering - normalize your column of choice.

# COMMAND ----------

import pandas as pd
import numpy as np

from databricks.feature_engineering import FeatureEngineeringClient

## Instantiate the FeatureEngineeringClient
fe = FeatureEngineeringClient()

## Normalize the Age column and store it as Age_normalized

column = dbutils.widgets.get(<FILL_IN>)
target_column = dbutils.widgets.get(<FILL_IN>)

df[f'{column}_normalized'] = <FILL_IN>


df = df.drop(target_column, axis=1)
df = df.drop(column, axis=1)
normalized_df = spark.createDataFrame(df)

primary_key = dbutils.widgets.get(<FILL_IN>)

## Set the feature table name for storage in UC
feature_table_name = f'{<FILL_IN>}.{<FILL_IN>}.{<FILL_IN>}_features'

## print(f"The name of the feature table: {feature_table_name}\n\n")

spark.sql(f'drop table if exists {feature_table_name}')

## Create the feature table
fe.create_table(
    <FILL_IN>
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Normalize the column and write features to the Feature Store
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC
# MAGIC from databricks.feature_engineering import FeatureEngineeringClient
# MAGIC
# MAGIC ## Instantiate the FeatureEngineeringClient
# MAGIC fe = FeatureEngineeringClient()
# MAGIC
# MAGIC ## Normalize the Age column and store it as Age_normalized
# MAGIC
# MAGIC column = dbutils.widgets.get("column")
# MAGIC target_column = dbutils.widgets.get("target_column")
# MAGIC
# MAGIC df[f'{column}_normalized'] = (df[column] - df[column].mean()) / df[column].std()
# MAGIC
# MAGIC
# MAGIC df = df.drop(target_column, axis=1)
# MAGIC df = df.drop(column, axis=1)
# MAGIC normalized_df = spark.createDataFrame(df)
# MAGIC
# MAGIC primary_key = dbutils.widgets.get("primary_key")
# MAGIC
# MAGIC ## Set the feature table name for storage in UC
# MAGIC feature_table_name = f'{catalog}.{schema}.{silver_table_name}_features'
# MAGIC
# MAGIC ## print(f"The name of the feature table: {feature_table_name}\n\n")
# MAGIC
# MAGIC spark.sql(f'drop table if exists {feature_table_name}')
# MAGIC
# MAGIC ## Create the feature table
# MAGIC fe.create_table(
# MAGIC     name = feature_table_name,
# MAGIC     primary_keys = primary_key,
# MAGIC     df = normalized_df, 
# MAGIC     description="{schema} quality features", 
# MAGIC     tags = {"source": "silver", "format": "delta"}
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>