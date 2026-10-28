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
# MAGIC # Lab - Pandas APIs
# MAGIC
# MAGIC In this lab, you will explore how to integrate Pandas with Spark by comparing performance, converting data between different DataFrame types, applying Pandas UDFs, and training group-specific models with Pandas Function APIs. The dataset you'll be working with is the **Airbnb dataset**, which contains listings with features such as price, neighborhood, and property details.
# MAGIC
# MAGIC **Lab Outline:**
# MAGIC
# MAGIC _By the end of this lab, you will be able to:_
# MAGIC 1. **Task 1:** Comparing Performance of Pandas, Spark, and Pandas API DataFrames  
# MAGIC 2. **Task 2:** Converting Between DataFrames  
# MAGIC 3. **Task 3:** Applying Pandas UDF to Spark DataFrames
# MAGIC 4. **Task 4:** Training Group-Specific Models with Pandas Function API
# MAGIC 5. **Task 5:** Group-Specific Inference Using Pandas Function API

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC    - Click **More** in the drop-down.
# MAGIC
# MAGIC    - In the **Attach to an existing compute resource** window, use the first drop-down to select your unique cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 3. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run this notebook, you need to use one of the following Databricks runtime(s): **17.3.x-cpu-ml-scala2.13**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Install required libraries.

# COMMAND ----------

# MAGIC %pip install pandas pyspark koalas
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC Before starting the Lab, run the provided classroom setup script.

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC **Other Conventions:**
# MAGIC
# MAGIC Throughout this lab, we'll refer to the object `DA`. This object, provided by Databricks Academy, contains variables such as your username, catalog name, schema name, working directory, and dataset locations. Run the code block below to view these details:

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Catalog Name:      {DA.catalog_name}")
print(f"Schema Name:       {DA.schema_name}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"Dataset Location:  {DA.paths.datasets.covid}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Loading the Dataset
# MAGIC In this lab, you will load the **COVID-19 dataset**, which contains time-series data of daily confirmed cases, deaths, recoveries, and other metrics. You will first load the data into a Spark DataFrame and then convert it into Pandas and Pandas API DataFrames for further analysis.
# MAGIC
# MAGIC **Instructions:**
# MAGIC
# MAGIC 1. **Load the dataset into a Spark DataFrame**  
# MAGIC    - Use the `spark.read.csv()` method to load the dataset from a CSV file. Ensure that the `header=True` and `inferSchema=True` options are set to correctly read the column names and data types.
# MAGIC
# MAGIC 2. **Explore the dataset**  
# MAGIC    - Use the `display()` function to view the first few rows of the loaded dataset and understand the structure of the data.
# MAGIC

# COMMAND ----------

import pandas as pd
import pyspark.pandas as ps

## Load the original COVID-19 dataset into a Spark DataFrame from the specified path
data_path = f"{DA.paths.datasets.covid}/coronavirusdataset/Time.csv"
covid_df = spark.read.csv(data_path, header=True, inferSchema=True)

## Display the first few rows of the original dataset to inspect the structure and data
display(covid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Additional Resources:**
# MAGIC - [Databricks Documentation: Reading Data with Spark](https://docs.databricks.com/data/data-sources/read-csv.html) – Learn more about how to efficiently read data into a Spark DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Comparing Performance of Pandas, Spark, and Pandas API DataFrames
# MAGIC
# MAGIC In this task, you will compare the performance of **Pandas**, **Spark**, and **Pandas API on Spark** DataFrames by calculating the mean of numeric columns. You will measure the time taken by each method to highlight the differences in performance between single-node and distributed environments.
# MAGIC
# MAGIC **Instructions:**
# MAGIC
# MAGIC 1. **Calculate the mean using a Pandas DataFrame**  
# MAGIC    - Convert the Spark DataFrame to a Pandas DataFrame.
# MAGIC    - Measure the time taken to calculate the mean for each numeric column.
# MAGIC
# MAGIC 2. **Calculate the mean using a Spark DataFrame**  
# MAGIC    - Use Spark’s distributed DataFrame to calculate the mean of numeric columns.
# MAGIC    - Measure the time taken to calculate the mean for each numeric column.
# MAGIC
# MAGIC 3. **Calculate the mean using Pandas API on Spark DataFrame (Koalas)**  
# MAGIC    - Convert the Spark DataFrame to a Pandas API on Spark DataFrame.
# MAGIC    - Measure the time taken to calculate the mean for each numeric column.
# MAGIC
# MAGIC By comparing the time taken, you'll see the performance differences between the three approaches.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.1 Data Preparation
# MAGIC
# MAGIC In this part of the task, you will prepare the data by selecting numeric columns and converting the Spark DataFrame into **Pandas** and **Pandas API on Spark** DataFrames.
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. **Select the numeric columns** from the dataset.
# MAGIC 2. **Convert** the Spark DataFrame into a **Pandas DataFrame** and a **Pandas API on Spark DataFrame** for further analysis.

# COMMAND ----------

import time
from pyspark.sql.functions import avg
from pyspark.sql.types import DoubleType, IntegerType

## Select numeric columns for averaging
numeric_columns = <FILL_IN>
## Convert Spark DataFrame to Pandas DataFrame
covid_pandas_df = <FILL_IN>

## Convert Spark DataFrame to Pandas API on Spark DataFrame
covid_pandas_on_spark_df = <FILL_IN>

## Print the number of rows in the test dataframe
<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Task 1.1 Data Preparation
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC import time
# MAGIC from pyspark.sql.functions import avg
# MAGIC from pyspark.sql.types import DoubleType, IntegerType
# MAGIC
# MAGIC ## Select numeric columns for averaging
# MAGIC numeric_columns = [col for col in covid_df.columns if covid_df.schema[col].dataType in [DoubleType(), IntegerType()]]
# MAGIC
# MAGIC ## Convert Spark DataFrame to Pandas DataFrame
# MAGIC covid_pandas_df = covid_df.toPandas()
# MAGIC
# MAGIC ## Convert Spark DataFrame to Pandas API on Spark DataFrame
# MAGIC covid_pandas_on_spark_df = ps.DataFrame(covid_df) 
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Task 1.2 Comparing Performance Based on Time
# MAGIC
# MAGIC In this part, you will measure the time taken by **Pandas**, **Spark**, and **Pandas API on Spark** to calculate the mean of numeric columns and compare their performance. This comparison will help you understand the computational efficiency of single-node and distributed environments.
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. **Measure the time** for calculating the mean of numeric columns using **Pandas** on a single node.
# MAGIC 2. **Measure the time** for calculating the mean of numeric columns using **Spark** on a distributed architecture.
# MAGIC 3. **Measure the time** for calculating the mean of numeric columns using **Pandas API on Spark**, which uses Pandas-like syntax but benefits from Spark’s distributed capabilities.
# MAGIC
# MAGIC   - **Expected Outcome:** 
# MAGIC     - The mean values of the numeric columns will be displayed along with the time taken for each approach.
# MAGIC     - You will observe differences in computational time between single-node processing (Pandas) and distributed processing (Spark and Pandas API on Spark).
# MAGIC
# MAGIC   - **Key Insights:**
# MAGIC     - **Pandas:** Suitable for smaller datasets but can become slow or memory-intensive with larger datasets.
# MAGIC     - **Spark:** Handles large datasets efficiently using distributed processing across multiple nodes.
# MAGIC     - **Pandas API on Spark:** Provides the familiar Pandas syntax while utilizing Spark's distributed processing power, making it a good balance between usability and scalability.

# COMMAND ----------

import time
from pyspark.sql.functions import avg
from pyspark.sql.types import DoubleType, IntegerType

start_time = time.time()
## Measure time for Pandas DataFrame
pandas_mean = <FILL_IN>
pandas_time = <FILL_IN>

## Measure time for Spark DataFrame
spark_mean = <FILL_IN>
spark_time = <FILL_IN>

## Measure time for Pandas API on Spark DataFrame
pandas_on_spark_mean = <FILL_IN>
pandas_on_spark_time = <FILL_IN>

## Display all the times together
print(f"Pandas DataFrame mean calculated in: {<FILL_IN>} seconds")
print(f"Spark DataFrame mean calculated in: {<FILL_IN>} seconds")
print(f"Pandas API on Spark DataFrame mean calculated in: {<FILL_IN>} seconds")

## Now, display all the means together
print("\nPandas DataFrame Mean:")
<FILL_IN>

print("Spark DataFrame Mean:")
<FILL_IN>

print("Pandas API on Spark DataFrame Mean:")
<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Task 1.2 Comparing Performance Based on Time
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC import time
# MAGIC from pyspark.sql.functions import avg
# MAGIC from pyspark.sql.types import DoubleType, IntegerType
# MAGIC
# MAGIC start_time = time.time()
# MAGIC ## Measure time for Pandas DataFrame
# MAGIC pandas_mean = covid_pandas_df[numeric_columns].mean()
# MAGIC pandas_time = time.time() - start_time
# MAGIC
# MAGIC ## Measure time for Spark DataFrame
# MAGIC spark_mean = covid_df.select([avg(col).alias(f"avg_{col}") for col in numeric_columns])
# MAGIC spark_time = time.time() - start_time
# MAGIC
# MAGIC ## Measure time for Pandas API on Spark DataFrame
# MAGIC pandas_on_spark_mean = covid_pandas_on_spark_df[numeric_columns].mean()
# MAGIC pandas_on_spark_time = time.time() - start_time
# MAGIC
# MAGIC ## Display all the times together
# MAGIC print(f"Pandas DataFrame mean calculated in: {pandas_time:.4f} seconds")
# MAGIC print(f"Spark DataFrame mean calculated in: {spark_time:.4f} seconds")
# MAGIC print(f"Pandas API on Spark DataFrame mean calculated in: {pandas_on_spark_time:.4f} seconds")
# MAGIC
# MAGIC ## Now, display all the means together
# MAGIC print("\nPandas DataFrame Mean:")
# MAGIC display(pandas_mean)
# MAGIC
# MAGIC print("Spark DataFrame Mean:")
# MAGIC display(spark_mean)
# MAGIC
# MAGIC print("Pandas API on Spark DataFrame Mean:")
# MAGIC display(pandas_on_spark_mean)
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploration Task:
# MAGIC
# MAGIC Now that you’ve seen how to calculate the mean across different frameworks, try exploring other statistical operations such as `median()`, `variance()`, or `correlation()`. How do these calculations scale when using Pandas, Spark, and Pandas API on Spark?
# MAGIC

# COMMAND ----------

## Example: Calculate median using Pandas API on Spark
pandas_on_spark_median = covid_pandas_on_spark_df.median()
## Display the median values calculated using Pandas API on Spark
display(<FILL_IN>)

## Try calculating variance
pandas_on_spark_var = <FILL_IN>.var()
## Display the variance values calculated using Pandas API on Spark
display(<FILL_IN>)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Exploration Task:
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC ## Example: Calculate median using Pandas API on Spark
# MAGIC pandas_on_spark_median = covid_pandas_on_spark_df.median()
# MAGIC ## Display the median values calculated using Pandas API on Spark
# MAGIC display(pandas_on_spark_median)
# MAGIC
# MAGIC ## Try calculating variance
# MAGIC pandas_on_spark_var = covid_pandas_on_spark_df.var()
# MAGIC ## Display the variance values calculated using Pandas API on Spark
# MAGIC display(pandas_on_spark_var)
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Learning Takeaways:**
# MAGIC
# MAGIC - **Pandas:** Great for small datasets, but struggles with larger ones due to single-node operation.
# MAGIC - **Spark:** Handles large datasets efficiently through distributed processing across multiple nodes.
# MAGIC - **Pandas API on Spark (Koalas):** Provides the ease of using Pandas-like syntax but operates on Spark’s distributed environment, making it both scalable and user-friendly.
# MAGIC
# MAGIC **Additional Resources:**
# MAGIC
# MAGIC - [Pandas API on Spark (Koalas) Documentation](https://koalas.readthedocs.io/en/latest/)
# MAGIC - [Optimizing Spark Performance](https://spark.apache.org/docs/latest/sql-performance-tuning.html) for large datasets in distributed environments.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Converting Between DataFrames
# MAGIC
# MAGIC In this task, you will learn how to convert between different types of DataFrames: **Spark DataFrame**, **Pandas API on Spark (Koalas) DataFrame**, and **Pandas DataFrame**. Being able to seamlessly switch between these types allows you to leverage the strengths of each framework for specific tasks.
# MAGIC
# MAGIC **Why is this important?**
# MAGIC - **Spark DataFrame**: Ideal for large datasets and distributed computing, particularly when using Spark’s built-in functions and scalability.
# MAGIC - **Pandas API on Spark (Koalas)**: Provides the simplicity and familiarity of Pandas but operates in Spark’s distributed environment, offering the best of both worlds.
# MAGIC - **Pandas DataFrame**: Perfect for small datasets or when you need to use Pandas-specific functions that are not available in Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.1 Convert Spark DataFrame to Pandas API on Spark DataFrame
# MAGIC
# MAGIC You will start by converting a **Spark DataFrame** into a **Pandas API on Spark DataFrame** (Koalas). This allows you to perform Pandas-like operations but with the scalability of Spark’s distributed environment.
# MAGIC
# MAGIC **Steps:**
# MAGIC - **Step 1:** Use the `.to_pandas_on_spark()` method to convert the Spark DataFrame into a Pandas API on Spark DataFrame.
# MAGIC - **Expected Outcome:** You can now apply Pandas-like functions on this DataFrame while benefiting from Spark's distributed processing.
# MAGIC
# MAGIC
# MAGIC **Exploration Tip:** Try performing some common Pandas operations like `describe()`, `groupby()`, or `sum()` on this new Pandas API DataFrame.
# MAGIC

# COMMAND ----------

## Convert Spark DataFrame to Pandas API on Spark DataFrame (Koalas)
pandas_on_spark_df = covid_df.<FILL_IN>

## Display the Pandas API on Spark DataFrame
<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Task 2.1 Convert Spark DataFrame to Pandas API on Spark DataFrame
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC ## Convert Spark DataFrame to Pandas API on Spark DataFrame (Koalas)
# MAGIC pandas_on_spark_df = covid_df.toPandas()
# MAGIC
# MAGIC ## Display the Pandas API on Spark DataFrame
# MAGIC display(pandas_on_spark_df)
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2 Convert Pandas API on Spark DataFrame Back to Spark DataFrame
# MAGIC
# MAGIC You may want to convert a **Pandas API on Spark DataFrame** back to a **Spark DataFrame** when you need to use Spark-specific functions that are unavailable in Pandas.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC - **Step 1:** Use the `.to_spark()` method to convert the Pandas API on Spark DataFrame back into a Spark DataFrame.
# MAGIC
# MAGIC **Expected Outcome:** The DataFrame is now a Spark DataFrame again, and you can use Spark’s distributed computing and functions like `select()`, `groupBy()`, or `filter()` to manipulate large datasets efficiently.
# MAGIC
# MAGIC **Exploration Tip:** Try using Spark’s built-in functions, such as `groupBy()` and `agg()`, after converting back to Spark DataFrame.
# MAGIC

# COMMAND ----------

## Convert Pandas API on Spark DataFrame back to Spark DataFrame
spark_df = pandas_on_spark_df.<FILL_IN>

## Display the Spark DataFrame
<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Task 2.2 Convert Pandas API on Spark DataFrame Back to Spark DataFrame
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC ## Convert Pandas API on Spark DataFrame back to Spark DataFrame
# MAGIC spark_df = ps.DataFrame(pandas_on_spark_df).to_spark()
# MAGIC
# MAGIC ## Display the Spark DataFrame
# MAGIC display(spark_df)
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.3 Convert Pandas API on Spark DataFrame to Pandas DataFrame
# MAGIC
# MAGIC In some cases, you might want to work with a smaller dataset locally, or use Pandas-specific functions that aren’t available in Pandas API on Spark. In such cases, you can convert the **Pandas API on Spark DataFrame** to a **Pandas DataFrame**.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC - **Step 1:** Use the `.to_pandas()` method to convert the Pandas API on Spark DataFrame into a Pandas DataFrame.
# MAGIC
# MAGIC **Expected Outcome:** The DataFrame is now a regular Pandas DataFrame, allowing you to use all Pandas functionalities, but it will be constrained to single-node operation.
# MAGIC
# MAGIC **Exploration Tip:** After converting to a Pandas DataFrame, try performing some advanced Pandas functions such as `pivot_table()`, `corr()`, or `rolling()` to explore how Pandas can handle data locally.

# COMMAND ----------

## Convert Pandas API on Spark DataFrame to Pandas DataFrame
pandas_df = pandas_on_spark_df.<FILL_IN>
## Display the Pandas DataFrame
<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Task 2.3 Convert Pandas API on Spark DataFrame to Pandas DataFrame
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC ## Convert Pandas API on Spark DataFrame to Pandas DataFrame
# MAGIC pandas_df = ps.DataFrame(pandas_on_spark_df).to_pandas()
# MAGIC ## Display the Pandas DataFrame
# MAGIC display(pandas_df)
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Takeaways:**
# MAGIC - **Conversion Flexibility:** Switching between DataFrame types (Spark, Pandas API on Spark, and Pandas) allows you to take advantage of each framework’s strengths. 
# MAGIC - **Scaling Operations:** Pandas API on Spark provides a familiar Pandas-like interface with the scalability of Spark, making it easier to switch between smaller and larger datasets seamlessly.
# MAGIC - **Local vs Distributed:** Use Pandas DataFrame for smaller, local data processing, while Pandas API on Spark and Spark DataFrames are better suited for distributed, large-scale data processing.
# MAGIC
# MAGIC **Further Exploration:**
# MAGIC - [Databricks Documentation on Pandas API on Spark](https://docs.databricks.com/aws/en/pandas/pandas-on-spark)
# MAGIC - [Pandas Documentation](https://pandas.pydata.org/docs/)
# MAGIC - [Spark SQL, DataFrames, and Datasets Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Applying Pandas UDF to a Spark DataFrame  
# MAGIC In this task, you will use a pre-trained **RandomForest** model from **Scikit-learn** and apply it to a Spark DataFrame using a **Pandas UDF**. Pandas UDFs allow you to apply custom Python functions to Spark DataFrames in a distributed manner, combining the flexibility of Python with the scalability of Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.1 Training a RandomForest Model  
# MAGIC You’ll start by training a simple **RandomForest** model using **Scikit-learn** on a **local Pandas DataFrame**. The goal is to predict the number of confirmed COVID-19 cases based on other features in the dataset.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC - **Step 1:** Convert the Spark DataFrame into a Pandas DataFrame using `.toPandas()`.
# MAGIC - **Step 2:** Train a **RandomForestRegressor** model using the **confirmed** cases as the target variable (`y`) and the other columns as features (`X`).

# COMMAND ----------

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

## Convert Spark DataFrame to Pandas DataFrame for local model training
covid_pandas_df = <FILL_IN>

## Define features (X) and target variable (y)
X = <FILL_IN>  # Features
y = <FILL_IN>  # Target variable: confirmed cases

## Train the RandomForest model
model = RandomForestRegressor()
## Fit the model to the data
model.<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Task 3.1 Training a RandomForest Model
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC from sklearn.ensemble import RandomForestRegressor
# MAGIC from sklearn.model_selection import train_test_split
# MAGIC
# MAGIC ## Convert Spark DataFrame to Pandas DataFrame for local model training
# MAGIC covid_pandas_df = covid_df.toPandas()
# MAGIC
# MAGIC ## Define features (X) and target variable (y)
# MAGIC X = covid_pandas_df.drop(columns=["confirmed", "deceased", "released", "date"])  # Features
# MAGIC y = covid_pandas_df["confirmed"]  # Target variable: confirmed cases
# MAGIC
# MAGIC ## Train the RandomForest model
# MAGIC model = RandomForestRegressor()
# MAGIC ## Fit the model to the data
# MAGIC model.fit(X, y)
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2 Define and Apply a Pandas UDF  
# MAGIC Next, you define a **Pandas UDF** to apply the trained **RandomForest** model to our **Spark DataFrame**. The UDF will take multiple columns as input and output predictions for each row.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC - **Step 1:** Define a Pandas UDF using the `@pandas_udf` decorator, specifying that the return type is a **double** (which represents the predicted confirmed cases).
# MAGIC - **Step 2:** Use **`pd.concat`** to combine all input columns into a single DataFrame that the model can process for prediction.
# MAGIC
# MAGIC **Exploration Tip:** Try adjusting the output type of the UDF if you want to predict a different type of variable.

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd

## Define a Pandas UDF to apply the trained RandomForest model
@pandas_udf("double")  # Output type of the UDF is a double (for predicted confirmed cases)
def predict_udf(*cols: pd.Series) -> pd.Series:
    ## Combine all input columns into a single DataFrame for model prediction
    features = <FILL_IN>
    return pd.Series(model.<FILL_IN>)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Task 3.2 Define and Apply a Pandas UDF
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import pandas_udf
# MAGIC import pandas as pd
# MAGIC
# MAGIC ## Define a Pandas UDF to apply the trained RandomForest model
# MAGIC @pandas_udf("double")  # Output type of the UDF is a double (for predicted confirmed cases)
# MAGIC def predict_udf(*cols: pd.Series) -> pd.Series:
# MAGIC     ## Combine all input columns into a single DataFrame for model prediction
# MAGIC     features = pd.concat(cols, axis=1)
# MAGIC     return pd.Series(model.predict(features))
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.3 Applying the Pandas UDF to the Spark DataFrame  
# MAGIC Now that you’ve defined the UDF, it’s time to apply it to the Spark DataFrame to generate predictions for each row.
# MAGIC
# MAGIC **Steps;**
# MAGIC
# MAGIC - **Step 1:** Use the `.withColumn()` method to apply the **Pandas UDF** to the DataFrame and create a new column, **`prediction`**, that contains the predicted confirmed cases.
# MAGIC - **Step 2:** Exclude the target column (`confirmed`) and other irrelevant columns (`deceased`, `released`, `date`) from the input features.
# MAGIC - **Exploration Tip:** Check how well the predictions align with the actual values by comparing the `prediction` column with the `confirmed` column.

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd

## Define the features used for prediction, excluding the target columns "confirmed", "deceased", "released", and "date"
feature_names = [col for col in covid_df.columns if col not in ["confirmed", "deceased", "released", "date"]]

@pandas_udf("double")
def predict_udf(*cols: pd.Series) -> pd.Series:
    ## Combine input columns into a DataFrame for model prediction
    features = <FILL_IN>
    features.columns = feature_names  # Set the correct feature names
    
    ## Return predictions from the trained model
    return pd.Series(model.predict(features))

## Apply the Pandas UDF to the Spark DataFrame for predictions, excluding the target columns
prediction_df = covid_df.select(<FILL_IN>).withColumn(
    "prediction", 
    predict_udf(*[<FILL_IN>])
)

## Display the DataFrame with predictions
<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Task 3.3 Applying the Pandas UDF to the Spark DataFrame
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import pandas_udf
# MAGIC import pandas as pd
# MAGIC
# MAGIC ## Define the features used for prediction, excluding the target columns "confirmed", "deceased", "released", and "date"
# MAGIC feature_names = [col for col in covid_df.columns if col not in ["confirmed", "deceased", "released", "date"]]
# MAGIC
# MAGIC @pandas_udf("double")
# MAGIC def predict_udf(*cols: pd.Series) -> pd.Series:
# MAGIC     ## Combine input columns into a DataFrame for model prediction
# MAGIC     features = pd.concat(cols, axis=1)
# MAGIC     features.columns = feature_names  # Set the correct feature names
# MAGIC
# MAGIC     ## Return predictions from the trained model
# MAGIC     return pd.Series(model.predict(features))
# MAGIC
# MAGIC ## Apply the Pandas UDF to the Spark DataFrame for predictions, excluding the target columns
# MAGIC prediction_df = covid_df.select([col for col in feature_names]).withColumn(
# MAGIC     "prediction", 
# MAGIC     predict_udf(*[covid_df[col] for col in feature_names])
# MAGIC )
# MAGIC
# MAGIC ## Display the DataFrame with predictions
# MAGIC display(prediction_df)
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploration Task:
# MAGIC
# MAGIC **Try Training Other Models:**
# MAGIC
# MAGIC - After training a **RandomForest** model, experiment with other models such as **LinearRegression** or **GradientBoostingRegressor** from Scikit-learn. 
# MAGIC - Train these models on the same dataset, then define and apply the same Pandas UDF pattern for predictions.
# MAGIC
# MAGIC **Evaluate Model Performance:**
# MAGIC
# MAGIC - Add additional code to calculate metrics such as **Mean Squared Error (MSE)** or **R² score** to evaluate the performance of your models.
# MAGIC - Compare the performance of different models (e.g., RandomForest vs. LinearRegression vs. GradientBoosting) to determine which model performs best on the dataset.

# COMMAND ----------

## Example:
from sklearn.linear_model import LinearRegression

## Train a LinearRegression model
lr_model = LinearRegression()
## Fit the model to the data
lr_model.fit(X, y)

## Define a Pandas UDF for Linear Regression predictions
@pandas_udf("double")
def lr_predict_udf(*cols: pd.Series) -> pd.Series:
    features = <FILL_IN>
    return pd.Series(lr_model.<FILL_IN>)

from sklearn.metrics import mean_squared_error, r2_score

## Calculate MSE and R² for the RandomForest model predictions
rf_predictions = model.predict(X)
mse_rf = mean_squared_error(y, rf_predictions)
r2_rf = r2_score(y, rf_predictions)

print(f"RandomForest MSE: {<FILL_IN>}, R²: {<FILL_IN>}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Exploration Task:
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC ## Example:
# MAGIC from sklearn.linear_model import LinearRegression
# MAGIC
# MAGIC ## Train a LinearRegression model
# MAGIC lr_model = LinearRegression()
# MAGIC # Fit the model to the data
# MAGIC lr_model.fit(X, y)
# MAGIC
# MAGIC ## Define a Pandas UDF for Linear Regression predictions
# MAGIC @pandas_udf("double")
# MAGIC def lr_predict_udf(*cols: pd.Series) -> pd.Series:
# MAGIC     features = pd.concat(cols, axis=1)
# MAGIC     return pd.Series(lr_model.predict(features))
# MAGIC
# MAGIC from sklearn.metrics import mean_squared_error, r2_score
# MAGIC
# MAGIC ## Calculate MSE and R² for the RandomForest model predictions
# MAGIC rf_predictions = model.predict(X)
# MAGIC mse_rf = mean_squared_error(y, rf_predictions)
# MAGIC r2_rf = r2_score(y, rf_predictions)
# MAGIC
# MAGIC print(f"RandomForest MSE: {mse_rf}, R²: {r2_rf}")
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Training Group-Specific Models with Pandas Function API
# MAGIC
# MAGIC In this task, you will learn how to train group-specific models for different dates using **Pandas Function APIs**. We'll also log each trained model with **MLflow** for tracking and comparison. This technique allows for creating separate machine learning models for each group (in this case, dates) and analyzing performance differences.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.1 Define the Group-Specific Model Training Function  
# MAGIC Your will start by defining a function that:  
# MAGIC
# MAGIC - **Trains a model** for each group of data.
# MAGIC - **Logs the model** with MLflow for tracking.
# MAGIC - **Returns model metadata**, such as the path to the saved model, Mean Squared Error (MSE), and the number of records used for training.
# MAGIC
# MAGIC This function will be applied to each group defined by the `date` column.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC - **Step 1:** The `train_group_model` function extracts the group label (in this case, the **date**) and defines the features (`X_group`) and target variable (`y_group`).
# MAGIC - **Step 2:** A **RandomForestRegressor** model is trained on the subset of data for the specific date.
# MAGIC - **Step 3:** The model is logged using **MLflow** to save it for future reference and evaluation.
# MAGIC - **Step 4:** The function calculates the **Mean Squared Error (MSE)** for the group and returns relevant details, including the model's path in MLflow.

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
from sklearn.metrics import mean_squared_error
import mlflow
import mlflow.sklearn

## Define a pandas function to train a group-specific model and log each model with MLflow
def train_group_model(df_pandas: pd.DataFrame) -> pd.DataFrame:
    # Extract the group label (in this case, the date)
    date = df_pandas['date'].iloc[0]
    
    ## Define features (X) and target variable (y)
    X_group = df_pandas.drop(columns=["confirmed", "deceased", "released", "date"])
    y_group = df_pandas["confirmed"]
    
    ## Train a RandomForest model for the group
    model = RandomForestRegressor(<FILL_IN>)
    # Fit the model to the data
    model.<FILL_IN>
    
    ## Calculate Mean Squared Error (MSE) for the group
    predictions = model.predict(X_group)
    mse = <FILL_IN>

    ## Return a DataFrame containing group information and model performance
    return pd.DataFrame({
        <FILL_IN>
    })

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Task 4.1 Define the Group-Specific Model Training Function
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
# MAGIC from sklearn.metrics import mean_squared_error
# MAGIC import mlflow
# MAGIC import mlflow.sklearn
# MAGIC
# MAGIC ## Define a pandas function to train a group-specific model and log each model with MLflow
# MAGIC def train_group_model(df_pandas: pd.DataFrame) -> pd.DataFrame:
# MAGIC     # Extract the group label (in this case, the date)
# MAGIC     date = df_pandas['date'].iloc[0]
# MAGIC
# MAGIC     ## Define features (X) and target variable (y)
# MAGIC     X_group = df_pandas.drop(columns=["confirmed", "deceased", "released", "date"])
# MAGIC     y_group = df_pandas["confirmed"]
# MAGIC
# MAGIC     ## Train a RandomForest model for the group
# MAGIC     model = RandomForestRegressor(n_estimators=1, random_state=42)
# MAGIC     model.fit(X_group, y_group)
# MAGIC
# MAGIC     ## Calculate Mean Squared Error (MSE) for the group
# MAGIC     predictions = model.predict(X_group)
# MAGIC     mse = mean_squared_error(y_group, predictions)
# MAGIC
# MAGIC     ## Return a DataFrame containing group information and model performance
# MAGIC     return pd.DataFrame({
# MAGIC         "date": [str(date)],   # Group label (date)
# MAGIC         "mse": [float(mse)],  # Model performance metric
# MAGIC         "n_used": [int(len(df_pandas))]  # Number of records used in training
# MAGIC     })
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.2 Apply the Group-Specific Function Using Pandas API  
# MAGIC You will now apply the `train_group_model` function to each group using the **Pandas Function API**. This will allow us to train a separate model for each date in the dataset.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC - **Step 1:** You define the schema of the output DataFrame, which includes the group label (`date`), the **MLflow model path**, the **MSE**, and the number of records used in training.
# MAGIC - **Step 2:** You apply the `train_group_model` function using **`applyInPandas`** to each group defined by the `date` column. 
# MAGIC - **Step 3:** The resulting DataFrame, `result_df`, will display the **model path**, the calculated **MSE**, and how many records were used in training each model.

# COMMAND ----------

## Convert the date column to a numerical value using only the month starting with January
from pyspark.sql.functions import month
covid_df = covid_df.withColumn('date', <FILL_IN>)

## Define the schema for the output DataFrame
schema = StructType([
    <FILL_IN>
])

## Apply the group-specific model training function using 'applyInPandas'
result_df = covid_df.groupby("date").applyInPandas(train_group_model, schema=schema)

## Display the result DataFrame showing the model path, MSE, and number of records for each group
<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Task 4.2 Apply the Group-Specific Function Using Pandas API
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import col, month
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
# MAGIC # date is already INT (1–12), so just reuse it safely
# MAGIC covid_df_fixed = covid_df.withColumn("month", month(col("date")))
# MAGIC
# MAGIC ## Define the schema for the output DataFrame
# MAGIC schema = StructType([
# MAGIC     StructField("date", StringType(), True),
# MAGIC     StructField("mse", DoubleType(), True),
# MAGIC     StructField("n_used", IntegerType(), True)
# MAGIC ])
# MAGIC
# MAGIC ## Apply the group-specific model training function using 'applyInPandas'
# MAGIC result_df = covid_df.groupby("date").applyInPandas(train_group_model, schema=schema)
# MAGIC
# MAGIC ## Display the result DataFrame showing the model path, MSE, and number of records for each group
# MAGIC display(result_df)
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploration Task:
# MAGIC
# MAGIC **1. Experiment with Other Models:**
# MAGIC    - Try replacing the **RandomForest** model with other **Scikit-learn** models such as **LinearRegression** or **GradientBoostingRegressor**.
# MAGIC    - Observe how different models perform when applied to each group (date). Compare their performance metrics (such as **MSE**) and check whether one model performs better for certain groups.
# MAGIC
# MAGIC **2. Evaluate Model Performance:**
# MAGIC    - Use additional evaluation metrics, such as the **R² score** or **Mean Absolute Error (MAE)**. Log these metrics in MLflow along with the models.
# MAGIC    - Compare performance metrics across different dates or groups to track which model performs best for each group.
# MAGIC
# MAGIC **3. Test with Different Groupings:**
# MAGIC    - Instead of grouping by `date`, try changing the grouping criterion. For example, group by features such as **zipcode** or **property_type** (if available in your dataset).
# MAGIC    - This will help you understand how different group-specific models perform based on different segmentation criteria.
# MAGIC
# MAGIC **Further Exploration:**
# MAGIC
# MAGIC - **MLflow Documentation:** Learn more about tracking models and metrics using MLflow [MLflow Tracking Documentation](https://mlflow.org/docs/latest/tracking.html).
# MAGIC - **Pandas Function API Documentation:** Explore how to use Pandas Function API in PySpark for scalable group-specific operations [Pandas Function API in PySpark](https://docs.databricks.com/aws/en/pandas/pandas-function-apis).
# MAGIC
# MAGIC This exploration step will help users understand the benefits of group-specific modeling and how different models and evaluation metrics behave across distinct segments of the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Group-Specific Inference Using Pandas Function API  
# MAGIC In this task, you will use the models trained in the previous task to perform group-specific predictions (inference) for each date. We’ll load the models from **MLflow**, apply them to the test data, and calculate the overall prediction accuracy.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.1 Define the Inference Function  
# MAGIC You will create a function to:
# MAGIC
# MAGIC - **Load the saved model** for each date from **MLflow**.
# MAGIC - Use the corresponding **feature columns** to make predictions for each group.
# MAGIC - Return both the **predicted confirmed cases** and the **actual confirmed cases** for comparison.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC - **Step 1:** The function `apply_model` retrieves the **model path** for each date from the input DataFrame (`df_pandas`).
# MAGIC - **Step 2:** The model is loaded from **MLflow** using the `mlflow.sklearn.load_model` method.
# MAGIC - **Step 3:** The function selects the relevant feature columns (`test`, `negative`, and `released`) for inference.
# MAGIC - **Step 4:** The model makes predictions based on the features, and the results (both predicted and actual confirmed cases) are returned in a new DataFrame.

# COMMAND ----------

def apply_model(df_pandas: pd.DataFrame) -> pd.DataFrame:
    
    ## Define the feature columns that were used during training
    feature_columns = ["test", "negative", "released"]
    X = df_pandas[feature_columns]
    
    ## Make predictions using the loaded model
    predictions = <FILL_IN>
    
    ## Return a DataFrame containing both the predicted and actual confirmed cases
    return pd.DataFrame({
        <FILL_IN>
    })

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Task 5.1 Define the Inference Function
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC def apply_model(df_pandas: pd.DataFrame) -> pd.DataFrame:
# MAGIC
# MAGIC     ## Define the feature columns that were used during training
# MAGIC     feature_columns = ["time", "test", "negative"]
# MAGIC     X = df_pandas[feature_columns]
# MAGIC
# MAGIC     ## Make predictions using the loaded model
# MAGIC     predictions = model.predict(X)
# MAGIC
# MAGIC     ## Return a DataFrame containing both the predicted and actual confirmed cases
# MAGIC     return pd.DataFrame({
# MAGIC         "prediction": predictions,
# MAGIC         "actual_confirmed": df_pandas["confirmed"]
# MAGIC     })
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.2 Apply the Inference Function  
# MAGIC You will apply the `apply_model` function using the **Pandas Function API** to perform inference for each group (date). This step will calculate predictions for the test data.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC - **Step 1:** You define the schema for the output of the `apply_model` function, specifying that it will contain **predicted** and **actual confirmed cases**.
# MAGIC - **Step 2:** You join the result DataFrame (`result_df`, which contains the model paths) with the original **COVID-19** dataset to include the necessary features for inference.
# MAGIC - **Step 3:** The `applyInPandas` function is used to apply the inference function across each group (`date`).
# MAGIC - **Step 4:** The final DataFrame (`inference_df`) will display the **predicted values** alongside the **actual confirmed cases** for comparison.

# COMMAND ----------

from pyspark.sql.functions import abs

## Define the schema to include the predicted and actual confirmed cases
inference_schema = <FILL_IN>

## Join the result DataFrame with the original dataset to ensure model_path and feature columns are included
inference_df = result_df.join(covid_df, "date")

## Apply the model using Pandas Function API, grouped by 'date'
inference_df = inference_df.groupby("date").applyInPandas(apply_model, schema=inference_schema)

## Display the result DataFrame with predictions and actual confirmed cases
<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Task 5.2 Apply the Inference Function
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import abs
# MAGIC
# MAGIC ## Define the schema to include the predicted and actual confirmed cases
# MAGIC inference_schema = "prediction double, actual_confirmed double"
# MAGIC
# MAGIC ## Join the result DataFrame with the original dataset to ensure model_path and feature columns are included
# MAGIC inference_df = result_df.join(covid_df, "date")
# MAGIC
# MAGIC ## Apply the model using Pandas Function API, grouped by 'date'
# MAGIC inference_df = inference_df.groupby("date").applyInPandas(apply_model, schema=inference_schema)
# MAGIC
# MAGIC ## Display the result DataFrame with predictions and actual confirmed cases
# MAGIC display(inference_df)
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.3 Calculating Overall Accuracy  (Optional)
# MAGIC In this step, you will calculate the overall accuracy of the predictions. We'll define accuracy as the percentage of predictions within **10%** of the actual confirmed cases.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC - **Step 1:** You calculate the accuracy by checking if the **absolute difference** between the predicted and actual values is less than **10%** of the actual value.
# MAGIC - **Step 2:** You compute the overall accuracy as the percentage of rows where the prediction falls within this range.
# MAGIC - **Step 3:** The result is printed, showing the overall prediction accuracy as a percentage.

# COMMAND ----------

## Calculate accuracy: percentage of predictions within 10% of the actual confirmed cases
inference_df = inference_df.withColumn(
    <FILL_IN>
)

## Calculate the overall accuracy
overall_accuracy = inference_df.filter("accuracy = true").count() / inference_df.count() * 100

## Display the overall prediction accuracy
<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Task 5.3 Calculating Overall Accuracy  (Optional)
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC ## Calculate accuracy: percentage of predictions within 10% of the actual confirmed cases
# MAGIC inference_df = inference_df.withColumn(
# MAGIC     "accuracy", 
# MAGIC     (abs(inference_df["prediction"] - inference_df["actual_confirmed"]) / inference_df["actual_confirmed"]) < 0.1
# MAGIC )
# MAGIC
# MAGIC ## Calculate the overall accuracy
# MAGIC overall_accuracy = inference_df.filter("accuracy = true").count() / inference_df.count() * 100
# MAGIC
# MAGIC ## Display the overall prediction accuracy
# MAGIC print(f"Overall prediction accuracy: {overall_accuracy:.2f}%")
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploration Task:
# MAGIC
# MAGIC **1. Experiment with Model Inference:**
# MAGIC    - Try using the models trained with different Scikit-learn algorithms (such as **LinearRegression** or **GradientBoostingRegressor**) and apply the inference function again. Compare the accuracy of these models with the original **RandomForest** model.
# MAGIC
# MAGIC **2. Modify Accuracy Threshold:**
# MAGIC    - Adjust the accuracy threshold from **10%** to another value (e.g., **5%** or **20%**) and observe how the overall prediction accuracy changes. This will give you a sense of how sensitive the models are to accuracy thresholds.
# MAGIC
# MAGIC **3. Compare Predictions Across Dates:**
# MAGIC    - Group the results by other columns (such as **region** or **age group**, if available) and observe how the models perform in predicting confirmed cases across different regions or demographic groups.
# MAGIC
# MAGIC **Further Exploration:**
# MAGIC
# MAGIC - **MLflow Model Inference:** Learn more about using **MLflow** to track and load models for inference in a production environment [MLflow Model Inference Documentation](https://mlflow.org/docs/latest/deployment/deploy-model-locally/).
# MAGIC - **Pandas Function API in PySpark:** Explore how to use **Pandas Function APIs** in PySpark for distributed and scalable operations [Pandas Function API Documentation](https://docs.databricks.com/aws/en/pandas/pandas-function-apis).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Conclusion
# MAGIC In this lab, you explored the performance and versatility of Pandas, Spark, and Pandas API DataFrames for data processing. You learned how to convert between DataFrame types, apply Pandas UDFs for distributed inference, and train group-specific models using Pandas Function APIs. Finally, you performed group-specific inference with models logged in MLflow, demonstrating how to scale machine learning workflows efficiently using Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>