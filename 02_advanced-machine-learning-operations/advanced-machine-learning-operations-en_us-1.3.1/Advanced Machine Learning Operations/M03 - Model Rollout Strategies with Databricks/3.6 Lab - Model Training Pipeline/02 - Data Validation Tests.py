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
# MAGIC ## Workflow Notebook - Data Validation Tests
# MAGIC
# MAGIC This notebook's purpose is to validate schema, missing values, and confirm nonnegative values using unittest.

# COMMAND ----------

# MAGIC %pip install -U databricks-feature-engineering

# COMMAND ----------

catalog = dbutils.widgets.get(<FILL_IN>)
schema = dbutils.widgets.get(<FILL_IN>)
silver_table_name = dbutils.widgets.get('<FILL_IN>')

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Read the catalog, schema, and silver-table widgets
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC catalog = dbutils.widgets.get('catalog')
# MAGIC schema = dbutils.widgets.get('schema')
# MAGIC silver_table_name = dbutils.widgets.get('silver_table_name')
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

import unittest
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, LongType
from pyspark.sql.functions import col, sum, min


class TestDataValidation(unittest.TestCase):
    """
    Unit tests for schema validation, missing values, and non-negative values in PySpark DataFrames.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up shared resources for the tests.
        """
        # Load the test DataFrame (assume a table named 'diabetes' is present)
        cls.df = spark.read.format("delta").table(f"{catalog}.{schema}.{silver_table_name}").select(
            'id', 'Diabetes_binary', 'HighBP', 'BMI', 'Smoker', 'Stroke', 
            'HeartDiseaseorAttack', 'Age'
        )

    def test_validate_schema(self):
        """
        Test if the DataFrame schema matches the expected schema.
        """
        expected_schema = StructType([
            <FILL_IN>
        ])
        actual_schema = self.df.schema
        self.assertEqual(
            <FILL_IN>
            f"Schema validation failed.\nExpected: {expected_schema}\nActual: {actual_schema}"
        )

    def test_validate_no_missing_values(self):
        """
        Test that there are no missing (null) values in the DataFrame.
        """
        missing_values = self.df.agg(*[
            sum(col(c).isNull().cast("int")).alias(c) for c in self.df.columns
        ]).collect()[0].asDict()

        missing_columns = {col: missing_values[col] for col in self.df.columns if missing_values[col] > 0}
        self.assertFalse(
            <FILL_IN>
            f"Missing values found in the following columns: {missing_columns}"
        )

    def test_validate_non_negative_values(self):
        """
        Test that all columns in the DataFrame contain non-negative values (>= 0).
        """
        negative_values = self.df.agg(*[
            min(col(c)).alias(c) for c in self.df.columns
        ]).collect()[0].asDict()

        negative_columns = {col: negative_values[col] for col in self.df.columns if negative_values[col] < 0}
        self.assertFalse(
            <FILL_IN>
            f"Negative values found in the following columns: {negative_columns}"
        )


# Run the tests
suite = unittest.<FILL_IN>
unittest.<FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #2574B5); color: white; padding: 14px 20px; cursor: pointer; font-weight: 700; font-size: 13pt; border-radius: 8px; user-select: none; display: flex; align-items: center; gap: 10px;">
# MAGIC <span style="background: rgba(255,255,255,0.2); border-radius: 4px; padding: 2px 8px; font-size: 11pt;">ANSWER</span> Define unit tests for schema, nulls, and non-negative values
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 18px 20px; background: #F8F9FC; position: relative;"><button onclick="var c=this.parentElement.querySelector('pre code');var t=document.createElement('textarea');t.value=c?c.textContent:'';t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='Copied!';var b=this;setTimeout(function(){b.textContent='Copy'},1500)" style="position:absolute;top:10px;right:12px;background:linear-gradient(135deg,#1B5162,#2574B5);color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:11pt;font-weight:600;cursor:pointer;z-index:2;">Copy</button>
# MAGIC
# MAGIC ```python
# MAGIC import unittest
# MAGIC from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, LongType
# MAGIC from pyspark.sql.functions import col, sum, min
# MAGIC
# MAGIC
# MAGIC class TestDataValidation(unittest.TestCase):
# MAGIC     """
# MAGIC     Unit tests for schema validation, missing values, and non-negative values in PySpark DataFrames.
# MAGIC     """
# MAGIC
# MAGIC     @classmethod
# MAGIC     def setUpClass(cls):
# MAGIC         """
# MAGIC         Set up shared resources for the tests.
# MAGIC         """
# MAGIC         # Load the test DataFrame (assume a table named 'diabetes' is present)
# MAGIC         cls.df = spark.read.format("delta").table(silver_table_name).select(
# MAGIC             'id', 'Diabetes_binary', 'HighBP', 'BMI', 'Smoker', 'Stroke', 
# MAGIC             'HeartDiseaseorAttack', 'Age'
# MAGIC         )
# MAGIC
# MAGIC     def test_validate_schema(self):
# MAGIC         """
# MAGIC         Test if the DataFrame schema matches the expected schema.
# MAGIC         """
# MAGIC         expected_schema = StructType([
# MAGIC             StructField("id", LongType(), True),
# MAGIC             StructField("Diabetes_binary", DoubleType(), True),
# MAGIC             StructField("HighBP", DoubleType(), True),
# MAGIC             StructField("BMI", DoubleType(), True),
# MAGIC             StructField("Smoker", DoubleType(), True),
# MAGIC             StructField("Stroke", DoubleType(), True),
# MAGIC             StructField("HeartDiseaseorAttack", DoubleType(), True),
# MAGIC             StructField("Age", DoubleType(), True),
# MAGIC         ])
# MAGIC         actual_schema = self.df.schema
# MAGIC         self.assertEqual(
# MAGIC             actual_schema, expected_schema,
# MAGIC             f"Schema validation failed.\nExpected: {expected_schema}\nActual: {actual_schema}"
# MAGIC         )
# MAGIC
# MAGIC     def test_validate_no_missing_values(self):
# MAGIC         """
# MAGIC         Test that there are no missing (null) values in the DataFrame.
# MAGIC         """
# MAGIC         missing_values = self.df.agg(*[
# MAGIC             sum(col(c).isNull().cast("int")).alias(c) for c in self.df.columns
# MAGIC         ]).collect()[0].asDict()
# MAGIC
# MAGIC         missing_columns = {col: missing_values[col] for col in self.df.columns if missing_values[col] > 0}
# MAGIC         self.assertFalse(
# MAGIC             missing_columns,
# MAGIC             f"Missing values found in the following columns: {missing_columns}"
# MAGIC         )
# MAGIC
# MAGIC     def test_validate_non_negative_values(self):
# MAGIC         """
# MAGIC         Test that all columns in the DataFrame contain non-negative values (>= 0).
# MAGIC         """
# MAGIC         negative_values = self.df.agg(*[
# MAGIC             min(col(c)).alias(c) for c in self.df.columns
# MAGIC         ]).collect()[0].asDict()
# MAGIC
# MAGIC         negative_columns = {col: negative_values[col] for col in self.df.columns if negative_values[col] < 0}
# MAGIC         self.assertFalse(
# MAGIC             negative_columns,
# MAGIC             f"Negative values found in the following columns: {negative_columns}"
# MAGIC         )
# MAGIC
# MAGIC
# MAGIC # Run the tests
# MAGIC suite = unittest.TestLoader().loadTestsFromTestCase(TestDataValidation)
# MAGIC result = unittest.TextTestRunner().run(suite)
# MAGIC
# MAGIC # Check the number of failures
# MAGIC if len(result.failures) > 0:
# MAGIC     raise Exception(f"Test failed: More than 1 failure detected ({len(result.failures)} failures).")
# MAGIC ```
# MAGIC
# MAGIC </div>
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>