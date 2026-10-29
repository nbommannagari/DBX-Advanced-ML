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
# MAGIC
# MAGIC
# MAGIC ## Advanced Machine Learning Operations
# MAGIC
# MAGIC In this course, you will be provided with a comprehensive understanding of the machine learning lifecycle and MLOps, emphasizing best practices for data and model management, testing, and scalable architectures. It covers key MLOps components, including CI/CD, pipeline management, and environment separation, while showcasing Databricks’ tools for automation and infrastructure management, such as Declarative Automation Bundles (DABs), Lakeflow Jobs, and Databricks Model Serving. 
# MAGIC
# MAGIC You will learn about data profiling, custom metrics, drift detection, model rollout strategies, A/B testing, and the principles of reliable MLOps systems, providing a holistic view of implementing and managing ML projects in Databricks.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC The content was developed for participants with these skills/knowledge/abilities:  
# MAGIC - The user should have intermediate-level knowledge of traditional machine learning concepts, development, and the use of Python and Git for ML projects.
# MAGIC - It is recommended that the user has intermediate-level experience with Python. 
# MAGIC - Databricks Academy content is designed to be run on the provided Vocareum environment only. 
# MAGIC - Databricks does not guarantee that the content can be run successfully on any other instances of a Databricks Workspace.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Course Agenda  
# MAGIC The following modules are part of the **Advanced Machine Learning Operations** course by **Databricks Academy**.
# MAGIC
# MAGIC | # | Module Name | Lesson Name |
# MAGIC |---|-------------|-------------|
# MAGIC | 1 | [Overview of Machine Learning Operations]($./M01 - Overview of Machine Learning Operations on Databricks) | • [Lecture: Review of MLOps]($./M01 - Overview of Machine Learning Operations on Databricks/1.1 Lecture - Review of MLOps) <br> • [Lecture: Streamlining Development to Deployment]($./M01 - Overview of Machine Learning Operations on Databricks/1.2 Lecture - Streamlining Development to Deployment) |
# MAGIC | 2 | [Streamlining MLOps with Databricks]($./M02 - Streamlining MLOps with Databricks) | • [Lecture: Streamlining MLOps]($./M02 - Streamlining MLOps with Databricks/2.1 Lecture - Streamlining MLOps) <br> • [Lecture: Streamlining MLOps with Databricks]($./M02 - Streamlining MLOps with Databricks/2.2 Lecture - Streamlining MLOps with Databricks) <br> • [Setup: Generate Tokens]($./M02 - Streamlining MLOps with Databricks/0 - Generate Tokens) <br> • [Demo: Building a CI-CD Pipeline with Databricks CLI]($./M02 - Streamlining MLOps with Databricks/2.3 Demo - Building a CI-CD Pipeline with Databricks CLI) <br> • [Lab: Building a CI-CD Pipeline with Databricks CLI]($./M02 - Streamlining MLOps with Databricks/2.4 Lab - Building a CI-CD Pipeline with Databricks CLI) |
# MAGIC | 3 | [Model Rollout Strategies with Databricks]($./M03 - Model Rollout Strategies with Databricks) | • [Lecture: Automate Comprehensive Testing]($./M03 - Model Rollout Strategies with Databricks/3.1 Lecture - Automate Comprehensive Testing) <br> • [Demo: Common Testing Strategies]($./M03 - Model Rollout Strategies with Databricks/3.2 Demo - Common Testing Strategies) <br> • [Demo: Integration Tests with Lakeflow Jobs]($./M03 - Model Rollout Strategies with Databricks/3.3 Demo - Integration Tests with Lakeflow Jobs) <br> • [Lecture: Model Rollout Strategies with Databricks]($./M03 - Model Rollout Strategies with Databricks/3.4 Lecture - Model Rollout Strategies with Databricks) <br> • [Demo: Model Rollout Strategies with Databricks Model Serving]($./M03 - Model Rollout Strategies with Databricks/3.5 Demo - Model Rollout Strategies with Databricks Model Serving) <br> • [Lab: Rollout Strategies with Lakeflow Jobs]($./M03 - Model Rollout Strategies with Databricks/3.6 Lab - Rollout Strategies with Lakeflow Jobs) |
# MAGIC | 4 | [Data Profiling]($./M04 - Data Profiling) | • [Lecture: Data Profiling]($./M04 - Data Profiling/4.1 Lecture - Data Profiling) <br> • [Demo: Data Profiling Model Quality]($./M04 - Data Profiling/4.2 Demo - Data Profiling Model Quality) <br> • [Lab: Monitoring Drift with Data Profiling]($./M04 - Data Profiling/4.3 Lab - Monitoring Drift with Data Profiling) |
# MAGIC | 5 | [Build ML Assets as Code]($./M05 - Build ML Assets as Code) | • [Lecture: Build ML assets as Code]($./M05 - Build ML Assets as Code/5.1 Lecture - Build ML assets as Code) <br> • [Setup: Generate Tokens]($./M05 - Build ML Assets as Code/0 - Generate Tokens) <br> • [Demo: Working with Declarative Automation Bundles]($./M05 - Build ML Assets as Code/5.2 Demo - Working with Declarative Automation Bundles) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * Use Databricks Runtime version: **`17.3.x-cpu-ml-scala2.13`** for running all demo and lab notebooks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC **Instruction:** In case of any issues, please refer to the [Troubleshooting Content Notebook]($./TROUBLESHOOTING_CONTENT).

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>