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

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Lecture - Streamlining MLOps
# MAGIC
# MAGIC Modern MLOps is built on four continuous practices. This lecture defines Continuous Integration, Continuous Training, Continuous Deployment, and Continuous Monitoring, and maps each to the concrete pipelines that implement it. You will see how these practices fit together into a recommended end-to-end architecture and which core Databricks tools — Lakeflow Jobs, Managed MLflow, and Git folders — make them work.
# MAGIC
# MAGIC <br>
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this lecture, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Understand components of CI, CT, CD, and CM</li>
# MAGIC         <li>Understand recommended architecture continuous frameworks</li>
# MAGIC         <li>Understand core tools available with Databricks for continuous frameworks</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## A. Continuous Terminology
# MAGIC
# MAGIC ### <span style="color:#4d7dad;"><b>DevOps</b></span>, <span style="color:#ed5e47;"><b>Data Ops</b></span>, <span style="color:#4ba973;"><b>Model Ops</b></span>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC - **Continuous Integration (CI)** extends the testing and validating code by adding testing and validating of <span style="color:#4d7dad;"><b>code</b></span>, <span style="color:#ed5e47;"><b>data</b></span> and <span style="color:#4ba973;"><b>models</b></span>.
# MAGIC
# MAGIC - **Continuous Training (CT)** <span style="color:#4d7dad;"><b>automatically</b></span> (re)trains ML <span style="color:#4ba973;"><b>models</b></span> on <span style="color:#ed5e47;"><b>data</b></span> for (re)deployment. 
# MAGIC
# MAGIC - **Continuous Deployment (CD)** concerned with <span style="color:#4d7dad;"><b>delivery</b></span> of a ML training pipeline that <span style="color:#4d7dad;"><b>automatically</b></span> deploys another ML <span style="color:#4ba973;"><b>model</b></span> and needed <span style="color:#ed5e47;"><b>stores</b></span>.
# MAGIC
# MAGIC - **Continuous Monitoring (CM)** concerned with <span style="color:#4d7dad;"><b>continuous monitoring</b></span> of <span style="color:#ed5e47;"><b>production data</b></span> and <span style="color:#4ba973;"><b>model performance metrics</b></span>.
# MAGIC
# MAGIC <hr>
# MAGIC
# MAGIC <div style="background-color: #1b5162; color: #fff; padding: 12px 16px; border-radius: 4px; margin: 8px 0;">
# MAGIC   <b>Goal</b>: Increase developer productivity, improve model quality, code quality, and accelerate trained model delivery.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## B. Continuous Integration and Training
# MAGIC
# MAGIC <br>
# MAGIC <div style="text-align: center; margin-top: 0px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L1_28.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC
# MAGIC <div style="display:flex;gap:0;margin:24px 0;align-items:stretch;">
# MAGIC
# MAGIC   <div style="flex:1;background:#1b5162;border-top:4px solid #000;border-radius:12px 0 0 12px;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#fff;font-size:16px;margin-bottom:14px;">Continuous Integration (CI)</div>
# MAGIC     <ul style="font-size:16px;color:#fff;line-height:2;margin:0;padding-left:20px;">
# MAGIC         <li>Automatically trigger pipelines after code commits.</li>
# MAGIC         <li>Run tests and validation on code, data, and models.</li>
# MAGIC         <li>Track all changes via version control.</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;background:#757575;border-top:4px solid #000;border-radius:0 12px 12px 0;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#fff;font-size:16px;margin-bottom:14px;">Continuous Training (CT)</div>
# MAGIC     <ul style="font-size:16px;color:#fff;line-height:2;margin:0;padding-left:20px;">
# MAGIC         <li>Retrain models automatically when new code or data is available or performance indicates.</li>
# MAGIC         <li>Log all experiments and model metrics for reproducibility.</li>
# MAGIC         <li>Automate hyperparameter tuning.</li>
# MAGIC         <li>Evaluate and validate model.</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## C. Continuous Integration Deeper Dive
# MAGIC
# MAGIC ### Possible Pipelines…
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC         <ul>
# MAGIC         <li><b>Code Testing and Integration Pipeline:</b></li>
# MAGIC             <ul><li>Ensures code changes are merged and tested without breaking the workflow.</li></ul>
# MAGIC             <div style="height:10px;"></div>
# MAGIC         <li><b>Data Pipeline Validation Pipeline:</b></li>
# MAGIC             <ul><li>Verifies the integrity of data pipelines and their integration with existing workflows.</li></ul>
# MAGIC             <div style="height:10px;"></div>
# MAGIC         <li><b>Feature Engineering Validation Pipeline:</b></li>
# MAGIC             <ul><li>Validates the quality and consistency of features produced by updated feature engineering scripts.</li></ul>
# MAGIC             <div style="height:10px;"></div>
# MAGIC         <li><b>Automated Security and Compliance Testing Pipeline:</b></li> 
# MAGIC             <ul><li>Ensures that security and compliance checks are in place for code and data handling.</li></ul>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 0px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L1_29.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## D. Continuous Training Deeper Dive
# MAGIC
# MAGIC ### Possible Pipelines…
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC         <ul>
# MAGIC         <li><b>Data Ingestion and Feature Engineering Pipeline:</b></li>
# MAGIC             <ul><li>Automate data collection, cleaning, feature creation, and validation.</li></ul>
# MAGIC             <div style="height:8px;"></div>
# MAGIC         <li><b>Model Tuning Pipeline (Optional):</b></li>
# MAGIC             <ul><li>Optimize performance by tuning hyperparameters automatically.</li>
# MAGIC             <li>May have been previous established.</li></ul>
# MAGIC             <div style="height:8px;"></div>
# MAGIC         <li><b>Model Training Pipeline:</b></li>
# MAGIC             <ul><li>Automate model training with the latest data, logging results and artifacts.</li></ul>
# MAGIC             <div style="height:8px;"></div>
# MAGIC         <li><b>Model Evaluation Pipeline:</b></li>
# MAGIC             <ul><li>Test and compare models against production versions to ensure improvements.</li></ul>
# MAGIC             <div style="height:8px;"></div>
# MAGIC         <li><b>Model Registry and Promotion Pipeline:</b></li>
# MAGIC             <ul><li>Manage and promote through different lifecycle stages.</li></ul>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 0px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L1_30.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## E. Continuous Deployment and Monitoring 
# MAGIC
# MAGIC
# MAGIC <br>
# MAGIC <div style="text-align: center; margin-top: 0px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L1_31.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC
# MAGIC <div style="display:flex;gap:0;margin:24px 0;align-items:stretch;">
# MAGIC
# MAGIC   <div style="flex:1;background:#618794;border-top:4px solid #000;border-radius:12px 0 0 12px;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#fff;font-size:16px;margin-bottom:14px;">Continuous Deployment (CD)</div>
# MAGIC     <ul style="font-size:16px;color:#fff;line-height:2;margin:0;padding-left:20px;">
# MAGIC         <li>Validate models in staging before production.</li>
# MAGIC         <li>Deploy code or models automatically across environments.</li>
# MAGIC         <li>Use manual approval (Continuous Delivery) or automate deployments (Continuous Deployment).</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;background:#9e9e9e;border-top:4px solid #000;border-radius:0 12px 12px 0;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#fff;font-size:16px;margin-bottom:14px;">Continuous Monitoring (CM)</div>
# MAGIC     <ul style="font-size:16px;color:#fff;line-height:2;margin:0;padding-left:20px;">
# MAGIC         <li>Track model performance and drift in production.</li>
# MAGIC         <li>Automate retraining or notifications based on performance metrics.</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## F. Continuous Deployment Deeper Dive
# MAGIC
# MAGIC ### Possible Pipelines…
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC         <ul>
# MAGIC         <li><b>Model Validation Pipeline:</b></li>
# MAGIC             <ul><li>Validate models using automated tests and compliance checks.</li></ul>
# MAGIC             <div style="height:8px;"></div>
# MAGIC         <li><b>Deployment to Staging Pipeline:</b></li>
# MAGIC             <ul><li>Deploy the model to the staging environment for pre-production testing.</li></ul>
# MAGIC             <div style="height:8px;"></div>
# MAGIC         <li><b>Staging Validation Pipeline:</b></li>
# MAGIC             <ul><li>Conduct integration, E2E, and performance testing in staging.</li></ul>
# MAGIC             <div style="height:8px;"></div>
# MAGIC         <li><b>Deployment to Production Pipeline:</b></li>
# MAGIC             <ul><li>Move the validated model to production, using options like canary or blue/green deployment.</li></ul>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 0px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L1_32.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## G. Continuous Monitoring Deeper Dive
# MAGIC
# MAGIC ### Possible Pipelines…
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC         <ul>
# MAGIC         <li><b>Logging and Monitoring Pipeline:</b></li>
# MAGIC             <ul><li>Logs all predictions, requests, errors, and performance metrics for future analysis.</li></ul>
# MAGIC             <div style="height:8px;"></div>
# MAGIC         <li><b>Drift Detection Pipeline:</b></li>
# MAGIC             <ul><li>Continuously monitors data and concept drift, triggering retraining if drift is detected.</li></ul>
# MAGIC             <div style="height:8px;"></div>
# MAGIC         <li><b>Model Performance Monitoring Pipeline:</b></li>
# MAGIC             <ul><li>Tracks key performance metrics like accuracy and latency, triggering alerts if performance degrades.</li></ul>
# MAGIC             <div style="height:8px;"></div>
# MAGIC         <li><b>Model Health Monitoring Pipeline:</b></li>
# MAGIC             <ul><li>Monitors infrastructure, ensuring that the model endpoint remains online and resources are utilized efficiently.</li></ul>
# MAGIC             <div style="height:8px;"></div>
# MAGIC         <li><b>Feedback Loop Pipeline:</b></li>
# MAGIC             <ul><li>Collects real-world feedback and integrates it into the model for continuous improvement.</li></ul>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 0px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L1_33.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## H. CI/CT/CD/CM & Recommended Architecture
# MAGIC
# MAGIC
# MAGIC <br>
# MAGIC <div style="text-align: center; margin-top: 0px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L1_34.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## I. Core Databricks ML CI/CT/CD/CM Tools
# MAGIC
# MAGIC ### Automating and Streamlining Machine Learning Pipelines
# MAGIC
# MAGIC
# MAGIC - **Lakeflow Jobs**
# MAGIC     - Jobs
# MAGIC         - Automates the scheduling and orchestration of ML tasks across different stages.
# MAGIC     - Lakeflow Spark Declarative Pipelines
# MAGIC         - Manages ETL pipelines for real-time data, ensuring fresh data for model training.
# MAGIC
# MAGIC - **Managed MLflow**
# MAGIC     - Tracks experiments, manages models, and automates deployment across environments.
# MAGIC
# MAGIC - **Databricks Git Folder**
# MAGIC     - Enables version control of code and notebooks for collaborative development.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>