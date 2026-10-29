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
# MAGIC # Lecture - Build ML assets as Code
# MAGIC
# MAGIC Treating infrastructure as code makes ML projects reproducible, versioned, and easy to deploy everywhere. This lecture breaks down the anatomy of an ML project — its code, resources, and execution environment — and shows how Declarative Automation Bundles (DABs) describe all of it in YAML. It then contrasts traditional MLOps stacks with Databricks MLOps Stacks, a customizable framework that scaffolds CI/CD, infrastructure-as-code, and project structure for productionizing ML.
# MAGIC
# MAGIC <br>
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this lecture, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Understand why infrastructure as code is important.</li>
# MAGIC         <li>Explain complexities of anatomy of ML projects.</li>
# MAGIC         <li>Describe what problems DABs attempts to solve.</li>
# MAGIC         <li>Understand the difference between traditional MLOps stacks and Databricks MLOps stacks.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## A. Infrastructure as Code
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC - The way to describe infrastructure objects using general purpose or specialized language
# MAGIC
# MAGIC - Pros:
# MAGIC     - **Easy to create new** environments & maintain them
# MAGIC     - **Versioned**, easy to rollback in case of problems
# MAGIC     - **Changes** are made via pull requests, that are **tested** by build pipelines, **published** by release pipelines to multiple environments (staging/production)
# MAGIC     - Could help to build **completely automated releases**, without “manual” access to production environments
# MAGIC     - Usually it’s easy to **integrate with CI/CD** pipelines
# MAGIC
# MAGIC - Popular implementations: **Declarative Automation Bundles**, HashiCorp Terraform, Ansible, AWS CloudFormation, Azure Resource Management templates, ...

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## B. Anatomy of your ML projects in Databricks
# MAGIC
# MAGIC ### Let’s describe them
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:33%">
# MAGIC     <col style="width:34%">
# MAGIC     <col style="width:33%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding: 10px; font-size: 1em; border-right: 4px solid #ddd;">
# MAGIC         <h4>Consist of a variety of components</h4>
# MAGIC     </td>
# MAGIC     <td style="padding: 10px; font-size: 1em; border-right: 4px solid #ddd;">
# MAGIC         <h4>Produce a variety of data products</h4>
# MAGIC     </td>
# MAGIC     <td style="padding: 10px; font-size: 1em;">
# MAGIC         <h4>The task determines the components</h4>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 10px; font-size: 1em; border-right: 4px solid #ddd;">
# MAGIC         <ul>
# MAGIC             <li><span style="color: #FF3621;">Code</span>: Notebooks, Python .whl, JAR, dbt, etc.</li>
# MAGIC              <div style="height:8px;"></div>
# MAGIC             <li><span style="color: #FF3621;">Execution Environment</span>: Databricks Workspace, compute configuration.</li>
# MAGIC              <div style="height:8px;"></div>
# MAGIC             <li><span style="color: #FF3621;">Resources</span>: Lakeflow Jobs, MLflow Tracking, Model Serving and Registry, Dashboards and Alerts…</li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC     <td style="padding: 10px; font-size: 1em; border-right: 4px solid #ddd;">
# MAGIC         <ul>
# MAGIC             <li>Create tables and pipelines, machine learning models, dashboards, call external services, etc.</li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC     <td style="padding: 10px; font-size: 1em;">
# MAGIC         <ul>
# MAGIC             <li>A simple report might consist of a notebook running on single node compute.</li>
# MAGIC              <div style="height:8px;"></div>
# MAGIC             <li>A full MLOps pipeline would require MLflow, Feature Store, and Model Serving components.</li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## C. Let’s break this down further…
# MAGIC
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-left: 10px; font-size: 1em;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M5L1_96.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC     </td>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC         <ul>
# MAGIC         <li><b>ML Product</b>: Predictive model for market trend forecasting</li>
# MAGIC         <div style="height:6px;"></div>
# MAGIC         <li><b>Code</b>: Python Notebooks leveraging ML libraries (e.g., sparkML, scikit-learn, PyTorch)</li>
# MAGIC         <div style="height:6px;"></div>
# MAGIC         <li><b>Resources</b>: Lakeflow Job, MLflow for experiment tracking</li>
# MAGIC         <div style="height:6px;"></div>
# MAGIC         <li><b>Execution Environment</b>: Databricks Workspace with ML runtime, compute configuration optimized for training</li>
# MAGIC         <div style="height:6px;"></div>
# MAGIC         <li><b>Data Source</b>: Historical web data and market reports for model training</li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## D. What if our task changes?
# MAGIC
# MAGIC ### Just add the necessary components to complete the task
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-left: 10px; font-size: 1em;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M5L1_97.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC     </td>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC         <ul>
# MAGIC         <li><b>ML Product</b>: Predictive model for market trend forecasting <span style="color: #FF3621;">+persisted table</span></li>
# MAGIC         <div style="height:6px;"></div>
# MAGIC         <li><b>Code</b>: Python Notebooks</li>
# MAGIC         <div style="height:6px;"></div>
# MAGIC         <li><b>Resources</b>: Lakeflow Job, MLflow for experiment tracking <span style="color: #FF3621;">+Lakeflow Spark Declarative Pipelines</span></li>
# MAGIC         <div style="height:6px;"></div>
# MAGIC         <li><b>Execution Environment</b>: Databricks Workspace with ML runtime, compute configuration</li>
# MAGIC         <div style="height:6px;"></div>
# MAGIC         <li><b>Data Source</b>: Historical web data and market reports for model training <span style="color: #FF3621;">+Marketplace data</span></li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## E. Declarative Automation Bundles 
# MAGIC
# MAGIC ### Write code once, deploy everywhere
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:33%">
# MAGIC     <col style="width:34%">
# MAGIC     <col style="width:33%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding: 10px; font-size: 1em; border-right: 4px solid #ddd; text-align: center;">
# MAGIC         <h4>What are Declarative Automation Bundles?</h4>
# MAGIC     </td>
# MAGIC     <td style="padding: 10px; font-size: 1em; border-right: 4px solid #ddd; text-align: center;">
# MAGIC         <h4>How do bundles work?</h4>
# MAGIC     </td>
# MAGIC     <td style="padding: 10px; font-size: 1em; text-align: center;">
# MAGIC         <h4>Where are bundles used?</h4>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 10px; font-size: 1em; border-right: 4px solid #ddd;">
# MAGIC         <p><b>YAML</b> files that specify the artifacts, resources, and configurations of a Databricks project.</p>
# MAGIC     </td>
# MAGIC     <td style="padding: 10px; font-size: 1em; border-right: 4px solid #ddd;">
# MAGIC        <p>The <b>new databricks CLI</b> has functions to <b>validate, deploy and run</b> Declarative Automation Bundles using bundle.yml files.</p>
# MAGIC     </td>
# MAGIC     <td style="padding: 10px; font-size: 1em;">
# MAGIC         <p>Bundles are useful during <b>development and CI/CD processes</b>.</p>
# MAGIC         <br>
# MAGIC         <br>
# MAGIC         <br>
# MAGIC         <br>
# MAGIC         <a href="https://docs.databricks.com/aws/en/dev-tools/bundles#next-steps" style="color:#FF3621;">See Sample Bundle Walkthroughs & Tip</a>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## F. A closer look 
# MAGIC
# MAGIC #### Name and default Workspace
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:40%">
# MAGIC     <col style="width:60%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC         <ul>
# MAGIC         <li><b>Resource configurations</b></li>
# MAGIC             <ul><li>Jobs, Lakeflow Spark Declarative Pipelines, MLflow, etc.</li>
# MAGIC             <li>Follows REST API schema</li></ul>
# MAGIC             <div style="height:12px;"></div>
# MAGIC         <li><b>Environment-based specs</b></li>
# MAGIC             <ul><li>Control project behavior in different environments</li></ul>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC     <td style="padding-left: 10px; font-size: 1em;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M5L1_99.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## G. Where can we use bundles?
# MAGIC
# MAGIC ### As part of Development and CI/CD processes
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 10px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M5L1_100.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## H. Implementing and managing infrastructure for MLOps is difficult! 
# MAGIC
# MAGIC **Companies spend 80% of resources and time on DevOps activities in a ML project, leaving little time to add value through new models.**
# MAGIC
# MAGIC <br>
# MAGIC <div style="text-align: center; margin-top: 10px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M5L1_101.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## I. Default MLOps Stack
# MAGIC
# MAGIC ### Provides out-of-the-box of the following tooling
# MAGIC
# MAGIC <br>
# MAGIC <table style="border-collapse: collapse; width:100%;">
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <th style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;">Component</th>
# MAGIC     <th style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;">Tool in Databricks</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;">ML model development code</td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;"><a href="https://docs.databricks.com/aws/en/notebooks" style="color:#2176FF;">Databricks notebooks, MLflow</a></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;">Feature development and management</td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;"><a href="https://docs.databricks.com/aws/en/machine-learning/feature-store" style="color:#2176FF;">Feature engineering</a></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;">ML model repository</td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;"><a href="https://docs.databricks.com/aws/en/machine-learning/manage-model-lifecycle" style="color:#2176FF;">Models in Unity Catalog</a></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;">ML model serving</td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;"><a href="https://docs.databricks.com/aws/en/machine-learning/model-serving" style="color:#2176FF;">Model Serving</a></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;">Infrastructure-as-code</td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;"><a href="https://docs.databricks.com/aws/en/dev-tools/bundles" style="color:#2176FF;">Declarative Automation Bundles</a></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;">Orchestrator</td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;"><a href="https://docs.databricks.com/aws/en/jobs" style="color:#2176FF;">Lakeflow Jobs</a></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;">CI/CD</td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;"><a href="https://docs.github.com/en/actions" style="color:#2176FF;">GitHub Actions, Azure DevOps</a></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;">Data and model performance monitoring</td>
# MAGIC     <td style="border: 1px solid #ddd; padding: 8px; font-size: 1.1em;"><a href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-quality-monitoring/data-profiling" style="color: #FF3621;">Data Profiling </a></td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## J. MLOps Stacks create a repo containing CI/CD, infra-as-code and sample project structure for productionizing ML
# MAGIC
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:40%">
# MAGIC     <col style="width:60%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1.15em;">
# MAGIC         <ul>
# MAGIC         <li>Use <b>‘databricks bundle init mlops-stack’</b> and <span style="color: #FF3621;">answer some questions</span> to generate the core folder structure on workstation using CLI.</li>
# MAGIC         <div style="height:12px;"></div>
# MAGIC         <li>All environment and resource configurations are stored in <b>databricks.yml</b> as well as other YAML files inside <b>resources/</b> folder.</li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC     <td style="padding-left: 10px; font-size: 1em;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M5L1_103.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## K. Databricks MLOps Stacks
# MAGIC
# MAGIC ### A customizable framework for managing the ML lifecycle on Databricks
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding: 10px; font-size: 1.25em; border-right: 4px solid #ddd;">
# MAGIC         <p>Follows all <b>MLOps best practices</b></p>
# MAGIC     </td>
# MAGIC     <td style="padding: 10px; font-size: 1.25em;">
# MAGIC         <p>Focus on <b>ML</b>, not infrastructure</p>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 10px; font-size: 1em; border-right: 4px solid #ddd;">
# MAGIC         <ul>
# MAGIC             <li>Create and manage <b>production MLOps infrastructure</b> on Databricks.</li>
# MAGIC             <div style="height:10px;"></div>
# MAGIC             <li>Integrates with common CI/CD providers like <b>GitHub</b> and <b>Azure DevOps</b>.</li>
# MAGIC             <div style="height:10px;"></div>
# MAGIC             <li>Manage AI assets (experiments, features, models, monitoring, etc.) and workflows as <b>IaC with Declarative Automation Bundles</b>.</li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC     <td style="padding: 10px; font-size: 1em;">
# MAGIC         <ul>
# MAGIC             <li>DS get started with <b>project development option</b> in Stacks.</li>
# MAGIC             <div style="height:10px;"></div>
# MAGIC             <li>MLEs easily set up CI/CD and customize the architecture as needed via the <b>CI/CD option</b> in Stacks.</li>
# MAGIC             <div style="height:10px;"></div>
# MAGIC             <li>DS then safely deploy project to production through secure CI/CD pipelines and workflows setup by the MLEs.</li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>