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
# MAGIC # Lecture - Streamlining MLOps with Databricks
# MAGIC
# MAGIC Continuous workflows need an orchestration engine. This lecture focuses on Lakeflow Jobs, the fully managed service for coordinating complex, multi-step ML and data flows on Databricks. You will explore its building blocks — jobs, tasks, control flows, and triggers — together with its Unity Catalog integration, flexible authoring options, real-time monitoring, and built-in AI, connecting the MLflow model lifecycle to production-grade, automated pipelines.
# MAGIC
# MAGIC <br>
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this lecture, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Understand the importance of modern task orchestration for managing complex multi-data flows in machine learning.</li>
# MAGIC         <li>Explore how Lakeflow Jobs provide fully managed, cloud-based task orchestration.</li>
# MAGIC         <li>Identify the building blocks of Lakeflow Jobs, including jobs, control flows, and triggers.</li>
# MAGIC         <li>Understand how to utilize control flows within Lakeflow Jobs.</li>
# MAGIC         <li>Develop the ability to use simple authoring tools designed for all data practitioners in Databricks.</li>
# MAGIC         <li>Learn how to leverage real-time monitoring to gain actionable insights from workflows.</li>
# MAGIC         <li>Understand how seamless AI integration enhances Lakeflow Jobs.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## A. mlFlow Model Lifecycle
# MAGIC
# MAGIC
# MAGIC <br>
# MAGIC <div style="text-align: center; margin-top: 0px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_38.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## B. Modern Machine Learning Requires
# MAGIC
# MAGIC ### Modern Task Orchestration
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC       <h4>Requirements:</h4>
# MAGIC         <ul>
# MAGIC         <li>Handle <b>multiple, varied data sources</b> and assets including streaming</li>
# MAGIC         <li>Orchestrate complex <b>multi-step data</b> flows</li>
# MAGIC         <li>Maintain <b>dependencies</b> between different steps and workflows</li>
# MAGIC         <li>Support multiple <b>use cases</b> such as ETL, BI and AI including real-time use cases</li>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 0px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_39.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## C. Lakeflow Jobs
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 0px; font-size: 1em;">
# MAGIC       Lakeflow Jobs is a <b>fully-managed cloud-based general-purpose task orchestration service</b> for the <u>entire unified Lakehouse</u>.
# MAGIC       <ul>
# MAGIC         <li>Allows you to <b>build ETL/ML task orchestration</b> using a simple <b>UI, CLI, bundles, etc</b>.</li>
# MAGIC         <li><b>No Additional Cost</b></li>
# MAGIC         <li><b>99.95%</b> Uptime</li>
# MAGIC         <li>Reduces infrastructure <b>overhead</b></li>
# MAGIC         <li>Cloud-provider <b>independent</b></li>
# MAGIC         <li><b>Serverless</b> Compute is Generally Available</li>
# MAGIC             <ul><li>< 60 second startup</li></ul>
# MAGIC         </ul>    
# MAGIC    </td>
# MAGIC     <td style="padding-left: 0px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_40.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## D. Fully Integrated with Unity Catalog
# MAGIC
# MAGIC ### Automated Governance
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:30%">
# MAGIC     <col style="width:70%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC         <ul>
# MAGIC         <li>Automatic real-time data lineage for Lakeflow Jobs</li>
# MAGIC         <div style="height:10px;"></div>
# MAGIC         <li>Connect tables to Lakeflow Jobs</li>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 10px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_41.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## E. Building Blocks of Lakeflow Jobs
# MAGIC
# MAGIC ### A unit of orchestration in Lakeflow Jobs is called a Job.
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:20%">
# MAGIC     <col style="width:80%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em; background-color: #1b5162; color: #fff; vertical-align: middle; border: 4px solid #ddd;">
# MAGIC      <b>Jobs</b> consist of one or more <b>Tasks</b>
# MAGIC     </td>
# MAGIC     <td style="padding-left: 10px; border: 2px solid #ddd;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_42_1.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em; background-color: #1b5162; color: #fff; vertical-align: middle; border: 4px solid #ddd;">
# MAGIC         <b>Control flows</b> can be established between <b>Tasks</b>
# MAGIC     </td>
# MAGIC     <td style="padding-left: 10px; 2px solid #ddd;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_42_2.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em; background-color: #1b5162; color: #fff; vertical-align: middle; border: 4px solid #ddd;">
# MAGIC         <b>Jobs</b> supports different <b>Triggers</b>
# MAGIC     </td>
# MAGIC     <td style="padding-left: 10px; 2px solid #ddd;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_42_3.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## F. Modular Task Management and Orchestration
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:33%">
# MAGIC     <col style="width:34%">
# MAGIC     <col style="width:33%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em; vertical-align: middle; border-right: 4px solid #ddd;">
# MAGIC      <b>Source of your Task code:</b>
# MAGIC     </td>
# MAGIC     <td style="padding-right: 10px; font-size: 1em; vertical-align: bottom; border-right: 4px solid #ddd;">
# MAGIC      <b>Install external dependent libraries<br>(e.g. JARs, Python Eggs and Wheels)<br>from the following sources:</b>
# MAGIC     </td>
# MAGIC     <td style="padding-right: 10px; font-size: 1em; vertical-align: middle;">
# MAGIC      <b>Pass Parameters:</b>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding-left: 0px; border-right: 4px solid #ddd;">        
# MAGIC         <div style="display: flex; align-items: flex-start; gap: 16px;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_43_1.png" alt="Image description" style="max-width: 80px; height: auto; border-radius: 0px;">
# MAGIC         <div>
# MAGIC             <ul style="margin: 0; padding-left: 18px;">
# MAGIC             <li><b>Databricks Workspace</b></li>
# MAGIC             <ul style="margin: 0; padding-left: 6px;"><li>Quickstart from the File Browser</li></ul>
# MAGIC             </ul>
# MAGIC             <div style="height:30px;"></div>
# MAGIC             <ul style="margin: 0; padding-left: 18px;">
# MAGIC             <li><b>Remote Git Repository</b></li>
# MAGIC             <ul style="margin: 0; padding-left: 6px;"><li>Simplify CI/CD</li></ul>
# MAGIC             </ul>
# MAGIC         </div>
# MAGIC         </div>
# MAGIC     </td>
# MAGIC     <td style="padding-left: 10px; border-right: 4px solid #ddd; vertical-align: top;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_43_2.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC     <td style="padding-left: 10px;">
# MAGIC      <b>Parameters can be passed to:</b> <br>
# MAGIC       Notebook, JAR, Spark Submit, Jobs, SQL, Python Wheel and Script Tasks.<br><br>
# MAGIC       Jobs and Tasks can pass parameters.
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 0px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_43_3.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## G. More on Control Flows
# MAGIC
# MAGIC ### Build more **dynamic, mature, and sophisticated** data pipelines with Lakeflow Jobs!
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:33%">
# MAGIC     <col style="width:34%">
# MAGIC     <col style="width:33%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="font-size: 1em; vertical-align: middle; border-right: 4px solid #ddd; text-align: center; background-color: #98102a; color: #fff;">
# MAGIC      <h4>Task Dependencies</h4>
# MAGIC     </td>
# MAGIC     <td style="font-size: 1em; vertical-align: middle; border-right: 4px solid #ddd; text-align: center; background-color: #98102a; color: #fff;">
# MAGIC      <h4>Parameterisation</h4>
# MAGIC     </td>
# MAGIC     <td style="font-size: 1em; vertical-align: middle; border-right: 4px solid #ddd; text-align: center; background-color: #98102a; color: #fff;">
# MAGIC      <h4>Webhooks</h4>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding-left: 0px; border-right: 4px solid #ddd;">        
# MAGIC         <p>When a task is Done, it can be in a Success, Failure, or Excluded state.</p><br>
# MAGIC             <div style="display: flex; align-items: center; gap: 140px;">
# MAGIC             <div>
# MAGIC                 <b>All Succeeded</b><br>Default behaviour 
# MAGIC             </div>
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_44_1.png" alt="Image description" style="max-width: 80px; height: auto; border-radius: 0px;">
# MAGIC             </div>
# MAGIC             <hr>
# MAGIC             <div style="display: flex; align-items: center; gap: 50px;">
# MAGIC             <div>
# MAGIC                 <b>At Least 1 Succeeded</b><br>e.g. Fan in with at least some success
# MAGIC             </div>
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_44_2.png" alt="Image description" style="max-width: 80px; height: auto; border-radius: 0px;">
# MAGIC             </div>
# MAGIC             <hr>
# MAGIC             <div style="display: flex; align-items: center; gap: 50px;">
# MAGIC             <div>
# MAGIC                 <b>None Failed</b><br>e.g. Run task(s) at the end of DAG if nothing fails
# MAGIC             </div>
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_44_3.png" alt="Image description" style="max-width: 80px; height: auto; border-radius: 0px;">
# MAGIC             </div>
# MAGIC             <hr>
# MAGIC             <div style="display: flex; align-items: center; gap: 50px;">
# MAGIC             <div>
# MAGIC                 <b>All Done</b><br>e.g. Perform clean up even if tasks have failed or excluded
# MAGIC             </div>
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_44_4.png" alt="Image description" style="max-width: 80px; height: auto; border-radius: 0px;">
# MAGIC             </div>
# MAGIC             <hr>
# MAGIC             <div style="display: flex; align-items: center; gap: 50px;">
# MAGIC             <div>
# MAGIC                 <b>All Least 1 Failed</b><br>e.g. Perform clean-up with observability or specific actions
# MAGIC             </div>
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_44_5.png" alt="Image description" style="max-width: 80px; height: auto; border-radius: 0px;">
# MAGIC             </div>
# MAGIC             <hr>
# MAGIC             <div style="display: flex; align-items: center; gap: 50px;">
# MAGIC             <div>
# MAGIC                 <b>All Failed</b><br>e.g. Perform clean-up with observability or specific actions
# MAGIC             </div>
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_44_6.png" alt="Image description" style="max-width: 80px; height: auto; border-radius: 0px;">
# MAGIC             </div>
# MAGIC     </td>
# MAGIC     <td style="padding-left: 10px; border-right: 4px solid #ddd; vertical-align: top;">   
# MAGIC         <br>     
# MAGIC         <div style="display: flex; align-items: center; gap: 20px;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_44_7.png" alt="Image description" style="max-width: 80px; height: auto; border-radius: 0px;">
# MAGIC         <div>
# MAGIC             <b>Job Parameters</b><br>
# MAGIC             Passed into each Task with behaviour based on the type<br>
# MAGIC             e.g. additional options for JARs, spark-submit, Python Args
# MAGIC         </div>
# MAGIC         </div>
# MAGIC         <br>     
# MAGIC         <br>     
# MAGIC         <br>     
# MAGIC         <div style="display: flex; align-items: center; gap: 20px;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_44_8.png" alt="Image description" style="max-width: 80px; height: auto; border-radius: 0px;">
# MAGIC         <div>
# MAGIC             <b>Dynamic Value references</b><br>
# MAGIC             Special set of templated variables that provide introspective metadata about job and task<br>
# MAGIC             e.g. run_id, job_id, start_time
# MAGIC         </div>
# MAGIC         </div>
# MAGIC         <br>     
# MAGIC         <br>     
# MAGIC         <br>     
# MAGIC         <div style="display: flex; align-items: center; gap: 20px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_44_9.png" alt="Image description" style="max-width: 80px; height: auto; border-radius: 0px;">
# MAGIC         <div>
# MAGIC             <b>Task Values</b><br>
# MAGIC             Custom parameters that can be shared between Tasks in a Job<br>
# MAGIC             e.g. anything that can be programmatically set or retrieved!
# MAGIC         </div>
# MAGIC        </div>
# MAGIC     </td>
# MAGIC     <td style="padding-left: 10px;">
# MAGIC         <br>
# MAGIC         <ul style="padding-left: 8px;">
# MAGIC             <li>Allows customers to build event-driven integrations with Databricks.</li>
# MAGIC             <div style="height:8px;"></div>
# MAGIC             <li>Supported destinations are Slack, Email, Microsoft Teams, PagerDuty, and Webhooks, with the below notification events.</li>
# MAGIC             <div style="height:8px;"></div>
# MAGIC             <li>For example, you can send a message to a Slack #channel when:</li>
# MAGIC         </ul>
# MAGIC         <div style="display: flex; align-items: center; gap: 10px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_44_10.png" alt="Image description" style="max-width: 40px; height: auto; border-radius: 0px; padding-left: 20px;">
# MAGIC             <div>
# MAGIC             <b>On start:</b> Send a message to a when a job or a parent run is started
# MAGIC             </div>
# MAGIC         </div>
# MAGIC         <br>
# MAGIC         <div style="display: flex; align-items: center; gap: 10px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_44_11.png" alt="Image description" style="max-width: 30px; height: auto; border-radius: 0px; padding-left: 20px;">
# MAGIC             <div>
# MAGIC             <b>On success:</b> When a job or a parent run finished without any errors
# MAGIC             </div>
# MAGIC         </div><br>
# MAGIC         <div style="display: flex; align-items: center; gap: 10px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_44_12.png" alt="Image description" style="max-width: 30px; height: auto; border-radius: 0px; padding-left: 20px;">
# MAGIC             <div>
# MAGIC             <b>On failure:</b> When a job fails or a parent run is terminated with one of the children in a failed state.
# MAGIC             </div>
# MAGIC         </div><br>
# MAGIC         <div style="display: flex; align-items: center; gap: 10px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_44_13.png" alt="Image description" style="max-width: 40px; height: auto; border-radius: 0px; padding-left: 20px;">
# MAGIC             <div>
# MAGIC             <b>Duration warning:</b> when a job exceeds the configured duration.
# MAGIC             </div>
# MAGIC         </div>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## H. Simple authoring for all data practitioners
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-left: 10px; border-right: 4px solid #ddd;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_45_1.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC     <td style="padding-left: 10px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_45_2.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 40px; font-size: 1em; border-right: 4px solid #ddd;">
# MAGIC         Build sophisticated workflows inside your Databricks workspace with a few clicks
# MAGIC     </td>
# MAGIC     <td style="padding: 40px; font-size: 1em;">
# MAGIC         Or connect your favorite IDE to develop workflows locally and run them on Databricks
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## I. Actionable insights from real-time monitoring 
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-left: 10px; border-right: 4px solid #ddd;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_46_1.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC     <td style="padding-left: 10px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_46_2.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 40px; font-size: 1em; border-right: 4px solid #ddd;">
# MAGIC         A simple and intuitive monitoring UI provides real-time metrics and detailed analytics for every workflow run.
# MAGIC     </td>
# MAGIC     <td style="padding: 40px; font-size: 1em;">
# MAGIC         Drill down to understand which tasks are failing and why. Troubleshoot issues before your customers are impacted.
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## J. Timeline View of task execution
# MAGIC
# MAGIC ### Improved observability for job runs
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:40%">
# MAGIC     <col style="width:60%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC       <h4>Visualize critical path for troubleshooting and optimizations</h4>
# MAGIC         <ul>
# MAGIC         <li>Easily understand non-trivial workflow runs</li>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li>Find workloads optimization opportunities</li> 
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li>Diagnose and retry failed or slow tasks</li>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 0px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_47.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## K. Seamlessly Integrated with AI 
# MAGIC
# MAGIC ### Use any foundation model such as DBRX or 3rd party external models
# MAGIC
# MAGIC <br>
# MAGIC <div style="text-align: center; margin-top: 0px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M2L2_48.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>