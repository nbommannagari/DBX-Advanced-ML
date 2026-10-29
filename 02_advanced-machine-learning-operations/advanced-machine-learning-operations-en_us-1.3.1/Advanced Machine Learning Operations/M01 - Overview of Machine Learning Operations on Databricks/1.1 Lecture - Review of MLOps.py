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
# MAGIC # Lecture - Review of MLOps
# MAGIC
# MAGIC MLOps brings the discipline of software operations to machine learning. This opening lecture reviews how Databricks supports the entire ML lifecycle — from data and training to serving and monitoring — and shows how the three operational pillars of DataOps, DevOps, and ModelOps combine to manage data, code, and models reliably at scale. It frames the development and production workflows that the rest of the course builds on.
# MAGIC
# MAGIC <br>
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this lecture, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Identify Databricks features that support general ML workloads.</li>
# MAGIC         <li>Understand the Machine Learning Lifecycle.</li>
# MAGIC         <li>Identify the Three Main Operational Components of MLOps.</li>
# MAGIC         <li>Understand DataOps for Quality Data Management.</li>
# MAGIC         <li>Understand DevOps for Model Deployment.</li>
# MAGIC         <li>Understand ModelOps for Model Lifecycle Management.</li>
# MAGIC         <li>Understand How Databricks Enables Users to Implement Best Practices for MLOps.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## A. DSML on Databricks
# MAGIC
# MAGIC <br>
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 2000 908" font-family="Helvetica, Arial, sans-serif">
# MAGIC   <!-- ===================== EDA & development (top band) ===================== -->
# MAGIC   <rect x="10" y="22" width="1980" height="146" rx="14" fill="#ececea"/>
# MAGIC   <text x="42" y="80" font-size="42" font-weight="bold" fill="#1f2d38">EDA &amp; development</text>
# MAGIC   <text x="42" y="118" font-size="21" font-style="italic" fill="#1f2d38">Fast development with AI assistance and human collaboration</text>
# MAGIC
# MAGIC   <rect x="845" y="54" width="515" height="62" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="1102" y="93" font-size="27" fill="#ffffff" text-anchor="middle">Genie Code for ML</text>
# MAGIC
# MAGIC   <rect x="1404" y="54" width="516" height="62" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="1662" y="93" font-size="27" fill="#ffffff" text-anchor="middle">Notebooks &amp; IDE integration</text>
# MAGIC
# MAGIC   <!-- ===================== Four middle columns (orange border) ===================== -->
# MAGIC   <!-- Column 1: Data -->
# MAGIC   <rect x="12" y="200" width="458" height="392" rx="12" fill="#ececea" stroke="#e5a41f" stroke-width="3"/>
# MAGIC   <text x="241" y="254" font-size="36" font-weight="bold" fill="#1f2d38" text-anchor="middle">Data</text>
# MAGIC   <text x="241" y="288" font-size="21" font-style="italic" fill="#1f2d38" text-anchor="middle">Seamless flow from data to feature engineering.</text>
# MAGIC   <text x="241" y="313" font-size="21" font-style="italic" fill="#1f2d38" text-anchor="middle">Batch, streaming, and real-time.</text>
# MAGIC   <rect x="35" y="352" width="414" height="58" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="242" y="389" font-size="27" fill="#ffffff" text-anchor="middle">Lakeflow</text>
# MAGIC   <rect x="35" y="436" width="414" height="58" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="242" y="473" font-size="27" fill="#ffffff" text-anchor="middle">Feature Store</text>
# MAGIC
# MAGIC   <!-- Column 2: Training -->
# MAGIC   <rect x="510" y="200" width="472" height="392" rx="12" fill="#ececea" stroke="#e5a41f" stroke-width="3"/>
# MAGIC   <text x="746" y="254" font-size="36" font-weight="bold" fill="#1f2d38" text-anchor="middle">Training</text>
# MAGIC   <text x="746" y="288" font-size="21" font-style="italic" fill="#1f2d38" text-anchor="middle">Flexible environments with CPU &amp; GPU compute</text>
# MAGIC   <text x="746" y="313" font-size="21" font-style="italic" fill="#1f2d38" text-anchor="middle">for training &amp; tuning</text>
# MAGIC   <rect x="535" y="352" width="422" height="58" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="746" y="389" font-size="27" fill="#ffffff" text-anchor="middle">AI Runtime (serverless)</text>
# MAGIC   <rect x="535" y="436" width="422" height="58" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="746" y="473" font-size="27" fill="#ffffff" text-anchor="middle">ML Runtime (classic compute)</text>
# MAGIC
# MAGIC   <!-- Column 3: Serving -->
# MAGIC   <rect x="1022" y="200" width="458" height="392" rx="12" fill="#ececea" stroke="#e5a41f" stroke-width="3"/>
# MAGIC   <text x="1251" y="254" font-size="36" font-weight="bold" fill="#1f2d38" text-anchor="middle">Serving</text>
# MAGIC   <text x="1251" y="288" font-size="21" font-style="italic" fill="#1f2d38" text-anchor="middle">Production inference for batch and real-time</text>
# MAGIC   <text x="1251" y="313" font-size="21" font-style="italic" fill="#1f2d38" text-anchor="middle">deployment</text>
# MAGIC   <rect x="1045" y="332" width="414" height="56" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="1252" y="368" font-size="27" fill="#ffffff" text-anchor="middle">Batch inference</text>
# MAGIC   <rect x="1045" y="418" width="414" height="56" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="1252" y="454" font-size="27" fill="#ffffff" text-anchor="middle">Model Serving</text>
# MAGIC   <rect x="1045" y="504" width="414" height="56" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="1252" y="540" font-size="27" fill="#ffffff" text-anchor="middle">Feature Serving</text>
# MAGIC
# MAGIC   <!-- Column 4: Monitoring -->
# MAGIC   <rect x="1522" y="200" width="468" height="392" rx="12" fill="#ececea" stroke="#e5a41f" stroke-width="3"/>
# MAGIC   <text x="1756" y="254" font-size="36" font-weight="bold" fill="#1f2d38" text-anchor="middle">Monitoring</text>
# MAGIC   <text x="1756" y="288" font-size="21" font-style="italic" fill="#1f2d38" text-anchor="middle">Production monitoring with dashboards and</text>
# MAGIC   <text x="1756" y="313" font-size="21" font-style="italic" fill="#1f2d38" text-anchor="middle">alerts</text>
# MAGIC   <rect x="1545" y="352" width="422" height="58" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="1756" y="389" font-size="27" fill="#ffffff" text-anchor="middle">Inference tables</text>
# MAGIC   <rect x="1545" y="436" width="422" height="58" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="1756" y="473" font-size="27" fill="#ffffff" text-anchor="middle">Data quality monitoring for ML</text>
# MAGIC
# MAGIC   <!-- ===================== MLOps band (blue border) ===================== -->
# MAGIC   <rect x="10" y="628" width="1980" height="118" rx="10" fill="#ececea" stroke="#6f9fcf" stroke-width="3"/>
# MAGIC   <text x="42" y="690" font-size="42" font-weight="bold" fill="#1f2d38">MLOps</text>
# MAGIC   <text x="42" y="725" font-size="21" font-style="italic" fill="#1f2d38">Tracking and infrastructure as code for the full ML lifecycle</text>
# MAGIC
# MAGIC   <rect x="845" y="652" width="515" height="70" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="1102" y="682" font-size="27" fill="#ffffff" text-anchor="middle">Declarative Automation Bundles (DABs)</text>
# MAGIC   <text x="1102" y="710" font-size="27" fill="#ffffff" text-anchor="middle">and MLOps Stacks</text>
# MAGIC
# MAGIC   <rect x="1404" y="652" width="516" height="70" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="1662" y="695" font-size="27" fill="#ffffff" text-anchor="middle">MLflow Tracking &amp; Registry</text>
# MAGIC
# MAGIC   <!-- ===================== Governance and storage band (blue border) ===================== -->
# MAGIC   <rect x="10" y="762" width="1980" height="126" rx="10" fill="#ececea" stroke="#6f9fcf" stroke-width="3"/>
# MAGIC   <text x="42" y="828" font-size="42" font-weight="bold" fill="#1f2d38">Governance and storage</text>
# MAGIC   <text x="42" y="864" font-size="21" font-style="italic" fill="#1f2d38">Unified data and ML platform with open standards and formats</text>
# MAGIC
# MAGIC   <rect x="845" y="800" width="515" height="62" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="1102" y="839" font-size="27" fill="#ffffff" text-anchor="middle">Unity Catalog</text>
# MAGIC
# MAGIC   <rect x="1404" y="800" width="516" height="62" rx="6" fill="#2d4a5a"/>
# MAGIC   <text x="1662" y="839" font-size="27" fill="#ffffff" text-anchor="middle">Delta Lake</text>
# MAGIC </svg>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## B. Data + AI 2026 Announcements
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td>
# MAGIC         <div style="text-align: left; margin-top: 0px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M1L1_9_1.png" alt="Image description" style="max-width: 80%; height: auto; border-radius: 50px;">
# MAGIC         </div>
# MAGIC         <br>
# MAGIC         <div style="padding-left: 20px; font-size: 1.25em;">
# MAGIC             <b>Serverless GPU compute for enterprise deep learning</b>
# MAGIC             <ul>
# MAGIC                 <li>Powerful GPUs, on-demand</li>
# MAGIC                 <li>Research-grade, enterprise-scale</li>
# MAGIC                 <li>[NEW] Multi-node training</li>
# MAGIC             </ul>
# MAGIC         </div>
# MAGIC         <br><br>
# MAGIC         <div style="text-align: left; margin-top: 0px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M1L1_9_3.png" alt="Image description" style="max-width: 80%; height: auto; border-radius: 50px;">
# MAGIC         </div>
# MAGIC         <br>
# MAGIC         <div style="padding-left: 20px; font-size: 1.25em;">
# MAGIC             <b>An agent that runs your ML systems</b>
# MAGIC             <ul>
# MAGIC                 <li>Watches every production model, continuously</li>
# MAGIC                 <li>Investigates and resolves issues, autonomously</li>
# MAGIC                 <li>Brings you deployment-ready fixes to approve</li>
# MAGIC             </ul>
# MAGIC         </div>
# MAGIC    </td>
# MAGIC     <td style="vertical-align: middle;">        
# MAGIC         <div style="text-align: left; margin-top: 0px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M1L1_9_2.png" alt="Image description" style="max-width: 80%; height: auto; border-radius: 50px;">
# MAGIC         </div>
# MAGIC         <br>
# MAGIC         <div style="padding-left: 20px; font-size: 1.25em;">
# MAGIC             <b>An agent for production ML engineering</b>
# MAGIC             <ul>
# MAGIC                 <li>Upgraded intelligence for ML engineering</li>
# MAGIC                 <li>Native to UC and every Databricks ML product</li>
# MAGIC                 <li>Powered by Genie Ontology — your context & workflows built-in</li>
# MAGIC             </ul>
# MAGIC         </div>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## C. Operational Excellence in Machine Learning
# MAGIC
# MAGIC ### Enhancing Efficiency and Reliability in Machine Learning Operations
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:40%">
# MAGIC     <col style="width:60%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-left: 0px; font-size: 1.15em;">
# MAGIC         <b>Standardize MLOps</b> processes for consistent and reliable ML.
# MAGIC         <ul>
# MAGIC             <li>Implement <b>DevOps processes</b> (CI/CD) by automating building, testing, and deploying code.</li>
# MAGIC             <li><b>Modularize code</b> for clearly defined steps, testing and simplify refactoring.</li>
# MAGIC             <li>Encapsulate individual pipelines and workflows into an <b>orchestrator</b>.</li>
# MAGIC             <li>Incorporate <b>versioning</b> for tracking models and datasets.</li>
# MAGIC             <li>Set up <b>monitoring, alerting, </b>and <b>logging</b>.</li>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 0px; font-size: 1.15em;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M1L1_10.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## D. Machine Learning Operations
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-left: 0px; font-size: 1.15em;">
# MAGIC         <b>What is MLOps?</b>
# MAGIC         <ul>
# MAGIC             <li>A combination of <b>DevOps, DataOps,</b>  and <b>ModelOps</b>.</li>
# MAGIC             <li>A set of <b>processes</b> and <b>automation</b> for managing: <b>models, data,</b> and <b>code</b> assets.</li>
# MAGIC         </ul>
# MAGIC         <b>Results of MLOps implementation</b>
# MAGIC         <ul>
# MAGIC             <li><b>Efficiency</b>: Higher quality models in less time.</li>
# MAGIC             <li><b>Scalability</b>: Thousands of models can be overseen, controlled, managed, and monitored.</li>
# MAGIC             <li><b>Risk reduction</b>: Through repeatable workflows, automated monitoring and transparency.</li>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 0px; font-size: 1.15em;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M1L1_11.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## E. DataOps, DevOps, and ModelOps are…
# MAGIC
# MAGIC ### A set of practices, processes, and technologies to: 
# MAGIC
# MAGIC <div style="display:flex;gap:0;margin:24px 0;align-items:stretch;">
# MAGIC
# MAGIC   <div style="flex:1;background:#FFF3E0;border-top:4px solid #F57C00;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#E65100;font-size:18px;margin-bottom:14px;">DataOps</div>
# MAGIC     <u>Ensure data <b>quality</b></u>
# MAGIC     <div style="height:8px;"></div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC         <li>Optimize data <b>processing</b></li>
# MAGIC         <li>Centralize data <b>discovery</b>, <b>management</b>, and <b>governance</b></li>
# MAGIC         <li>Establish traceable data <b>lineage</b> and <b>monitoring</b></li>
# MAGIC         <li><b>Enhance collaboration</b> across teams</li>
# MAGIC         <li><b>Monitor</b> data</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;background:#E3F2FD;border-top:4px solid #1976D2;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#0D47A1;font-size:18px;margin-bottom:14px;">DevOps</div>
# MAGIC     <u>Treat machine learning as <b>code</b></u>
# MAGIC     <div style="height:8px;"></div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC         <li>Automate <b>CI/CD</b></li>
# MAGIC         <li>Enable continuous <b>code testing</b></li>
# MAGIC         <li><b>Version control</b></li>
# MAGIC         <li>Establish production-grade <b>workflows</b></li>
# MAGIC             <ul style="font-size:14px;"><li>Orchestration & automation</li></ul>
# MAGIC         <li><b>Monitor</b> system performance</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex:1;background:#E8F5E9;border-top:4px solid #388E3C;border-radius:0px 0 0 0px;padding:24px;">
# MAGIC     <div style="font-weight:bold;color:#1B5E20;font-size:18px;margin-bottom:14px;">ModelOps</div>
# MAGIC     <u>Move beyond models as <b>objects</b></u>
# MAGIC     <div style="height:8px;"></div>
# MAGIC     <ul style="font-size:16px;color:#555;line-height:2;margin:0;padding-left:20px;">
# MAGIC         <li>Treating <b>model code</b> as <b>software</b></li>
# MAGIC         <li>Treating models as <b>data</b></li>
# MAGIC         <li>Manage the <b>model lifecycle</b></li>
# MAGIC         <li><b>Monitor</b> model performance</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## F. MLOps in the Machine Learning Lifecycle
# MAGIC
# MAGIC ### From Data to Deployment with Machine Learning Libraries

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC #### F1. The Development Workflow
# MAGIC
# MAGIC <br>
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 2000 776" font-family="Helvetica, Arial, sans-serif">
# MAGIC   <defs>
# MAGIC     <marker id="arrowNavy13" markerWidth="10" markerHeight="10" refX="7" refY="5" orient="auto" markerUnits="userSpaceOnUse">
# MAGIC       <path d="M0,0 L9,5 L0,10 Z" fill="#1e3a4a"/>
# MAGIC     </marker>
# MAGIC   </defs>
# MAGIC
# MAGIC   <!-- ===================== Top lifecycle row ===================== -->
# MAGIC   <!-- 1 Business Problem -->
# MAGIC   <rect x="10" y="18" width="202" height="167" rx="12" fill="#efeeea" stroke="#1e3a4a" stroke-width="3"/>
# MAGIC   <text x="111" y="94" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Business</text>
# MAGIC   <text x="111" y="128" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Problem</text>
# MAGIC
# MAGIC   <!-- 2 Define Success Criteria -->
# MAGIC   <rect x="234" y="18" width="202" height="167" rx="12" fill="#efeeea" stroke="#1e3a4a" stroke-width="3"/>
# MAGIC   <text x="335" y="77" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Define</text>
# MAGIC   <text x="335" y="111" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Success</text>
# MAGIC   <text x="335" y="145" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Criteria</text>
# MAGIC
# MAGIC   <!-- 3 Data Collection -->
# MAGIC   <rect x="474" y="18" width="216" height="167" rx="12" fill="#efeeea" stroke="#1e3a4a" stroke-width="3"/>
# MAGIC   <text x="582" y="94" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Data</text>
# MAGIC   <text x="582" y="128" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Collection</text>
# MAGIC
# MAGIC   <!-- 4 Data Preprocessing / Feature Engineering (dark navy) -->
# MAGIC   <rect x="712" y="18" width="256" height="167" rx="12" fill="#22333f"/>
# MAGIC   <text x="840" y="64" font-size="27" font-weight="bold" fill="#ffffff" text-anchor="middle">Data</text>
# MAGIC   <text x="840" y="98" font-size="27" font-weight="bold" fill="#ffffff" text-anchor="middle">Preprocessing/</text>
# MAGIC   <text x="840" y="132" font-size="27" font-weight="bold" fill="#ffffff" text-anchor="middle">Feature</text>
# MAGIC   <text x="840" y="166" font-size="27" font-weight="bold" fill="#ffffff" text-anchor="middle">Engineering</text>
# MAGIC
# MAGIC   <!-- 5 Model Training & Tuning (green) -->
# MAGIC   <rect x="986" y="18" width="254" height="167" rx="12" fill="#33571f"/>
# MAGIC   <text x="1113" y="94" font-size="27" font-weight="bold" fill="#ffffff" text-anchor="middle">Model Training &amp;</text>
# MAGIC   <text x="1113" y="128" font-size="27" font-weight="bold" fill="#ffffff" text-anchor="middle">Tuning</text>
# MAGIC
# MAGIC   <!-- 6 Model Evaluation (green) -->
# MAGIC   <rect x="1258" y="18" width="238" height="167" rx="12" fill="#33571f"/>
# MAGIC   <text x="1377" y="94" font-size="27" font-weight="bold" fill="#ffffff" text-anchor="middle">Model</text>
# MAGIC   <text x="1377" y="128" font-size="27" font-weight="bold" fill="#ffffff" text-anchor="middle">Evaluation</text>
# MAGIC
# MAGIC   <!-- 7 Model Deployment -->
# MAGIC   <rect x="1514" y="18" width="238" height="167" rx="12" fill="#efeeea" stroke="#1e3a4a" stroke-width="3"/>
# MAGIC   <text x="1633" y="94" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Model</text>
# MAGIC   <text x="1633" y="128" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Deployment</text>
# MAGIC
# MAGIC   <!-- 8 Model Monitoring -->
# MAGIC   <rect x="1772" y="18" width="226" height="167" rx="12" fill="#efeeea" stroke="#1e3a4a" stroke-width="3"/>
# MAGIC   <text x="1885" y="94" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Model</text>
# MAGIC   <text x="1885" y="128" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Monitoring</text>
# MAGIC
# MAGIC   <!-- Forward arrows between stages -->
# MAGIC   <line x1="212" y1="101" x2="232" y2="101" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy13)"/>
# MAGIC   <line x1="436" y1="101" x2="472" y2="101" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy13)"/>
# MAGIC   <line x1="690" y1="101" x2="710" y2="101" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy13)"/>
# MAGIC   <line x1="968" y1="101" x2="984" y2="101" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy13)"/>
# MAGIC   <line x1="1240" y1="101" x2="1256" y2="101" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy13)"/>
# MAGIC   <line x1="1496" y1="101" x2="1512" y2="101" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy13)"/>
# MAGIC   <line x1="1752" y1="101" x2="1770" y2="101" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy13)"/>
# MAGIC
# MAGIC   <!-- Curved feedback arrows returning to preprocessing stage -->
# MAGIC   <!-- Model Evaluation -> Data Preprocessing/Feature Engineering -->
# MAGIC   <path d="M1377,186 C 1377,232 1080,240 843,187" fill="none" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy13)"/>
# MAGIC   <!-- Model Evaluation -> Data Collection -->
# MAGIC   <path d="M1377,186 C 1377,262 900,268 560,187" fill="none" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy13)"/>
# MAGIC   <!-- Model Monitoring -> Data Collection -->
# MAGIC   <path d="M1885,186 C 1885,296 950,302 605,187" fill="none" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy13)"/>
# MAGIC
# MAGIC   <!-- ===================== Left column (navy chevrons) ===================== -->
# MAGIC   <!-- Row template: gray band, then navy chevron, then label + value -->
# MAGIC   <!-- r1 -->
# MAGIC   <rect x="75" y="280" width="860" height="64" fill="#d3d3d1"/>
# MAGIC   <path d="M75,280 L415,280 L445,312 L415,344 L75,344 Z" fill="#26333f"/>
# MAGIC   <text x="95" y="319" font-size="20" fill="#ffffff">Ingest &amp; transform data</text>
# MAGIC   <text x="690" y="321" font-size="25" font-weight="bold" fill="#1e2e3a" text-anchor="middle">Auto Loader and Apache Spark</text>
# MAGIC   <!-- r2 -->
# MAGIC   <rect x="75" y="350" width="860" height="64" fill="#d3d3d1"/>
# MAGIC   <path d="M75,350 L415,350 L445,382 L415,414 L75,414 Z" fill="#26333f"/>
# MAGIC   <text x="95" y="377" font-size="20" fill="#ffffff">Track data changes to including</text>
# MAGIC   <text x="95" y="401" font-size="20" fill="#ffffff">versioning &amp; lineage</text>
# MAGIC   <text x="690" y="379" font-size="25" font-weight="bold" fill="#1e2e3a" text-anchor="middle">UC + supported Delta/Iceberg</text>
# MAGIC   <text x="690" y="407" font-size="25" font-weight="bold" fill="#1e2e3a" text-anchor="middle">Tables</text>
# MAGIC   <!-- r3 -->
# MAGIC   <rect x="75" y="420" width="860" height="64" fill="#d3d3d1"/>
# MAGIC   <path d="M75,420 L415,420 L445,452 L415,484 L75,484 Z" fill="#26333f"/>
# MAGIC   <text x="95" y="447" font-size="20" fill="#ffffff">Build, manage, &amp; monitor</text>
# MAGIC   <text x="95" y="471" font-size="20" fill="#ffffff">data processing pipelines</text>
# MAGIC   <text x="690" y="461" font-size="25" font-weight="bold" fill="#1e2e3a" text-anchor="middle">Lakeflow pipelines</text>
# MAGIC   <!-- r4 -->
# MAGIC   <rect x="75" y="490" width="860" height="64" fill="#d3d3d1"/>
# MAGIC   <path d="M75,490 L415,490 L445,522 L415,554 L75,554 Z" fill="#26333f"/>
# MAGIC   <text x="95" y="517" font-size="20" fill="#ffffff">Ensure data security &amp;</text>
# MAGIC   <text x="95" y="541" font-size="20" fill="#ffffff">governance</text>
# MAGIC   <text x="690" y="531" font-size="25" font-weight="bold" fill="#1e2e3a" text-anchor="middle">Unity Catalog</text>
# MAGIC   <!-- r5 -->
# MAGIC   <rect x="75" y="560" width="860" height="64" fill="#d3d3d1"/>
# MAGIC   <path d="M75,560 L415,560 L445,592 L415,624 L75,624 Z" fill="#26333f"/>
# MAGIC   <text x="95" y="587" font-size="20" fill="#ffffff">Exploratory data analysis,</text>
# MAGIC   <text x="95" y="611" font-size="20" fill="#ffffff">dashboards, &amp; general coding</text>
# MAGIC   <text x="690" y="587" font-size="25" font-weight="bold" fill="#1e2e3a" text-anchor="middle">Databricks SQL, AI/BI dashboards,</text>
# MAGIC   <text x="690" y="611" font-size="25" font-weight="bold" fill="#1e2e3a" text-anchor="middle">and notebooks</text>
# MAGIC   <!-- r6 -->
# MAGIC   <rect x="75" y="630" width="860" height="64" fill="#d3d3d1"/>
# MAGIC   <path d="M75,630 L415,630 L445,662 L415,694 L75,694 Z" fill="#26333f"/>
# MAGIC   <text x="95" y="657" font-size="20" fill="#ffffff">Schedule data pipelines &amp;</text>
# MAGIC   <text x="95" y="681" font-size="20" fill="#ffffff">Automate general workflows</text>
# MAGIC   <text x="690" y="671" font-size="25" font-weight="bold" fill="#1e2e3a" text-anchor="middle">Lakeflow Jobs</text>
# MAGIC   <!-- r7 -->
# MAGIC   <rect x="75" y="700" width="860" height="64" fill="#d3d3d1"/>
# MAGIC   <path d="M75,700 L415,700 L445,732 L415,764 L75,764 Z" fill="#26333f"/>
# MAGIC   <text x="95" y="727" font-size="20" fill="#ffffff">Create, store, manage, &amp;</text>
# MAGIC   <text x="95" y="751" font-size="20" fill="#ffffff">discover features</text>
# MAGIC   <text x="690" y="741" font-size="25" font-weight="bold" fill="#1e2e3a" text-anchor="middle">Databricks Feature Store</text>
# MAGIC
# MAGIC   <!-- ===================== Right column (green chevrons) ===================== -->
# MAGIC   <!-- r1 -->
# MAGIC   <rect x="1060" y="280" width="870" height="56" fill="#d3d3d1"/>
# MAGIC   <path d="M1060,280 L1400,280 L1430,308 L1400,336 L1060,336 Z" fill="#33571f"/>
# MAGIC   <text x="1080" y="315" font-size="20" fill="#ffffff">No-code model development</text>
# MAGIC   <text x="1680" y="318" font-size="25" font-weight="bold" fill="#33571f" text-anchor="middle">Databricks AutoML</text>
# MAGIC   <!-- r2 -->
# MAGIC   <rect x="1060" y="341" width="870" height="56" fill="#d3d3d1"/>
# MAGIC   <path d="M1060,341 L1400,341 L1430,369 L1400,397 L1060,397 Z" fill="#33571f"/>
# MAGIC   <text x="1080" y="376" font-size="20" fill="#ffffff">Track model development</text>
# MAGIC   <text x="1680" y="379" font-size="25" font-weight="bold" fill="#33571f" text-anchor="middle">MLflow Tracking</text>
# MAGIC   <!-- r3 -->
# MAGIC   <rect x="1060" y="402" width="870" height="56" fill="#d3d3d1"/>
# MAGIC   <path d="M1060,402 L1400,402 L1430,430 L1400,458 L1060,458 Z" fill="#33571f"/>
# MAGIC   <text x="1080" y="437" font-size="20" fill="#ffffff">General coding</text>
# MAGIC   <text x="1680" y="426" font-size="25" font-weight="bold" fill="#33571f" text-anchor="middle">Databricks Notebooks &amp; Databricks</text>
# MAGIC   <text x="1680" y="454" font-size="25" font-weight="bold" fill="#33571f" text-anchor="middle">Runtime</text>
# MAGIC   <!-- r4 -->
# MAGIC   <rect x="1060" y="463" width="870" height="56" fill="#d3d3d1"/>
# MAGIC   <path d="M1060,463 L1400,463 L1430,491 L1400,519 L1060,519 Z" fill="#33571f"/>
# MAGIC   <text x="1080" y="491" font-size="20" fill="#ffffff">Hyperparameter Tuning and</text>
# MAGIC   <text x="1080" y="513" font-size="20" fill="#ffffff">Model Choosing</text>
# MAGIC   <text x="1680" y="499" font-size="25" font-weight="bold" fill="#33571f" text-anchor="middle">Scikit-Learn, Optuna, and Ray</text>
# MAGIC   <!-- r5 -->
# MAGIC   <rect x="1060" y="524" width="870" height="56" fill="#d3d3d1"/>
# MAGIC   <path d="M1060,524 L1400,524 L1430,552 L1400,580 L1060,580 Z" fill="#33571f"/>
# MAGIC   <text x="1080" y="552" font-size="20" fill="#ffffff">Model code version control and</text>
# MAGIC   <text x="1080" y="574" font-size="20" fill="#ffffff">sharing</text>
# MAGIC   <text x="1680" y="547" font-size="25" font-weight="bold" fill="#33571f" text-anchor="middle">Declarative Automation Bundles and</text>
# MAGIC   <text x="1680" y="575" font-size="25" font-weight="bold" fill="#33571f" text-anchor="middle">Databricks Git Folders</text>
# MAGIC   <!-- r6 -->
# MAGIC   <rect x="1060" y="585" width="870" height="56" fill="#d3d3d1"/>
# MAGIC   <path d="M1060,585 L1400,585 L1430,613 L1400,641 L1060,641 Z" fill="#33571f"/>
# MAGIC   <text x="1080" y="620" font-size="20" fill="#ffffff">Manage model lifecycle</text>
# MAGIC   <text x="1680" y="623" font-size="25" font-weight="bold" fill="#33571f" text-anchor="middle">Models in Unity Catalog</text>
# MAGIC   <!-- r7 -->
# MAGIC   <rect x="1060" y="646" width="870" height="56" fill="#d3d3d1"/>
# MAGIC   <path d="M1060,646 L1400,646 L1430,674 L1400,702 L1060,702 Z" fill="#33571f"/>
# MAGIC   <text x="1080" y="674" font-size="20" fill="#ffffff">Schedule &amp; Automate</text>
# MAGIC   <text x="1080" y="696" font-size="20" fill="#ffffff">machine learning workflows</text>
# MAGIC   <text x="1680" y="684" font-size="25" font-weight="bold" fill="#33571f" text-anchor="middle">Lakeflow Jobs</text>
# MAGIC   <!-- r8 -->
# MAGIC   <rect x="1060" y="707" width="870" height="56" fill="#d3d3d1"/>
# MAGIC   <path d="M1060,707 L1400,707 L1430,735 L1400,763 L1060,763 Z" fill="#33571f"/>
# MAGIC   <text x="1080" y="742" font-size="20" fill="#ffffff">Model monitoring</text>
# MAGIC   <text x="1680" y="731" font-size="25" font-weight="bold" fill="#33571f" text-anchor="middle">Data Quality Monitoring and MLflow</text>
# MAGIC   <text x="1680" y="759" font-size="25" font-weight="bold" fill="#33571f" text-anchor="middle">Evaluation</text>
# MAGIC </svg>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC #### F2. The Production Workflow
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 2000 786" font-family="Helvetica, Arial, sans-serif">
# MAGIC   <defs>
# MAGIC     <marker id="arrowNavy14" markerWidth="10" markerHeight="10" refX="7" refY="5" orient="auto" markerUnits="userSpaceOnUse">
# MAGIC       <path d="M0,0 L9,5 L0,10 Z" fill="#1e3a4a"/>
# MAGIC     </marker>
# MAGIC   </defs>
# MAGIC
# MAGIC   <!-- ===================== Top lifecycle row ===================== -->
# MAGIC   <!-- 1 Business Problem -->
# MAGIC   <rect x="10" y="18" width="202" height="167" rx="12" fill="#efeeea" stroke="#1e3a4a" stroke-width="3"/>
# MAGIC   <text x="111" y="94" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Business</text>
# MAGIC   <text x="111" y="128" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Problem</text>
# MAGIC
# MAGIC   <!-- 2 Define Success Criteria -->
# MAGIC   <rect x="234" y="18" width="202" height="167" rx="12" fill="#efeeea" stroke="#1e3a4a" stroke-width="3"/>
# MAGIC   <text x="335" y="77" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Define</text>
# MAGIC   <text x="335" y="111" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Success</text>
# MAGIC   <text x="335" y="145" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Criteria</text>
# MAGIC
# MAGIC   <!-- 3 Data Collection -->
# MAGIC   <rect x="474" y="18" width="216" height="167" rx="12" fill="#efeeea" stroke="#1e3a4a" stroke-width="3"/>
# MAGIC   <text x="582" y="94" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Data</text>
# MAGIC   <text x="582" y="128" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Collection</text>
# MAGIC
# MAGIC   <!-- 4 Data Preprocessing / Feature Engineering -->
# MAGIC   <rect x="712" y="18" width="256" height="167" rx="12" fill="#efeeea" stroke="#1e3a4a" stroke-width="3"/>
# MAGIC   <text x="840" y="64" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Data</text>
# MAGIC   <text x="840" y="98" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Preprocessing/</text>
# MAGIC   <text x="840" y="132" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Feature</text>
# MAGIC   <text x="840" y="166" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Engineering</text>
# MAGIC
# MAGIC   <!-- 5 Model Training & Tuning -->
# MAGIC   <rect x="986" y="18" width="254" height="167" rx="12" fill="#efeeea" stroke="#1e3a4a" stroke-width="3"/>
# MAGIC   <text x="1113" y="94" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Model Training &amp;</text>
# MAGIC   <text x="1113" y="128" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Tuning</text>
# MAGIC
# MAGIC   <!-- 6 Model Evaluation -->
# MAGIC   <rect x="1258" y="18" width="238" height="167" rx="12" fill="#efeeea" stroke="#1e3a4a" stroke-width="3"/>
# MAGIC   <text x="1377" y="94" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Model</text>
# MAGIC   <text x="1377" y="128" font-size="27" font-weight="bold" fill="#1e3a4a" text-anchor="middle">Evaluation</text>
# MAGIC
# MAGIC   <!-- 7 Model Deployment (coral, highlighted) -->
# MAGIC   <rect x="1514" y="18" width="238" height="167" rx="12" fill="#e8604c"/>
# MAGIC   <text x="1633" y="94" font-size="27" font-weight="bold" fill="#ffffff" text-anchor="middle">Model</text>
# MAGIC   <text x="1633" y="128" font-size="27" font-weight="bold" fill="#ffffff" text-anchor="middle">Deployment</text>
# MAGIC
# MAGIC   <!-- 8 Model Monitoring (blue-gray, highlighted) -->
# MAGIC   <rect x="1772" y="18" width="226" height="167" rx="12" fill="#5c7a89"/>
# MAGIC   <text x="1885" y="94" font-size="27" font-weight="bold" fill="#ffffff" text-anchor="middle">Model</text>
# MAGIC   <text x="1885" y="128" font-size="27" font-weight="bold" fill="#ffffff" text-anchor="middle">Monitoring</text>
# MAGIC
# MAGIC   <!-- Forward arrows between stages -->
# MAGIC   <line x1="212" y1="101" x2="232" y2="101" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy14)"/>
# MAGIC   <line x1="436" y1="101" x2="472" y2="101" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy14)"/>
# MAGIC   <line x1="690" y1="101" x2="710" y2="101" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy14)"/>
# MAGIC   <line x1="968" y1="101" x2="984" y2="101" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy14)"/>
# MAGIC   <line x1="1240" y1="101" x2="1256" y2="101" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy14)"/>
# MAGIC   <line x1="1496" y1="101" x2="1512" y2="101" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy14)"/>
# MAGIC   <line x1="1752" y1="101" x2="1770" y2="101" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy14)"/>
# MAGIC
# MAGIC   <!-- Feedback arrows -->
# MAGIC   <!-- Model Evaluation -> Data Preprocessing/Feature Engineering -->
# MAGIC   <path d="M1377,186 C 1377,232 1080,240 843,187" fill="none" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy14)"/>
# MAGIC   <!-- Model Evaluation -> Data Collection -->
# MAGIC   <path d="M1377,186 C 1377,262 900,268 560,187" fill="none" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy14)"/>
# MAGIC   <!-- Model Monitoring -> Data Collection -->
# MAGIC   <path d="M1885,186 C 1885,296 950,302 605,187" fill="none" stroke="#1e3a4a" stroke-width="3" marker-end="url(#arrowNavy14)"/>
# MAGIC
# MAGIC   <!-- ===================== Left column (coral chevrons) ===================== -->
# MAGIC   <!-- r1 -->
# MAGIC   <rect x="85" y="285" width="855" height="72" fill="#d3d3d1"/>
# MAGIC   <path d="M85,285 L400,285 L435,321 L400,357 L85,357 Z" fill="#e8604c"/>
# MAGIC   <text x="105" y="329" font-size="22" fill="#ffffff">Manage model lifecycle</text>
# MAGIC   <text x="687" y="331" font-size="26" font-weight="bold" fill="#e8604c" text-anchor="middle">Models in Unity Catalog</text>
# MAGIC   <!-- r2 -->
# MAGIC   <rect x="85" y="365" width="855" height="72" fill="#d3d3d1"/>
# MAGIC   <path d="M85,365 L400,365 L435,401 L400,437 L85,437 Z" fill="#e8604c"/>
# MAGIC   <text x="105" y="395" font-size="22" fill="#ffffff">Maintain a highly available low</text>
# MAGIC   <text x="105" y="421" font-size="22" fill="#ffffff">latency REST endpoint</text>
# MAGIC   <text x="687" y="411" font-size="26" font-weight="bold" fill="#e8604c" text-anchor="middle">Model Serving</text>
# MAGIC   <!-- r3 -->
# MAGIC   <rect x="85" y="445" width="855" height="72" fill="#d3d3d1"/>
# MAGIC   <path d="M85,445 L400,445 L435,481 L400,517 L85,517 Z" fill="#e8604c"/>
# MAGIC   <text x="105" y="475" font-size="22" fill="#ffffff">Automate and schedule</text>
# MAGIC   <text x="105" y="501" font-size="22" fill="#ffffff">workloads, from: ETL to ML</text>
# MAGIC   <text x="687" y="475" font-size="26" font-weight="bold" fill="#e8604c" text-anchor="middle">Lakeflow Jobs and Lakeflow</text>
# MAGIC   <text x="687" y="503" font-size="26" font-weight="bold" fill="#e8604c" text-anchor="middle">Pipelines</text>
# MAGIC   <!-- r4 -->
# MAGIC   <rect x="85" y="525" width="855" height="72" fill="#d3d3d1"/>
# MAGIC   <path d="M85,525 L400,525 L435,561 L400,597 L85,597 Z" fill="#e8604c"/>
# MAGIC   <text x="105" y="569" font-size="22" fill="#ffffff">Establish CI/CD Pipelines</text>
# MAGIC   <text x="687" y="555" font-size="26" font-weight="bold" fill="#e8604c" text-anchor="middle">Declarative Automation Bundles,</text>
# MAGIC   <text x="687" y="583" font-size="26" font-weight="bold" fill="#e8604c" text-anchor="middle">Azure DevOps, etc.</text>
# MAGIC   <!-- r5 -->
# MAGIC   <rect x="85" y="605" width="855" height="72" fill="#d3d3d1"/>
# MAGIC   <path d="M85,605 L400,605 L435,641 L400,677 L85,677 Z" fill="#e8604c"/>
# MAGIC   <text x="105" y="649" font-size="22" fill="#ffffff">Serve features and vectors</text>
# MAGIC   <text x="687" y="635" font-size="26" font-weight="bold" fill="#e8604c" text-anchor="middle">Databricks Feature Store and AI</text>
# MAGIC   <text x="687" y="663" font-size="26" font-weight="bold" fill="#e8604c" text-anchor="middle">Search</text>
# MAGIC   <!-- r6 -->
# MAGIC   <rect x="85" y="685" width="855" height="72" fill="#d3d3d1"/>
# MAGIC   <path d="M85,685 L400,685 L435,721 L400,757 L85,757 Z" fill="#e8604c"/>
# MAGIC   <text x="105" y="729" font-size="22" fill="#ffffff">Flexible Model Serving</text>
# MAGIC   <text x="687" y="731" font-size="26" font-weight="bold" fill="#e8604c" text-anchor="middle">MLflow and Models in UC</text>
# MAGIC
# MAGIC   <!-- ===================== Right column (blue-gray chevrons) ===================== -->
# MAGIC   <!-- r1 -->
# MAGIC   <rect x="1055" y="285" width="860" height="72" fill="#d3d3d1"/>
# MAGIC   <path d="M1055,285 L1375,285 L1410,321 L1375,357 L1055,357 Z" fill="#5c7a89"/>
# MAGIC   <text x="1075" y="315" font-size="22" fill="#ffffff">Monitoring, and Maintaining your</text>
# MAGIC   <text x="1075" y="341" font-size="22" fill="#ffffff">Applications</text>
# MAGIC   <text x="1662" y="331" font-size="26" font-weight="bold" fill="#5c7a89" text-anchor="middle">Data quality monitoring</text>
# MAGIC   <!-- r2 -->
# MAGIC   <rect x="1055" y="365" width="860" height="72" fill="#d3d3d1"/>
# MAGIC   <path d="M1055,365 L1375,365 L1410,401 L1375,437 L1055,437 Z" fill="#5c7a89"/>
# MAGIC   <text x="1075" y="409" font-size="22" fill="#ffffff">Notifications &amp; Alerts</text>
# MAGIC   <text x="1662" y="411" font-size="26" font-weight="bold" fill="#5c7a89" text-anchor="middle">Databricks SQL alerts</text>
# MAGIC   <!-- r3 -->
# MAGIC   <rect x="1055" y="445" width="860" height="72" fill="#d3d3d1"/>
# MAGIC   <path d="M1055,445 L1375,445 L1410,481 L1375,517 L1055,517 Z" fill="#5c7a89"/>
# MAGIC   <text x="1075" y="489" font-size="22" fill="#ffffff">Manage model lifecycle</text>
# MAGIC   <text x="1662" y="491" font-size="26" font-weight="bold" fill="#5c7a89" text-anchor="middle">Models in Unity Catalog</text>
# MAGIC   <!-- r4 -->
# MAGIC   <rect x="1055" y="525" width="860" height="72" fill="#d3d3d1"/>
# MAGIC   <path d="M1055,525 L1375,525 L1410,561 L1375,597 L1055,597 Z" fill="#5c7a89"/>
# MAGIC   <text x="1075" y="569" font-size="22" fill="#ffffff">Retrain on Schedule or Trigger</text>
# MAGIC   <text x="1662" y="571" font-size="26" font-weight="bold" fill="#5c7a89" text-anchor="middle">Lakeflow Jobs, GitHub Actions, etc.</text>
# MAGIC   <!-- r5 -->
# MAGIC   <rect x="1055" y="605" width="860" height="72" fill="#d3d3d1"/>
# MAGIC   <path d="M1055,605 L1375,605 L1410,641 L1375,677 L1055,677 Z" fill="#5c7a89"/>
# MAGIC   <text x="1075" y="635" font-size="22" fill="#ffffff">Monitor model quality and</text>
# MAGIC   <text x="1075" y="661" font-size="22" fill="#ffffff">endpoint health</text>
# MAGIC   <text x="1662" y="635" font-size="26" font-weight="bold" fill="#5c7a89" text-anchor="middle">Service logs, Build logs, Endpoint</text>
# MAGIC   <text x="1662" y="663" font-size="26" font-weight="bold" fill="#5c7a89" text-anchor="middle">health metrics, Unity AI Gateway</text>
# MAGIC   <!-- r6 -->
# MAGIC   <rect x="1055" y="685" width="860" height="72" fill="#d3d3d1"/>
# MAGIC   <path d="M1055,685 L1375,685 L1410,721 L1375,757 L1055,757 Z" fill="#5c7a89"/>
# MAGIC   <text x="1075" y="729" font-size="22" fill="#ffffff">Automatic Data Quality Testing</text>
# MAGIC   <text x="1662" y="731" font-size="26" font-weight="bold" fill="#5c7a89" text-anchor="middle">Lakeflow Pipelines and Expectations</text>
# MAGIC </svg>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>