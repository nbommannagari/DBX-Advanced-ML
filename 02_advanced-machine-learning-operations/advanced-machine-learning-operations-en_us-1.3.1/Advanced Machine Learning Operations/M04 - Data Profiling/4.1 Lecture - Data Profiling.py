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
# MAGIC # Lecture - Data Profiling
# MAGIC
# MAGIC Once a model is in production, monitoring keeps it trustworthy. This lecture distinguishes the types of monitoring and drift — data, concept, model-quality, and bias — and contrasts siloed classical monitoring with a unified modern approach. You will see what Databricks Data Profiling measures across snapshot, time-series, and inference tables, how to add custom metrics and SQL alerts, and how to visualize results in AI/BI dashboards and orchestrate them with Lakeflow Jobs.
# MAGIC
# MAGIC <br>
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this lecture, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Identify differences between classic monitoring and modern MLOps monitoring.
# MAGIC         <li>Explain what problems Data Profiling solves.
# MAGIC         <li>Explain various capabilities Data Profiling.
# MAGIC         <li>Understand how to use Databricks SQL queries to build custom metrics and how to setup SQL alerts.
# MAGIC         <li>Explain how DBSQL and AI/BI Dashboards help with monitoring within Databricks.
# MAGIC         <li>Explain how to integrate monitoring and alerts with Lakeflow Jobs.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## A. Monitoring in Machine Learning
# MAGIC
# MAGIC ### Types of Monitoring and Types of Drift
# MAGIC
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-bottom: 10px; font-size: 1em; border-right: 4px solid #ff5f46;">
# MAGIC         <h4>Types of Model Monitoring</h4>
# MAGIC     </td>
# MAGIC     <td style="padding-left: 20px; font-size: 1em;">
# MAGIC         <h4>Types of Drift</h4>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding-left: 10px; font-size: 1em; border-right: 4px solid #ff5f46;">
# MAGIC         <ul>
# MAGIC         <li><b>Business Requirements:</b></li>
# MAGIC             <ul style="padding-left: 30px;"><li>Ensuring the ML solution <b>aligns with and fulfills specific business objectives</b>.</li></ul><br>
# MAGIC         <li><b>Resource Utilization:</b></li>
# MAGIC             <ul style="padding-left: 30px;"><li>Ensuring <b>efficient resource utilization</b> within the ML infrastructure.</li></ul><br>
# MAGIC         <li><b>Traceability:</b></li>
# MAGIC             <ul style="padding-left: 30px;"><li><b>Tracking data lineage</b> to understand the origin, movement, and transformation of data.</li></ul><br>
# MAGIC         <li><b>Model Performance:</b></li>
# MAGIC             <ul style="padding-left: 30px;"><li>Detecting and addressing types of <b>drift or degradation</b>.</li></ul>
# MAGIC         </li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC     <td style="padding-top: 10px; font-size: 1em;">
# MAGIC         <ul>
# MAGIC         <li><b>Data Drift:</b></li>
# MAGIC             <ul style="padding-left: 30px;"><li>Occurs when the <b>statistical properties</b> of the input data <b>change over time</b>.</li></ul><br>
# MAGIC         <li><b>Concept Drift:</b></li>
# MAGIC             <ul style="padding-left: 30px;"><li>Happens when the <b>relationship</b> between input features and the <b>target variable</b> changes.</li></ul><br>
# MAGIC         <li><b>Model Quality Drift:</b></li>
# MAGIC             <ul style="padding-left: 30px;"><li>Reflects a <b>decrease in the model's predictive performance</b> over time.</li></ul><br>
# MAGIC         <li><b>Bias Drift:</b></li>
# MAGIC             <ul style="padding-left: 30px;"><li>Involves shifts in model outcomes that could lead to <b>unfair treatment</b> of certain groups.</li></ul>
# MAGIC         </li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## B. Classical Monitoring vs. Modern Monitoring
# MAGIC
# MAGIC ### Bridging Siloed Practices: Unified Approach to Data and ML Monitoring
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding: 10px; font-size: 1em; border-right: 4px solid #ff5f46;">
# MAGIC         <h4>Classical Monitoring</h4>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         Same needs, different paradigms, different tools, different approaches
# MAGIC     </td>
# MAGIC     <td style="padding: 30px; font-size: 1em;">
# MAGIC         <h4>Modern Monitoring</h4>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         Shared monitoring goals, tools, and approach
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 10px; font-size: 1em; border-right: 4px solid #ff5f46;">
# MAGIC         <ul>
# MAGIC             <li><b>Data Engineers/Analysts</b>
# MAGIC                 <ul style="padding-left: 30px;">
# MAGIC                     <li>Measure data quality flowing through pipelines and dashboards.</li>
# MAGIC                 </ul>
# MAGIC             </li><br>
# MAGIC             <li><b>Data Scientists/ML Engineers</b>
# MAGIC                 <ul style="padding-left: 30px;">
# MAGIC                     <li>Measure ML model quality and Data quality affects ML model quality</li>
# MAGIC                 </ul>
# MAGIC             </li>
# MAGIC         </ul>
# MAGIC
# MAGIC     </td>
# MAGIC     <td style="padding: 30px; font-size: 1em;">
# MAGIC         <ul>
# MAGIC             <li>Align monitoring for data quality and model performance across teams.</li>
# MAGIC             <div style="height:8px;"></div>
# MAGIC             <li>Use shared dashboards and pipelines for real-time collaboration.</li>
# MAGIC             <div style="height:8px;"></div>
# MAGIC             <li>Automate model retraining based on detected drifts or performance drops.</li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## C. What is Data Profiling
# MAGIC
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 2000 864" font-family="Arial, Helvetica, sans-serif">
# MAGIC   <defs>
# MAGIC     <linearGradient id="profileGrad" x1="0" y1="0" x2="0" y2="1">
# MAGIC       <stop offset="0" stop-color="#a11f4f"/>
# MAGIC       <stop offset="1" stop-color="#c25478"/>
# MAGIC     </linearGradient>
# MAGIC     <marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
# MAGIC       <path d="M0 0 L10 5 L0 10" fill="none" stroke="#2b3440" stroke-width="1.6"/>
# MAGIC     </marker>
# MAGIC   </defs>
# MAGIC   <!-- ===== dashed connector arrows ===== -->
# MAGIC   <g fill="none" stroke="#2b3440" stroke-width="2.5" stroke-dasharray="9 7">
# MAGIC     <!-- top rail split lines -->
# MAGIC     <path d="M278 118 L278 40 L905 40" marker-start="url(#arrow)"/>
# MAGIC     <path d="M905 40 L905 118" marker-end="url(#arrow)"/>
# MAGIC     <path d="M1075 118 L1075 40 L1720 40" marker-start="url(#arrow)"/>
# MAGIC     <path d="M1720 40 L1720 118" marker-end="url(#arrow)"/>
# MAGIC   </g>
# MAGIC
# MAGIC   <!-- ===== left: Data box ===== -->
# MAGIC   <rect x="20" y="120" width="570" height="235" rx="4" fill="#c6d6ea"/>
# MAGIC   <text x="305" y="205" text-anchor="middle" font-size="52" font-weight="bold" fill="#1c2733">Data</text>
# MAGIC   <text x="305" y="258" text-anchor="middle" font-size="38" fill="#1c2733">Automated profiling of</text>
# MAGIC   <text x="305" y="306" text-anchor="middle" font-size="38" fill="#1c2733">any Delta table</text>
# MAGIC
# MAGIC   <!-- table icon -->
# MAGIC   <g stroke="#1c2733" stroke-width="4" fill="none">
# MAGIC     <rect x="228" y="392" width="155" height="130" rx="6"/>
# MAGIC     <line x1="228" y1="432" x2="383" y2="432"/>
# MAGIC     <line x1="228" y1="470" x2="383" y2="470"/>
# MAGIC     <line x1="272" y1="432" x2="272" y2="522"/>
# MAGIC     <line x1="338" y1="432" x2="338" y2="522"/>
# MAGIC     <circle cx="245" cy="412" r="4" fill="#1c2733"/>
# MAGIC     <circle cx="262" cy="412" r="4" fill="#1c2733"/>
# MAGIC     <circle cx="279" cy="412" r="4" fill="#1c2733"/>
# MAGIC   </g>
# MAGIC
# MAGIC   <!-- ===== center: Data Profiling stacked cards ===== -->
# MAGIC   <g>
# MAGIC     <!-- stacked layers behind -->
# MAGIC     <path d="M712 120 h576 v320 l-288 60 -288 -60 Z" fill="#8a1a44" opacity="0.55" transform="translate(0,58)"/>
# MAGIC     <path d="M712 120 h576 v320 l-288 55 -288 -55 Z" fill="#8f1c46" opacity="0.65" transform="translate(0,46)"/>
# MAGIC     <path d="M712 120 h576 v320 l-288 50 -288 -50 Z" fill="#961e49" opacity="0.75" transform="translate(0,34)"/>
# MAGIC     <path d="M712 120 h576 v320 l-288 45 -288 -45 Z" fill="#9c204c" opacity="0.85" transform="translate(0,22)"/>
# MAGIC     <path d="M712 120 h576 v320 l-288 40 -288 -40 Z" fill="#a52250" opacity="0.92" transform="translate(0,11)"/>
# MAGIC     <!-- top card -->
# MAGIC     <path d="M712 120 h576 v320 l-288 40 -288 -40 Z" fill="url(#profileGrad)"/>
# MAGIC     <rect x="730" y="140" width="540" height="230" rx="2" fill="none" stroke="#ffffff" stroke-opacity="0.18" stroke-width="2"/>
# MAGIC     <text x="1000" y="205" text-anchor="middle" font-size="52" font-weight="bold" fill="#f2e63d">Data Profiling</text>
# MAGIC     <text x="1000" y="262" text-anchor="middle" font-size="40" fill="#ffffff">Quality across your entire</text>
# MAGIC     <text x="1000" y="312" text-anchor="middle" font-size="40" fill="#ffffff">data + AI estate</text>
# MAGIC   </g>
# MAGIC
# MAGIC   <!-- ===== right: AI box ===== -->
# MAGIC   <rect x="1410" y="120" width="570" height="235" rx="4" fill="#c6d6ea"/>
# MAGIC   <text x="1695" y="205" text-anchor="middle" font-size="52" font-weight="bold" fill="#1c2733">AI</text>
# MAGIC   <text x="1695" y="258" text-anchor="middle" font-size="38" fill="#1c2733">Model drift and</text>
# MAGIC   <text x="1695" y="306" text-anchor="middle" font-size="38" fill="#1c2733">performance</text>
# MAGIC
# MAGIC   <!-- node/cube icon -->
# MAGIC   <g stroke="#1c2733" stroke-width="4" fill="none">
# MAGIC     <path d="M1695 392 l40 24 v46 l-40 24 -40 -24 v-46 Z"/>
# MAGIC     <path d="M1655 416 l40 24 40 -24"/>
# MAGIC     <line x1="1695" y1="440" x2="1695" y2="486"/>
# MAGIC     <!-- center line: cube bottom down through branch to center circle -->
# MAGIC     <line x1="1695" y1="486" x2="1695" y2="552"/>
# MAGIC     <!-- side branches -->
# MAGIC     <path d="M1695 524 h-55 v18"/>
# MAGIC     <path d="M1695 524 h55 v18"/>
# MAGIC     <circle cx="1640" cy="562" r="10"/>
# MAGIC     <circle cx="1695" cy="562" r="10"/>
# MAGIC     <circle cx="1750" cy="562" r="10"/>
# MAGIC   </g>
# MAGIC
# MAGIC   <!-- ===== bottom: Unity Catalog bar ===== -->
# MAGIC   <rect x="20" y="588" width="1960" height="256" rx="4" fill="#f7e2c8"/>
# MAGIC   <text x="1000" y="678" text-anchor="middle" font-size="52" font-weight="bold" fill="#1c2733">Unity Catalog</text>
# MAGIC
# MAGIC   <g font-size="38" font-weight="bold" fill="#e0663a" text-anchor="middle">
# MAGIC     <rect x="318" y="708" width="330" height="88" rx="3" fill="#c6d6ea"/>
# MAGIC     <text x="483" y="765">Metadata</text>
# MAGIC     <rect x="792" y="708" width="330" height="88" rx="3" fill="#c6d6ea"/>
# MAGIC     <text x="957" y="765">Governance</text>
# MAGIC     <rect x="1266" y="708" width="330" height="88" rx="3" fill="#c6d6ea"/>
# MAGIC     <text x="1431" y="765">Serverless</text>
# MAGIC   </g>
# MAGIC </svg>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## D. Why Data Profiling?
# MAGIC
# MAGIC **Monitor all tables in your Lakehouse using different analysis metrics based on table type(s)**
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 2000 673" font-family="Arial, Helvetica, sans-serif">
# MAGIC   <defs>
# MAGIC     <marker id="arr" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">
# MAGIC       <path d="M0 0 L10 5 L0 10" fill="none" stroke="#1c2733" stroke-width="1.6"/>
# MAGIC     </marker>
# MAGIC   </defs>
# MAGIC   <!-- ================= column 1: Snapshot Table ================= -->
# MAGIC   <text x="167" y="45" text-anchor="middle" font-size="34" font-weight="bold" fill="#1c2733" text-decoration="underline">Snapshot Table</text>
# MAGIC   <text x="167" y="98" text-anchor="middle" font-size="30" font-style="italic" fill="#1c2733">Bronze/Silver/Gold</text>
# MAGIC   <!-- yellow table icon -->
# MAGIC   <g stroke="#f2b134" stroke-width="4.5" fill="none">
# MAGIC     <rect x="92" y="122" width="150" height="122" rx="6"/>
# MAGIC     <line x1="92" y1="160" x2="242" y2="160"/>
# MAGIC     <line x1="92" y1="196" x2="242" y2="196"/>
# MAGIC     <line x1="135" y1="160" x2="135" y2="244"/>
# MAGIC     <line x1="199" y1="160" x2="199" y2="244"/>
# MAGIC     <circle cx="108" cy="141" r="4" fill="#f2b134" stroke="none"/>
# MAGIC     <circle cx="124" cy="141" r="4" fill="#f2b134" stroke="none"/>
# MAGIC     <circle cx="140" cy="141" r="4" fill="#f2b134" stroke="none"/>
# MAGIC   </g>
# MAGIC   <!-- monitor tag -->
# MAGIC   <path d="M62 270 h210 v72 h-190 l-20 -36 Z" fill="#eda42a"/>
# MAGIC   <text x="167" y="317" text-anchor="middle" font-size="40" font-weight="bold" fill="#1c2733">monitor</text>
# MAGIC   <!-- info box -->
# MAGIC   <rect x="62" y="415" width="200" height="82" fill="none" stroke="#1c2733" stroke-width="2"/>
# MAGIC   <text x="90" y="466" font-size="30" fill="#1c2733">&#8226; Columns</text>
# MAGIC   <!-- ================= column 2: Time Series Table ================= -->
# MAGIC   <text x="550" y="45" text-anchor="middle" font-size="34" font-weight="bold" fill="#1c2733" text-decoration="underline">Time Series Table</text>
# MAGIC   <text x="550" y="98" text-anchor="middle" font-size="30" font-style="italic" fill="#1c2733">Feature table</text>
# MAGIC   <!-- gray table icon -->
# MAGIC   <g stroke="#8a9aa5" stroke-width="4.5" fill="none">
# MAGIC     <rect x="475" y="122" width="150" height="122" rx="6"/>
# MAGIC     <line x1="475" y1="160" x2="625" y2="160"/>
# MAGIC     <line x1="475" y1="196" x2="625" y2="196"/>
# MAGIC     <line x1="518" y1="160" x2="518" y2="244"/>
# MAGIC     <line x1="582" y1="160" x2="582" y2="244"/>
# MAGIC     <circle cx="491" cy="141" r="4" fill="#8a9aa5" stroke="none"/>
# MAGIC     <circle cx="507" cy="141" r="4" fill="#8a9aa5" stroke="none"/>
# MAGIC     <circle cx="523" cy="141" r="4" fill="#8a9aa5" stroke="none"/>
# MAGIC   </g>
# MAGIC   <!-- monitor tag -->
# MAGIC   <path d="M445 270 h210 v72 h-190 l-20 -36 Z" fill="#eda42a"/>
# MAGIC   <text x="550" y="317" text-anchor="middle" font-size="40" font-weight="bold" fill="#1c2733">monitor</text>
# MAGIC   <!-- info box -->
# MAGIC   <rect x="425" y="415" width="330" height="120" fill="none" stroke="#1c2733" stroke-width="2"/>
# MAGIC   <text x="452" y="463" font-size="30" font-weight="bold" fill="#1c2733">&#8226; TimeStamp</text>
# MAGIC   <text x="452" y="505" font-size="30" fill="#1c2733">&#8226; Columns/Features</text>
# MAGIC   <!-- arrow to model -->
# MAGIC   <line x1="710" y1="183" x2="835" y2="183" stroke="#1c2733" stroke-width="3" marker-end="url(#arr)"/>
# MAGIC   <!-- ================= column 3: Model(s) ================= -->
# MAGIC   <text x="972" y="45" text-anchor="middle" font-size="34" font-weight="bold" fill="#1c2733">Model(s)</text>
# MAGIC   <!-- model / neural node icon -->
# MAGIC   <g stroke="#1c2733" stroke-width="3.5" fill="none">
# MAGIC     <circle cx="972" cy="183" r="22"/>
# MAGIC     <g>
# MAGIC       <line x1="972" y1="161" x2="972" y2="120"/><circle cx="972" cy="112" r="9"/>
# MAGIC       <line x1="972" y1="205" x2="972" y2="246"/><circle cx="972" cy="254" r="9"/>
# MAGIC       <line x1="994" y1="183" x2="1035" y2="183"/><circle cx="1043" cy="183" r="9"/>
# MAGIC       <line x1="950" y1="183" x2="909" y2="183"/><circle cx="901" cy="183" r="9"/>
# MAGIC       <line x1="988" y1="167" x2="1013" y2="142"/><circle cx="1019" cy="136" r="9"/>
# MAGIC       <line x1="956" y1="167" x2="931" y2="142"/><circle cx="925" cy="136" r="9"/>
# MAGIC       <line x1="988" y1="199" x2="1013" y2="224"/><circle cx="1019" cy="230" r="9"/>
# MAGIC       <line x1="956" y1="199" x2="931" y2="224"/><circle cx="925" cy="230" r="9"/>
# MAGIC     </g>
# MAGIC   </g>
# MAGIC   <!-- dashed vertical divider (red) -->
# MAGIC   <line x1="1218" y1="95" x2="1218" y2="415" stroke="#e8615a" stroke-width="2" stroke-dasharray="5 5"/>
# MAGIC   <!-- arrow to inference -->
# MAGIC   <line x1="1075" y1="183" x2="1690" y2="183" stroke="#1c2733" stroke-width="3" marker-end="url(#arr)"/>
# MAGIC   <!-- dotted info box -->
# MAGIC   <rect x="895" y="415" width="640" height="200" fill="none" stroke="#1c2733" stroke-width="2" stroke-dasharray="3 4"/>
# MAGIC   <g font-size="30" fill="#1c2733">
# MAGIC     <text x="930" y="470">&#8226;</text>
# MAGIC     <text x="985" y="470">Databricks batch scoring pipeline</text>
# MAGIC     <text x="930" y="512">&#8226;</text>
# MAGIC     <text x="985" y="512">Databricks Model Serving Endpoint</text>
# MAGIC     <text x="930" y="554">&#8226;</text>
# MAGIC     <text x="985" y="554">ETL to ingest from external serving</text>
# MAGIC     <text x="985" y="592">(request logs) or batch pipelines</text>
# MAGIC   </g>
# MAGIC   <!-- ================= column 4: Inference Table ================= -->
# MAGIC   <text x="1778" y="45" text-anchor="middle" font-size="34" font-weight="bold" fill="#1c2733" text-decoration="underline">Inference Table</text>
# MAGIC   <!-- purple table icon -->
# MAGIC   <g stroke="#9a94e8" stroke-width="4.5" fill="none">
# MAGIC     <rect x="1703" y="122" width="150" height="122" rx="6"/>
# MAGIC     <line x1="1703" y1="160" x2="1853" y2="160"/>
# MAGIC     <line x1="1703" y1="196" x2="1853" y2="196"/>
# MAGIC     <line x1="1746" y1="160" x2="1746" y2="244"/>
# MAGIC     <line x1="1810" y1="160" x2="1810" y2="244"/>
# MAGIC     <circle cx="1719" cy="141" r="4" fill="#9a94e8" stroke="none"/>
# MAGIC     <circle cx="1735" cy="141" r="4" fill="#9a94e8" stroke="none"/>
# MAGIC     <circle cx="1751" cy="141" r="4" fill="#9a94e8" stroke="none"/>
# MAGIC   </g>
# MAGIC   <!-- monitor tag -->
# MAGIC   <path d="M1673 270 h210 v72 h-190 l-20 -36 Z" fill="#eda42a"/>
# MAGIC   <text x="1778" y="317" text-anchor="middle" font-size="40" font-weight="bold" fill="#1c2733">monitor</text>
# MAGIC   <!-- info box -->
# MAGIC   <rect x="1673" y="415" width="315" height="230" fill="none" stroke="#1c2733" stroke-width="2"/>
# MAGIC   <g font-size="30" fill="#1c2733">
# MAGIC     <text x="1698" y="463" font-weight="bold">&#8226; TimeStamp</text>
# MAGIC     <text x="1698" y="503">&#8226; Features</text>
# MAGIC     <text x="1698" y="543"><tspan font-weight="bold">&#8226; Prediction</tspan> column</text>
# MAGIC     <text x="1698" y="583">&#8226; Label column</text>
# MAGIC     <text x="1698" y="623" font-weight="bold">&#8226; Model ID</text>
# MAGIC   </g>
# MAGIC </svg>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## E. Table Monitoring : Capabilities - Part 1
# MAGIC
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 0.9em;">
# MAGIC         <br><br>
# MAGIC         <div style="display: flex; align-items: center; gap: 16px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_81_1.png" alt="Image description" style="max-width: 100px; height: auto; border-radius: 0px;">
# MAGIC             <div>
# MAGIC                 <b>Monitor Key Metrics</b><br>
# MAGIC                 For table fields such as Nulls, null %, zeros, zero %, avg, distincts, distinct %, max, min, stdev, median, max/min/avg length, value frequencies, quantiles, row counts
# MAGIC             </div>
# MAGIC         </div>
# MAGIC         <br>
# MAGIC         <br>
# MAGIC         <div style="display: flex; align-items: center; gap: 16px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_81_2.png" alt="Image description" style="max-width: 100px; height: auto; border-radius: 0px;">
# MAGIC             <div>
# MAGIC                 <b>Group Metrics Over Time</b><br>
# MAGIC                 Monitor metrics over time windows, e.g. every day, every 5 minutes, over n weeks
# MAGIC             </div>
# MAGIC         </div>
# MAGIC         <br>
# MAGIC         <br>
# MAGIC         <div style="display: flex; align-items: center; gap: 16px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_81_3.png" alt="Image description" style="max-width: 100px; height: auto; border-radius: 0px;">
# MAGIC             <div>
# MAGIC                 <b>Monitor Data Slices</b><br>
# MAGIC                 Slice metrics based on columns or predicates, e.g. state, product_class, “cart_total > 1000”
# MAGIC             </div>
# MAGIC         </div>
# MAGIC         <br>
# MAGIC         <br>
# MAGIC         <div style="display: flex; align-items: center; gap: 16px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_81_4.png" alt="Image description" style="max-width: 100px; height: auto; border-radius: 0px;">
# MAGIC             <div>
# MAGIC                 <b>Monitor Tables and ML Models</b><br>
# MAGIC                 Consistent quality & drift monitoring of all your production assets
# MAGIC             </div>
# MAGIC         </div>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 10px; font-size: 1em;">
# MAGIC         <h4 style="text-align:center;">UI (UC Data Explorer)</h4>
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_81_5.png" alt="Image description" style="max-width: 100%; height: auto;"><br>
# MAGIC         <h4 style="text-align:center;">Python API</h4>
# MAGIC         <div class="code-block-dark" data-language="python" data-source="raw-python-1"></div>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC
# MAGIC <textarea id="raw-python-1" style="display:none;">
# MAGIC import databricks.lakehouse_monitoring as lm
# MAGIC
# MAGIC # Set up monitoring parameters
# MAGIC
# MAGIC lm.create_monitor(
# MAGIC   table_name=”my_UC_table”, 
# MAGIC   …)
# MAGIC
# MAGIC # Refresh monitoring metrics 
# MAGIC lm.run_refresh(”my_table”)
# MAGIC </textarea>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-okaidia.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC
# MAGIC         var lang = block.getAttribute('data-language') || 'python';
# MAGIC         var sourceId = block.getAttribute('data-source');
# MAGIC
# MAGIC         var raw;
# MAGIC         if (sourceId) {
# MAGIC             var rawEl = document.getElementById(sourceId);
# MAGIC             if (!rawEl) return;
# MAGIC             raw = rawEl.value;
# MAGIC         } else {
# MAGIC             raw = block.textContent;
# MAGIC         }
# MAGIC
# MAGIC         var code = raw.trim();
# MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
# MAGIC
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#272822;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
# MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
# MAGIC                 '</pre>' +
# MAGIC             '</div>';
# MAGIC
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## F. Table Monitoring : Capabilities - Part 2
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 0.9em;">
# MAGIC         <br>
# MAGIC         <div style="display: flex; align-items: center; gap: 16px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_82_1.png" alt="Image description" style="max-width: 100px; height: auto; border-radius: 0px;">
# MAGIC             <div>
# MAGIC                 <b>Custom or Auto-thresholds</b><br>
# MAGIC                 Set thresholds or use auto-generated alert thresholds  based on baseline data
# MAGIC             </div>
# MAGIC         </div>
# MAGIC         <br>
# MAGIC         <div style="display: flex; align-items: center; gap: 16px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_82_2.png" alt="Image description" style="max-width: 100px; height: auto; border-radius: 0px;">
# MAGIC             <div>
# MAGIC                 <b>Dashboards and Alerts</b><br>
# MAGIC                 Auto-generated DB SQL dashboards to visualize metrics & trends, SQL Alerts for notifications
# MAGIC             </div>
# MAGIC         </div>
# MAGIC         <br>
# MAGIC         <div style="display: flex; align-items: center; gap: 16px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_82_3.png" alt="Image description" style="max-width: 100px; height: auto; border-radius: 0px;">
# MAGIC             <div>
# MAGIC                 <b>Open Monitoring Results</b><br>
# MAGIC                 Monitoring results stored in open Delta tables to build custom analytics using your favorite BI tool
# MAGIC             </div>
# MAGIC         </div>
# MAGIC         <br>
# MAGIC         <div style="display: flex; align-items: center; gap: 16px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_82_4.png" alt="Image description" style="max-width: 100px; height: auto; border-radius: 0px;">
# MAGIC             <div>
# MAGIC                 <b>Simple Operations</b><br>
# MAGIC                 Databricks Serverless eliminates infrastructure management and scaling complexity
# MAGIC             </div>
# MAGIC         </div>
# MAGIC    </td>
# MAGIC     <td style="padding: 10px;">
# MAGIC         <br>
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_82_5.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## G. Monitoring a table in the Lakehouse
# MAGIC
# MAGIC **Whether data comes from data ingestion or model outputs, data manifests as tables.**
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:60%">
# MAGIC     <col style="width:40%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-left: 10px;">
# MAGIC       <h3 style="text-align:center;">How does it work?</h3><br>       
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_83.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC         <br><br>
# MAGIC         <b>Background (scheduled) service that incrementally processes data in Unity Catalog tables:</b>
# MAGIC         <ul>
# MAGIC         <li>Calculates <b>profile metrics</b> stored in Unity Catalog table</li>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li>Calculates <b>drift metrics</b> stored in Unity Catalog table</li> 
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li>Supports <b>custom metrics</b> as SQL expressions</li>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li><b>Auto-generates dashboard</b> to visualize metrics over time</li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## H. Out-of-the-box Standard Metrics & Custom Metrics
# MAGIC
# MAGIC #### These can be exported to any BI tool (we connect to DBSQL automatically).
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-bottom: 0px; font-size: 1em; border-right: 4px solid #ddd;">
# MAGIC         <h4>Out-of-the-Box Metrics:</h4>
# MAGIC         Get a standard set of metrics by default
# MAGIC     </td>
# MAGIC     <td style="padding-left: 20px; font-size: 1em;">
# MAGIC         <h4>Custom Metric Support:</h4>
# MAGIC         Express custom business metrics as SQL expressions when you configure your monitor
# MAGIC
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding-left: 10px; font-size: 1em; border-right: 4px solid #ddd;">
# MAGIC         <ul>
# MAGIC         <li><b>Distributional statistics for inputs, output, ground-truth</b></li>
# MAGIC             <ul style="padding-left: 30px;"><li>Minimum, maximum, standard deviation, quantiles, top occurring value, …</li></ul><br>
# MAGIC         <li><b>Anomaly detection and drift for training-vs-scoring and scoring-vs-scoring</b></li>
# MAGIC             <ul style="padding-left: 30px;"><li>Delta/changes in nulls and counts, PSI, KS divergence, Mean shift, Total Variation distance, L-inf distance, χ2 test, Wasserstein distance, …</li></ul><br>
# MAGIC         <li><b>Model quality metrics (if labels are provided)</b></li>
# MAGIC             <ul style="padding-left: 30px;"><li>Classification: Accuracy, F1, precision, recall Regression: MSE, RMSE, MAE, R2, …</li></ul><br>
# MAGIC         </li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC     <td style="padding-top: 10px; font-size: 1em;">
# MAGIC         <ul>
# MAGIC         <li><b>Custom metrics</b></li>
# MAGIC             <ul style="padding-left: 30px;"><li>Expressed as SQL expressions</li></ul>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## I. Fully Integrated with Unity Catalog
# MAGIC
# MAGIC ### Enhancing collaboration and efficiency with AI powered SQL Editor
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
# MAGIC         <li><b>Use Databricks SQL Query to Create Metrics and Tables</b> and add the tables or visualizations to a dashboard.</li>
# MAGIC         <div style="height:10px;"></div>
# MAGIC         <li><b>Databricks Assistant</b> is an AI-based assistant that enhances <b>user productivity</b> by generating SQL queries, explaining complex code, and <b>fixing errors</b>.</li>
# MAGIC         <div style="height:10px;"></div>
# MAGIC         <li>Describe your task in <b>English</b>, and Databricks Assistant will <b>automatically handle SQL generation</b>, code explanation, and error correction.</li>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 10px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_85.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## J. Visualize performance in auto-generated AI/BI Dashboards
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
# MAGIC         <li><b>Frictionless:</b></li>
# MAGIC         <ul><li><b>Dashboard automatically created</b> for you.</li></ul>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li><b>Visualize Performance:</b></li>
# MAGIC         <ul><li>Gain insights into data integrity, performance, drift, and more.</li></ul>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li><b>Share with stakeholders:</b></li>
# MAGIC         <ul><li>Can be shared with other users or groups.</li></ul>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li><b>Customizable:</b></li>
# MAGIC         <ul><li>Dashboard can be edited and <b>visuals of custom metrics added</b> to best suit your needs.</li></ul>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li><b>AI Assistant:</b></li>
# MAGIC         <ul><li>Quickly generate custom widgets with Databricks AI Assistant.</li></ul>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 10px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_86.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## K. What are Databricks SQL alerts?
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
# MAGIC         <li>Databricks SQL alerts <b>periodically run queries</b>, <b>evaluate defined conditions</b>, and <b>send notifications</b> if a condition is met.</li>
# MAGIC             <div style="height:8px;"></div>
# MAGIC             <ul style="padding-left: 20px; font-size: 0.9em;"><li><b>Scheduled Execution:</b> Automatically runs queries at defined intervals to check specific conditions.</li>
# MAGIC             <div style="height:8px;"></div>
# MAGIC             <li><b>Multi-Channel Notifications:</b> Receive alerts via Email, Slack, Webhook, MS Teams, PagerDuty, and more.</li></ul>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li><b>Explore</b> the <a href="https://docs.databricks.com/aws/en/sql/user/alerts#create-an-alert" target="_blank" style="color: #ff0000;">documentation</a> for in-depth setup and customization options.</li>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 10px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_87.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## L. Orchestrate your SQL queries, dashboards, and alerts with Lakeflow Jobs
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
# MAGIC         <li>Create, schedule, operate and monitor workflows that include Databricks SQL objects such as queries, dashboards and alerts.</li>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li>Automate and schedule your Databricks SQL production workloads with advanced workflow orchestration, reliable monitoring and observability.</li>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li>Set up a Databricks Job and include your SQL tasks such as refreshing dashboards or triggering alerts.</li>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 10px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_88_1.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC         <br>
# MAGIC         <div style="display: flex; align-items: center; gap: 16px;">
# MAGIC             <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_88_2.png" alt="Image description" style="max-width: 50px; height: auto; border-radius: 0px;">
# MAGIC             <div style="display: flex; flex-direction: column; align-items: flex-start; gap: 8px;">
# MAGIC                 <b>Your Quality Bot!!!</b>
# MAGIC                 <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M4L1_88_3.png" alt="Image description" style="max-width: 500px; height: auto; border-radius: 0px;">
# MAGIC             </div>
# MAGIC         </div>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>