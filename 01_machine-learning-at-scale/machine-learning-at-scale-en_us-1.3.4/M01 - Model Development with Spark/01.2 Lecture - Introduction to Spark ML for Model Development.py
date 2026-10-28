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
# MAGIC # Lecture: Introduction to Spark ML for Model Development
# MAGIC #### Machine Learning Development with Spark
# MAGIC
# MAGIC How to develop machine learning models at scale with Apache Spark on Databricks — reading and governing data with Delta Lake and Unity Catalog, the Spark ML library and its core submodules, and an end-to-end walkthrough of preparing data, training, tuning, evaluating, and tracking a model.
# MAGIC
# MAGIC <div style="max-width:1180px;margin:20px 0 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><div style="background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-left:4px solid #FF5F46;border-radius:10px;padding:18px 22px;"><div style="display:flex;align-items:center;gap:12px;"><svg viewBox="0 0 32 32" width="30" height="30" aria-hidden="true" style="flex:0 0 30px;"><g fill="none" stroke="#FF5F46" stroke-width="1.7"><circle cx="16" cy="16" r="12"/><circle cx="16" cy="16" r="7"/><circle cx="16" cy="16" r="2.4" fill="#FF5F46"/></g></svg><div style="font-size:12px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#FF5F46;">Learning Objectives</div></div><p style="margin:12px 0 8px;font-size:15px;color:#33414B;">By the end of this lesson, you will be able to:</p><ul style="line-height:1.7;font-size:14.5px;color:#33414B;margin:0;padding-left:22px;"><li style="margin:4px 0;">Read and govern data for machine learning using Delta Lake and Unity Catalog.</li><li style="margin:4px 0;">Describe the Spark ML library and its core building blocks — transformers, estimators, and pipelines.</li><li style="margin:4px 0;">Build an end-to-end workflow to prepare, train, tune, evaluate, and track a model with Spark ML.</li></ul></div></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background:#0B2027;border-radius:8px;padding:46px 24px;text-align:center;margin:6px 0;">
# MAGIC   <span style="color:#ffffff;font-size:30px;font-weight:800;letter-spacing:.3px;">Reading, Writing, and Governance with Spark on Databricks</span>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## What is Delta Lake?
# MAGIC
# MAGIC A **unified data management layer** that brings data reliability and fast analytics to cloud data lakes. It is the optimized storage layer that provides the foundation for storing data and tables in the Databricks Data Intelligence Platform.
# MAGIC
# MAGIC <div style="max-width:1300px;margin:0 auto;">
# MAGIC <svg width="100%" viewBox="0 0 1300 430" role="img" style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC   <title>Delta Lake unifies data sources and enables downstream capabilities</title>
# MAGIC   <desc>Structured, semi-structured, unstructured, and streaming data flow into Delta Lake, which enables data quality, data lineage, and featurization.</desc>
# MAGIC   <defs><marker id="dl-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#5A6B73" stroke-width="1.8" stroke-linecap="round"/></marker></defs>
# MAGIC   <!-- inputs -->
# MAGIC   <g font-size="16" fill="#1c2733" text-anchor="middle">
# MAGIC     <rect x="20" y="40" width="220" height="52" rx="6" fill="#EEF3FA" stroke="#B4C4D6"/><text x="130" y="72">Structured</text>
# MAGIC     <rect x="20" y="120" width="220" height="52" rx="6" fill="#EEF3FA" stroke="#B4C4D6"/><text x="130" y="152">Semi-structured</text>
# MAGIC     <rect x="20" y="200" width="220" height="52" rx="6" fill="#EEF3FA" stroke="#B4C4D6"/><text x="130" y="232">Unstructured</text>
# MAGIC     <rect x="20" y="280" width="220" height="52" rx="6" fill="#EEF3FA" stroke="#B4C4D6"/><text x="130" y="312">Streaming</text>
# MAGIC   </g>
# MAGIC   <g stroke="#5A6B73" stroke-width="2" fill="none">
# MAGIC     <path d="M240 66 C 320 66, 340 176, 470 186" marker-end="url(#dl-ar)"/>
# MAGIC     <path d="M240 146 C 330 146, 360 186, 470 196" marker-end="url(#dl-ar)"/>
# MAGIC     <path d="M240 226 C 330 226, 360 206, 470 206" marker-end="url(#dl-ar)"/>
# MAGIC     <path d="M240 306 C 320 306, 340 226, 470 216" marker-end="url(#dl-ar)"/>
# MAGIC   </g>
# MAGIC   <!-- Delta Lake core -->
# MAGIC   <rect x="475" y="120" width="330" height="150" rx="10" fill="#00ADD4"/>
# MAGIC   <text x="640" y="185" text-anchor="middle" font-size="30" font-weight="800" fill="#0B2027">DELTA LAKE</text>
# MAGIC   <text x="640" y="220" text-anchor="middle" font-size="18" fill="#0B2027">Open | Fast | Reliable</text>
# MAGIC   <!-- outputs -->
# MAGIC   <g stroke="#5A6B73" stroke-width="2" fill="none">
# MAGIC     <path d="M805 165 C 900 150, 940 90, 1040 82" marker-end="url(#dl-ar)"/>
# MAGIC     <path d="M805 195 C 920 195, 960 195, 1040 195" marker-end="url(#dl-ar)"/>
# MAGIC     <path d="M805 225 C 900 240, 940 300, 1040 308" marker-end="url(#dl-ar)"/>
# MAGIC   </g>
# MAGIC   <g font-size="16" fill="#1c2733">
# MAGIC     <rect x="1040" y="52" width="240" height="62" rx="6" fill="#F4F6F7" stroke="#C7CDD2"/>
# MAGIC     <text x="1060" y="78" font-weight="700">Data Quality</text><text x="1060" y="100" font-size="13">Performance • Consistency</text>
# MAGIC     <rect x="1040" y="164" width="240" height="62" rx="6" fill="#F4F6F7" stroke="#C7CDD2"/>
# MAGIC     <text x="1060" y="190" font-weight="700">Data Lineage</text><text x="1060" y="212" font-size="13">Version tracking • Governance</text>
# MAGIC     <rect x="1040" y="276" width="240" height="62" rx="6" fill="#F4F6F7" stroke="#C7CDD2"/>
# MAGIC     <text x="1060" y="302" font-weight="700">Featurization</text><text x="1060" y="324" font-size="13">Definition • Sharing • Drift</text>
# MAGIC   </g>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC **Key features:** unified batch and streaming · schema validation · upserts with `MERGE` · schema evolution · Change Data Feed · time travel · optimized performance (data skipping &amp; clustering) · multi-language support (Python, Scala, SQL).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Spark DataFrames
# MAGIC ### Use Spark DataFrames for scalable data manipulation
# MAGIC
# MAGIC <table><colgroup><col style="width:52%"><col style="width:48%"></colgroup><tr>
# MAGIC <td style="vertical-align:middle;">
# MAGIC <div style="max-width:620px;">
# MAGIC <svg width="100%" viewBox="0 0 620 470" role="img" style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC   <title>Apache Spark components</title>
# MAGIC   <desc>Apache Spark components: language interfaces over the DataFrame API, which sits over Spark ML, Streaming, Spark SQL and GraphX, over the RDD API and Spark Core API.</desc>
# MAGIC   <rect x="10" y="10" width="600" height="450" rx="10" fill="#AEB9C2"/>
# MAGIC   <text x="310" y="52" text-anchor="middle" font-size="22" font-weight="800" fill="#0B2027">Apache Spark Components</text>
# MAGIC   <g font-size="14" fill="#fff" text-anchor="middle">
# MAGIC     <rect x="30" y="72" width="108" height="66" rx="4" fill="#22333F"/><text x="84" y="102" font-weight="700">Python</text><text x="84" y="122" font-size="11" font-style="italic">PySpark</text>
# MAGIC     <rect x="146" y="72" width="108" height="66" rx="4" fill="#22333F"/><text x="200" y="102" font-weight="700">R</text><text x="200" y="122" font-size="11" font-style="italic">SparkR</text>
# MAGIC     <rect x="262" y="72" width="108" height="66" rx="4" fill="#22333F"/><text x="316" y="102" font-weight="700">Scala</text>
# MAGIC     <rect x="378" y="72" width="108" height="66" rx="4" fill="#22333F"/><text x="432" y="102" font-weight="700">Java</text>
# MAGIC     <rect x="494" y="72" width="96" height="66" rx="4" fill="#22333F"/><text x="542" y="102" font-weight="700">SQL</text>
# MAGIC   </g>
# MAGIC   <rect x="30" y="150" width="560" height="40" rx="4" fill="#FF5F46"/><text x="310" y="177" text-anchor="middle" font-size="18" font-weight="800" fill="#0B2027">DataFrame API</text>
# MAGIC   <g font-size="13" fill="#0B2027" text-anchor="middle">
# MAGIC     <rect x="30" y="202" width="132" height="150" rx="4" fill="#D4B863"/><text x="96" y="272" font-weight="700">Spark ML</text><text x="96" y="294" font-size="11">MLlib</text>
# MAGIC     <rect x="172" y="202" width="132" height="150" rx="4" fill="#D4B863"/><text x="238" y="272" font-weight="700">Streaming</text>
# MAGIC     <rect x="314" y="202" width="132" height="150" rx="4" fill="#D4B863"/><text x="380" y="264" font-weight="700">Spark SQL +</text><text x="380" y="284" font-weight="700">DataFrames</text>
# MAGIC     <rect x="456" y="202" width="134" height="150" rx="4" fill="#D4B863"/><text x="523" y="272" font-weight="700">GraphX</text>
# MAGIC   </g>
# MAGIC   <rect x="30" y="364" width="560" height="38" rx="4" fill="#FF5F46"/><text x="310" y="390" text-anchor="middle" font-size="17" font-weight="800" fill="#0B2027">RDD API</text>
# MAGIC   <rect x="30" y="410" width="560" height="38" rx="4" fill="#1CA971"/><text x="310" y="436" text-anchor="middle" font-size="17" font-weight="800" fill="#0B2027">Spark Core API</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </td>
# MAGIC <td style="font-size:1.05em;vertical-align:middle;">
# MAGIC <ul style="line-height:1.9;">
# MAGIC <li><b>High-level API</b> for data manipulation &amp; UDFs</li>
# MAGIC <li>Well-defined <b>schema</b> with names &amp; data types</li>
# MAGIC <li>Query using <b>SQL syntax</b></li>
# MAGIC <li>Optimized processing — <b>Catalyst optimizer</b> &amp; Tungsten engine</li>
# MAGIC <li><b>Lazy evaluation</b></li>
# MAGIC <li><b>Distributed</b> across the cluster for parallel processing</li>
# MAGIC <li>Versatile with <b>data types</b> and <b>sources</b></li>
# MAGIC </ul>
# MAGIC </td>
# MAGIC </tr></table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Loading Data
# MAGIC ### Perform select, filter, join, groupBy, and aggregate operations efficiently
# MAGIC
# MAGIC Read a Delta table into a Spark DataFrame, transform it with the DataFrame API, and write it back to a Delta table (or a Feature Store table).
# MAGIC
# MAGIC <div style="overflow-x: auto; margin: 16px 0;">
# MAGIC <div style="min-width: 1040px; max-width: 1300px; margin: 0 auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 2%;">
# MAGIC <div style="flex:0 0 49%;max-width:49%;box-sizing:border-box;background:#F4F5F6;border:1px solid #C9CDD1;padding:12px 14px;overflow-x:auto;"><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;">%python
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Read a Delta table using spark.read.table</span>
# MAGIC df = spark.read.format(<span style="color:#2E8B57;">&quot;delta&quot;</span>).table(<span style="color:#2E8B57;">&quot;your_table_name&quot;</span>)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Display the DataFrame</span>
# MAGIC display(df)</pre></div>
# MAGIC <div style="flex:0 0 49%;max-width:49%;box-sizing:border-box;background:#F4F5F6;border:1px solid #C9CDD1;padding:12px 14px;overflow-x:auto;"><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#6E8B6E;font-style:italic;"># Write DataFrame to a Delta table</span>
# MAGIC df.write.mode(<span style="color:#2E8B57;">&quot;append&quot;</span>).saveAsTable(<span style="color:#2E8B57;">&quot;new_table_name&quot;</span>)
# MAGIC df.write.mode(<span style="color:#2E8B57;">&quot;overwrite&quot;</span>).saveAsTable(<span style="color:#2E8B57;">&quot;new_table_name&quot;</span>)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Write DataFrame to a Feature Store table</span>
# MAGIC fe = FeatureEngineeringClient()
# MAGIC fe.create_table(
# MAGIC     name=<span style="color:#2E8B57;">&quot;feature_table_name&quot;</span>,
# MAGIC     primary_keys=<span style="color:#2E8B57;">&quot;column_1&quot;</span>,
# MAGIC     df=df_ml_prepared)</pre></div>
# MAGIC </div>
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1200 268" role="img" style="display:block;margin:18px 0 4px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><title>Delta Lake to Spark DataFrame round trip</title><desc>A Delta table is read into a Spark DataFrame, transformed with the DataFrame API, and written back out to a Delta table.</desc><defs><marker id="ld-ar" viewBox="0 0 10 10" refX="7" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M0 0 L9 5 L0 10 Z" fill="#111111"/></marker></defs><foreignObject x="62" y="74" width="106" height="84"><div xmlns="http://www.w3.org/1999/xhtml" style="width:106px;height:84px;display:flex;align-items:center;justify-content:center;"><img src="data:image/png;base64,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" alt="Delta Lake" style="max-width:106px;max-height:84px;display:block;"/></div></foreignObject><text x="115" y="184" text-anchor="middle" font-size="19" font-weight="800" letter-spacing="1.2" fill="#1B3139">DELTA LAKE</text><text x="312" y="106" text-anchor="middle" font-size="17" fill="#1B3139">Read a Delta Table into a</text><text x="312" y="128" text-anchor="middle" font-size="17" fill="#1B3139">Spark DataFrame</text><path d="M196 143 H 440" stroke="#111111" stroke-width="4.5" fill="none" marker-end="url(#ld-ar)"/><foreignObject x="486" y="8" width="240" height="86"><div xmlns="http://www.w3.org/1999/xhtml" style="width:240px;height:86px;position:relative;overflow:hidden;"><img src="https://files.training.databricks.com/binder/prod_main/machine-learning-at-scale-en_us-1.3.4/images/20261023T044612Z/Includes/images/Apache Spark.png" alt="Apache Spark" style="position:absolute;left:37.3px;top:0.0px;width:165.9px;height:86.2px;max-width:none;"/></div></foreignObject><rect x="460" y="112" width="292" height="60" rx="3" fill="#DE7C3E"/><text x="606" y="153" text-anchor="middle" font-size="27" font-weight="800" fill="#FFFFFF">DataFrames</text><text x="606" y="200" text-anchor="middle" font-size="17" fill="#1B3139">Perform data</text><text x="606" y="222" text-anchor="middle" font-size="17" fill="#1B3139">manipulations using the</text><text x="606" y="244" text-anchor="middle" font-size="17" fill="#1B3139">Spark DataFrame API.</text><text x="892" y="106" text-anchor="middle" font-size="17" fill="#1B3139">Write to a Delta table</text><text x="892" y="128" text-anchor="middle" font-size="17" fill="#1B3139">from a Spark DataFrame.</text><path d="M772 143 H 1016" stroke="#111111" stroke-width="4.5" fill="none" marker-end="url(#ld-ar)"/><foreignObject x="1036" y="74" width="106" height="84"><div xmlns="http://www.w3.org/1999/xhtml" style="width:106px;height:84px;display:flex;align-items:center;justify-content:center;"><img src="data:image/png;base64,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" alt="Delta Lake" style="max-width:106px;max-height:84px;display:block;"/></div></foreignObject><text x="1089" y="184" text-anchor="middle" font-size="19" font-weight="800" letter-spacing="1.2" fill="#1B3139">DELTA LAKE</text></svg>
# MAGIC <div style="display: flex; justify-content: center;">
# MAGIC <div style="flex:0 0 74%;max-width:74%;box-sizing:border-box;background:#F4F5F6;border:1px solid #C9CDD1;padding:12px 14px;overflow-x:auto;"><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#6E8B6E;font-style:italic;"># Select features and label</span>
# MAGIC df_ml = df.select(<span style="color:#2E8B57;">&quot;feature1&quot;</span>, <span style="color:#2E8B57;">&quot;feature2&quot;</span>, <span style="color:#2E8B57;">&quot;label&quot;</span>)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Filter out rows with missing values</span>
# MAGIC df_ml_filtered = df_ml.na.drop()
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Create a new feature (e.g., interaction term)</span>
# MAGIC df_ml_prepared = df_ml_filtered.withColumn(
# MAGIC     <span style="color:#2E8B57;">&quot;feature_interaction&quot;</span>, col(<span style="color:#2E8B57;">&quot;feature1&quot;</span>) * col(<span style="color:#2E8B57;">&quot;feature2&quot;</span>))</pre></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Data Science &amp; AI on Databricks
# MAGIC
# MAGIC <table><colgroup><col style="width:26%"><col style="width:74%"></colgroup><tr>
# MAGIC <td style="vertical-align:middle;">
# MAGIC <div style="background:#0B2027;border:2px solid #FF5F46;border-radius:6px;padding:18px;color:#fff;">
# MAGIC <b style="font-size:1.15em;">Unity Catalog</b>
# MAGIC <p style="font-size:0.95em;line-height:1.5;">Plays a crucial role in managing, governing, and securing Delta Lake, enhancing Data Science, Data Processing, Lakeflow Jobs, and Databricks SQL capabilities.</p>
# MAGIC </div>
# MAGIC </td>
# MAGIC <td>
# MAGIC <div style="max-width:900px;">
# MAGIC <svg width="100%" viewBox="0 0 900 470" role="img" style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC   <title>Databricks Data Intelligence Platform</title>
# MAGIC   <desc>Four capabilities (Databricks AI, SDP, Lakeflow Jobs, Databricks SQL) sit on the Data Intelligence Engine, which sits on Unity Catalog and Delta Lake over the open data lake.</desc>
# MAGIC   <rect x="10" y="10" width="880" height="360" rx="8" fill="#1B5162"/>
# MAGIC   <g text-anchor="middle" font-family="-apple-system,sans-serif">
# MAGIC     <rect x="30" y="28" width="200" height="70" rx="5" fill="none" stroke="#7FA6B5" stroke-dasharray="4 3"/><text x="130" y="54" font-size="13" fill="#cfe0e6">Data Science &amp; AI/ML</text><text x="130" y="80" font-size="16" font-weight="700" fill="#fff">Databricks AI</text>
# MAGIC     <rect x="242" y="28" width="200" height="70" rx="5" fill="none" stroke="#7FA6B5" stroke-dasharray="4 3"/><text x="342" y="54" font-size="13" fill="#cfe0e6">ETL &amp; Real-time Analytics</text><text x="342" y="80" font-size="16" font-weight="700" fill="#fff">SDP</text>
# MAGIC     <rect x="454" y="28" width="200" height="70" rx="5" fill="none" stroke="#7FA6B5" stroke-dasharray="4 3"/><text x="554" y="54" font-size="13" fill="#cfe0e6">Orchestration</text><text x="554" y="80" font-size="16" font-weight="700" fill="#fff">Lakeflow Jobs</text>
# MAGIC     <rect x="666" y="28" width="204" height="70" rx="5" fill="none" stroke="#7FA6B5" stroke-dasharray="4 3"/><text x="768" y="54" font-size="13" fill="#cfe0e6">Data Warehousing</text><text x="768" y="80" font-size="16" font-weight="700" fill="#fff">Databricks SQL</text>
# MAGIC   </g>
# MAGIC   <text x="450" y="128" text-anchor="middle" font-size="18" font-weight="800" fill="#fff">Data Intelligence Engine</text>
# MAGIC   <rect x="30" y="146" width="840" height="86" rx="4" fill="#F4F1EA"/><text x="450" y="182" text-anchor="middle" font-size="20" font-weight="800" fill="#FF5F46">Unity Catalog</text><text x="450" y="210" text-anchor="middle" font-size="14" fill="#5A6B73">Securely get insights in natural language</text>
# MAGIC   <rect x="30" y="248" width="840" height="104" rx="4" fill="none" stroke="#7FA6B5" stroke-dasharray="5 4"/><text x="450" y="292" text-anchor="middle" font-size="20" font-weight="800" fill="#fff">Delta Lake</text><text x="450" y="320" text-anchor="middle" font-size="14" fill="#cfe0e6">Data layout is automatically optimized based on usage patterns</text>
# MAGIC   <text x="450" y="405" text-anchor="middle" font-size="20" font-weight="800" fill="#0B2027">Open Data Lake</text>
# MAGIC   <text x="450" y="435" text-anchor="middle" font-size="14" fill="#5A6B73">All Raw Data (Logs, Text, Audio, Video, Images)</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </td></tr></table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Unity Catalog
# MAGIC ### Key Capabilities
# MAGIC
# MAGIC <table><colgroup><col style="width:46%"><col style="width:54%"></colgroup><tr>
# MAGIC <td style="font-size:1.02em;vertical-align:top;padding-right:18px;">
# MAGIC <ul style="line-height:1.9;">
# MAGIC <li>Centralized governance, security, metadata and user management</li>
# MAGIC <li>Data access auditing &amp; data lineage</li>
# MAGIC <li>Data search and discovery</li>
# MAGIC <li>Easy sharing of assets across Databricks workspaces</li>
# MAGIC <li>Secure data sharing with Delta Sharing</li>
# MAGIC <li>Feature Engineering in Unity Catalog</li>
# MAGIC <li>A hosted version of MLflow Models in Unity Catalog</li>
# MAGIC </ul>
# MAGIC </td>
# MAGIC <td style="vertical-align:top;">
# MAGIC <div style="margin-bottom:14px;"><svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 720 172" role="img" style="display:block;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><title>Unity Catalog governed asset lifecycle</title><desc>Volumes of raw data become Delta tables, then Features, then Models, then Inference tables and Metric tables, all governed by Unity Catalog.</desc><defs><marker id="ucl-ar" viewBox="0 0 10 10" refX="7" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M0 0 L9 5 L0 10 Z" fill="#E8503A"/></marker></defs><rect x="6" y="6" width="708" height="160" rx="4" fill="#FFFFFF" stroke="#C7CDD2" stroke-width="1.4" stroke-dasharray="6 4"/><text x="24" y="30" font-size="11" font-weight="700" letter-spacing="1.1" fill="#5A6B73">UNITY CATALOG</text><g fill="none" stroke="#E8503A" stroke-width="2"><ellipse cx="70" cy="55" rx="18" ry="7"/><path d="M52 55 v24 a18 7 0 0 0 36 0 v-24"/><path d="M52 67 a18 7 0 0 0 36 0"/></g><text x="70" y="116" text-anchor="middle" font-size="11.5" fill="#1B3139">Volumes</text><text x="70" y="131" text-anchor="middle" font-size="11.5" fill="#1B3139">(raw data)</text><g fill="none" stroke="#E8503A" stroke-width="2" stroke-linejoin="round"><rect x="164" y="51" width="42" height="34" rx="3"/><path d="M164 61 h42 M164 71 h42 M178 61 v24"/></g><text x="185" y="116" text-anchor="middle" font-size="11.5" fill="#1B3139">Delta tables</text><path d="M100 68 H 153" stroke="#E8503A" stroke-width="2" fill="none" marker-end="url(#ucl-ar)"/><path d="M127.5 86 l9 15 h-18 Z" fill="none" stroke="#00ADD4" stroke-width="1.8" stroke-linejoin="round"/><g fill="none" stroke="#E8503A" stroke-width="2" stroke-linejoin="round"><rect x="279" y="51" width="42" height="34" rx="3"/><path d="M279 61 h42 M279 71 h42 M293 61 v24"/></g><text x="300" y="116" text-anchor="middle" font-size="11.5" fill="#1B3139">Features</text><path d="M215 68 H 268" stroke="#E8503A" stroke-width="2" fill="none" marker-end="url(#ucl-ar)"/><path d="M242.5 86 l9 15 h-18 Z" fill="none" stroke="#00ADD4" stroke-width="1.8" stroke-linejoin="round"/><g fill="none" stroke="#E8503A" stroke-width="2" stroke-linecap="round"><path d="M413 52 a11 11 0 0 0 -11 11 a9 9 0 0 0 -3 16 a10 10 0 0 0 11 8"/><path d="M417 52 a11 11 0 0 1 11 11 a9 9 0 0 1 3 16 a10 10 0 0 1 -11 8"/><path d="M415 52 v35"/><circle cx="406" cy="64" r="2.4"/><circle cx="424" cy="72" r="2.4"/></g><text x="415" y="116" text-anchor="middle" font-size="11.5" fill="#1B3139">Models</text><path d="M330 68 H 383" stroke="#E8503A" stroke-width="2" fill="none" marker-end="url(#ucl-ar)"/><g fill="none" stroke="#E8503A" stroke-width="2" stroke-linecap="round"><path d="M511 52 v33 h38"/><path d="M517 77 L528 64 L537 70 L546 56"/><circle cx="517" cy="77" r="2.2"/><circle cx="528" cy="64" r="2.2"/><circle cx="537" cy="70" r="2.2"/><circle cx="546" cy="56" r="2.2"/></g><text x="530" y="116" text-anchor="middle" font-size="11.5" fill="#1B3139">Inference tables</text><path d="M445 68 H 498" stroke="#E8503A" stroke-width="2" fill="none" marker-end="url(#ucl-ar)"/><path d="M472.5 86 l9 15 h-18 Z" fill="none" stroke="#00ADD4" stroke-width="1.8" stroke-linejoin="round"/><g fill="none" stroke="#E8503A" stroke-width="2" stroke-linecap="round"><path d="M626 52 v33 h38"/><path d="M632 76 L641 62 L650 69 L661 55"/></g><text x="645" y="116" text-anchor="middle" font-size="11.5" fill="#1B3139">Metric tables</text><path d="M560 68 H 613" stroke="#E8503A" stroke-width="2" fill="none" marker-end="url(#ucl-ar)"/><path d="M587.5 86 l9 15 h-18 Z" fill="none" stroke="#00ADD4" stroke-width="1.8" stroke-linejoin="round"/></svg></div>
# MAGIC <div style="margin-bottom:14px;border:1px solid #E3E6E8;border-radius:4px;padding:8px 10px;"><svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 720 196" role="img" style="display:block;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><title>Three-level namespace</title><desc>A catalog contains schemas, which contain tables; together they form the three-part name used in SELECT star FROM catalog dot schema dot table.</desc><rect x="198" y="8" width="92" height="92" rx="3" fill="#FFFFFF" stroke="#C7CDD2" stroke-width="1.6"/><g fill="none" stroke="#E8503A" stroke-width="1.8" stroke-linejoin="round"><path d="M244 32 l19 11 v22 l-19 11 l-19 -11 v-22 Z"/><path d="M244 32 v11 M244 76 v-11 M225 43 l10 6 M263 43 l-10 6 M225 65 l10 -6 M263 65 l-10 -6"/><circle cx="244" cy="49" r="4"/><circle cx="244" cy="32" r="3"/><circle cx="225" cy="43" r="3"/><circle cx="263" cy="43" r="3"/><circle cx="225" cy="65" r="3"/><circle cx="263" cy="65" r="3"/><circle cx="244" cy="76" r="3"/></g><path d="M244 100 V 126" stroke="#5A6B73" stroke-width="1.4" fill="none"/><circle cx="244" cy="126" r="2.6" fill="#5A6B73"/><rect x="314" y="8" width="92" height="92" rx="3" fill="#FFFFFF" stroke="#C7CDD2" stroke-width="1.6"/><g fill="none" stroke="#E8503A" stroke-width="2"><ellipse cx="360" cy="43" rx="18" ry="7"/><path d="M342 43 v24 a18 7 0 0 0 36 0 v-24"/><path d="M342 55 a18 7 0 0 0 36 0"/></g><path d="M360 100 V 126" stroke="#5A6B73" stroke-width="1.4" fill="none"/><circle cx="360" cy="126" r="2.6" fill="#5A6B73"/><rect x="430" y="8" width="92" height="92" rx="3" fill="#FFFFFF" stroke="#C7CDD2" stroke-width="1.6"/><g fill="none" stroke="#E8503A" stroke-width="2" stroke-linejoin="round"><rect x="455" y="37" width="42" height="34" rx="3"/><path d="M455 47 h42 M455 57 h42 M469 47 v24"/></g><path d="M476 100 V 126" stroke="#5A6B73" stroke-width="1.4" fill="none"/><circle cx="476" cy="126" r="2.6" fill="#5A6B73"/><path d="M244 126 H 476" stroke="#5A6B73" stroke-width="1.4" fill="none"/><path d="M360 126 V 142" stroke="#5A6B73" stroke-width="1.4" fill="none"/><text x="360" y="170" text-anchor="middle" font-size="18" fill="#1B3139">SELECT * FROM catalog.schema.table</text></svg></div>
# MAGIC <div style="border:1px solid #E3E6E8;border-radius:4px;padding:10px;background:#FAFBFB;"><svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 720 320" role="img" style="display:block;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><title>Unity Catalog object hierarchy</title><desc>A metastore contains catalogs, which contain schemas, which contain models, tables, views, functions and volumes.</desc><defs><marker id="uch-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></marker></defs><rect x="290" y="14" width="140" height="44" rx="5" fill="#F4F6F7" stroke="#1B3139" stroke-width="1.4"/><text x="360" y="42" text-anchor="middle" font-size="16" font-weight="700" fill="#1B3139">Metastore</text><path d="M360 58 v24" stroke="#1B3139" stroke-width="1.8" fill="none" marker-end="url(#uch-ar)"/><rect x="290" y="90" width="140" height="44" rx="5" fill="#F4F6F7" stroke="#1B3139" stroke-width="1.4"/><text x="360" y="118" text-anchor="middle" font-size="16" font-weight="700" fill="#1B3139">Catalog</text><path d="M360 134 v24" stroke="#1B3139" stroke-width="1.8" fill="none" marker-end="url(#uch-ar)"/><rect x="275" y="166" width="170" height="52" rx="5" fill="#F4F6F7" stroke="#1B3139" stroke-width="1.4"/><text x="360" y="190" text-anchor="middle" font-size="16" font-weight="700" fill="#1B3139">Schema</text><text x="360" y="209" text-anchor="middle" font-size="13" fill="#5A6B73">(Database)</text><path d="M360 218 V 248" stroke="#1B3139" stroke-width="1.6" fill="none"/><path d="M84 248 H 636" stroke="#1B3139" stroke-width="1.6" fill="none"/><path d="M84 248 v20" stroke="#1B3139" stroke-width="1.6" fill="none" marker-end="url(#uch-ar)"/><rect x="23" y="270" width="122" height="42" rx="5" fill="#FF5F46"/><text x="84" y="297" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Model</text><path d="M222 248 v20" stroke="#1B3139" stroke-width="1.6" fill="none" marker-end="url(#uch-ar)"/><rect x="161" y="270" width="122" height="42" rx="5" fill="#FF5F46"/><text x="222" y="297" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Table</text><path d="M360 248 v20" stroke="#1B3139" stroke-width="1.6" fill="none" marker-end="url(#uch-ar)"/><rect x="299" y="270" width="122" height="42" rx="5" fill="#FF5F46"/><text x="360" y="297" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">View</text><path d="M498 248 v20" stroke="#1B3139" stroke-width="1.6" fill="none" marker-end="url(#uch-ar)"/><rect x="437" y="270" width="122" height="42" rx="5" fill="#FF5F46"/><text x="498" y="297" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Function</text><path d="M636 248 v20" stroke="#1B3139" stroke-width="1.6" fill="none" marker-end="url(#uch-ar)"/><rect x="575" y="270" width="122" height="42" rx="5" fill="#FF5F46"/><text x="636" y="297" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Volume</text></svg></div>
# MAGIC </td></tr></table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Automated lineage for all workloads
# MAGIC ### End-to-end visibility into how data flows and is consumed in your organization
# MAGIC
# MAGIC <table><colgroup><col style="width:42%"><col style="width:58%"></colgroup><tr>
# MAGIC <td style="font-size:1.02em;vertical-align:top;">
# MAGIC <ul style="line-height:1.9;">
# MAGIC <li>Table and column lineage</li>
# MAGIC <li>Notebook, workflow and dashboard table-centric lineage</li>
# MAGIC <li>Streaming lineage</li>
# MAGIC <li>File, path lineage</li>
# MAGIC <li>Feature Store lineage</li>
# MAGIC <li>Lakehouse Query lineage</li>
# MAGIC <li>SDP lineage</li>
# MAGIC <li>Volume lineage</li>
# MAGIC <li>MLflow model lineage</li>
# MAGIC <li>AI/BI Dashboards</li>
# MAGIC </ul>
# MAGIC </td>
# MAGIC <td style="vertical-align:top;">
# MAGIC <div style="border:2px dashed #9AA5AC;border-radius:8px;text-align:center;color:#5A6B73;background:#F7F8F9;">
# MAGIC   <img src="https://files.training.databricks.com/binder/prod_main/machine-learning-at-scale-en_us-1.3.4/images/20261023T044612Z/Includes/images/Unity Catalog lineage graph.png" alt="Unity Catalog lineage graph" style="width:100%;max-width:1000px;min-width:100%;border-radius:8px;border:1.5px solid #9AA5AC;margin-bottom:12px;object-fit:cover;"/>
# MAGIC </div>
# MAGIC <br>
# MAGIC <div style="border:2px dashed #9AA5AC;border-radius:8px;text-align:center;color:#5A6B73;background:#F7F8F9;">
# MAGIC   <img src="https://files.training.databricks.com/binder/prod_main/machine-learning-at-scale-en_us-1.3.4/images/20261023T044612Z/Includes/images/Databricks Lineage Service.png" alt="Databricks Lineage Service" style="width:70%;max-width:400px;min-width:70%;border-radius:8px;border:1.5px solid #9AA5AC;margin-bottom:12px;object-fit:contain;"/>
# MAGIC </div>
# MAGIC </td></tr></table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Feature Engineering &amp; the Unity Catalog Feature Store
# MAGIC
# MAGIC <table><colgroup><col style="width:50%"><col style="width:50%"></colgroup><tr>
# MAGIC <td style="font-size:1.02em;vertical-align:top;border-right:3px solid #FF5F46;padding-right:16px;">
# MAGIC <h4>Feature Engineering</h4>
# MAGIC <p><i>Transforming raw data into model-friendly features.</i></p>
# MAGIC <ul style="line-height:1.8;">
# MAGIC <li>A <b>critical step</b> in preparing data for machine learning models.</li>
# MAGIC <li>Transforms raw data into a format that allows ML models to <b>extract meaningful patterns</b> and make accurate predictions.</li>
# MAGIC </ul>
# MAGIC <div style="margin-top:18px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 620 250" role="img" style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><title>Feature engineering workflow</title><desc>Raw data is selected, extracted and transformed into features, which produce insights.</desc><defs><marker id="fe-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></marker></defs><rect x="6" y="82" width="112" height="88" rx="8" fill="#F2F3F4" stroke="#C7CDD2" stroke-width="1.5"/><g fill="none" stroke="#1B3139" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"><path d="M46 96 h24 l12 12 v30 h-36 z"/><path d="M70 96 v12 h12"/><path d="M52 118 h20 M52 128 h14"/></g><text x="62" y="158" text-anchor="middle" font-size="15" font-weight="700" fill="#1B3139">Raw Data</text><rect x="348" y="82" width="112" height="88" rx="8" fill="#F2F3F4" stroke="#C7CDD2" stroke-width="1.5"/><g fill="none" stroke="#1B3139" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"><rect x="382" y="96" width="44" height="42" rx="3"/><path d="M382 108 h44 M382 122 h44 M396 96 v42"/></g><text x="404" y="158" text-anchor="middle" font-size="15" font-weight="700" fill="#1B3139">Features</text><rect x="500" y="82" width="114" height="88" rx="8" fill="#F2F3F4" stroke="#C7CDD2" stroke-width="1.5"/><g fill="none" stroke="#1B3139" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"><circle cx="551" cy="114" r="17"/><path d="M544 121 v-7 M551 121 v-13 M558 121 v-10"/><path d="M564 127 l9 9"/></g><text x="557" y="158" text-anchor="middle" font-size="15" font-weight="700" fill="#1B3139">Insights</text><rect x="152" y="40" width="160" height="46" rx="8" fill="#FFFFFF" stroke="#FF5F46" stroke-width="1.8"/><circle cx="174" cy="63" r="4" fill="#FF5F46"/><text x="192" y="68" font-size="15" font-weight="700" fill="#1B3139">Selecting</text><rect x="152" y="104" width="160" height="46" rx="8" fill="#FFFFFF" stroke="#FF5F46" stroke-width="1.8"/><circle cx="174" cy="127" r="4" fill="#FF5F46"/><text x="192" y="132" font-size="15" font-weight="700" fill="#1B3139">Extracting</text><rect x="152" y="168" width="160" height="46" rx="8" fill="#FFFFFF" stroke="#FF5F46" stroke-width="1.8"/><circle cx="174" cy="191" r="4" fill="#FF5F46"/><text x="192" y="196" font-size="15" font-weight="700" fill="#1B3139">Transforming</text><path d="M118 126 C 132 126, 134 63, 148 63" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#fe-ar)"/><path d="M312 63 C 326 63, 328 126, 344 126" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#fe-ar)"/><path d="M118 126 C 132 126, 134 127, 148 127" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#fe-ar)"/><path d="M312 127 C 326 127, 328 126, 344 126" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#fe-ar)"/><path d="M118 126 C 132 126, 134 191, 148 191" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#fe-ar)"/><path d="M312 191 C 326 191, 328 126, 344 126" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#fe-ar)"/><path d="M460 126 H 496" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#fe-ar)"/></svg>
# MAGIC </div>
# MAGIC </td>
# MAGIC <td style="font-size:1.02em;vertical-align:top;padding-left:16px;">
# MAGIC <h4>Unity Catalog Feature Store</h4>
# MAGIC <p><i>Features are organized as feature tables.</i></p>
# MAGIC <ul style="line-height:1.8;">
# MAGIC <li>Features are <b>stored in and provided from Unity Catalog</b> as a <b>Delta table</b> with the required <b>primary key</b> and additional metadata.</li>
# MAGIC <li>Feature Registry + Feature Provider support <b>batch (high throughput)</b> and <b>online (low latency)</b> access.</li>
# MAGIC </ul>
# MAGIC <div style="margin-top:18px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 620 250" role="img" style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><title>Unity Catalog Feature Store</title><desc>The Feature Store holds a Feature Registry and a Feature Provider, which serves features for batch high-throughput and online low-latency access.</desc><defs><marker id="fs2-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#2E6E80" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></marker></defs><rect x="6" y="14" width="608" height="222" rx="4" fill="#EDEFF0"/><text x="30" y="52" font-size="17" font-weight="700" fill="#2E6E80">Feature Store</text><g fill="none" stroke="#2E6E80" stroke-width="2.4" stroke-linejoin="round"><rect x="42" y="116" width="42" height="42" rx="4"/><path d="M52 116 v-10 h42 v42 h-10"/></g><rect x="120" y="96" width="164" height="88" rx="4" fill="#5F8797"/><text x="202" y="134" text-anchor="middle" font-size="16" font-weight="700" fill="#FFFFFF">Feature</text><text x="202" y="156" text-anchor="middle" font-size="16" font-weight="700" fill="#FFFFFF">Registry</text><path d="M290 140 H 316" fill="none" stroke="#2E6E80" stroke-width="2.2" marker-end="url(#fs2-ar)"/><rect x="324" y="96" width="134" height="88" rx="4" fill="#5F8797"/><text x="391" y="134" text-anchor="middle" font-size="16" font-weight="700" fill="#FFFFFF">Feature</text><text x="391" y="156" text-anchor="middle" font-size="16" font-weight="700" fill="#FFFFFF">Provider</text><rect x="458" y="96" width="150" height="88" rx="4" fill="#4E7387"/><text x="472" y="126" font-size="13.5" fill="#FFFFFF">Batch (high</text><text x="472" y="144" font-size="13.5" fill="#FFFFFF">throughput)</text><text x="472" y="168" font-size="13.5" fill="#FFFFFF">Online (low latency)</text></svg>
# MAGIC </div>
# MAGIC </td></tr></table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background:#0B2027;border-radius:8px;padding:46px 24px;text-align:center;margin:6px 0;">
# MAGIC   <span style="color:#ffffff;font-size:30px;font-weight:800;letter-spacing:.3px;">Overview of Spark ML</span>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Machine Learning in Spark
# MAGIC ### Spark Machine Learning Libraries — Spark's built-in capabilities for distributed machine learning
# MAGIC
# MAGIC <table style="width:100%;border-collapse:separate;border-spacing:10px;"><tr>
# MAGIC <td style="width:50%;vertical-align:top;background:#5A7A88;border-radius:8px;padding:20px;color:#fff;">
# MAGIC   <div style="text-align:center;font-size:1.3em;font-weight:800;color:#F2B134;">MLlib</div>
# MAGIC   <p style="text-align:center;">Original ML API for Spark<br><b>RDD-based API</b></p>
# MAGIC   <ul style="line-height:1.8;"><li><code style="color:#fff;">pyspark.mllib</code></li><li><code style="color:#fff;">org.apache.spark.mllib</code></li></ul>
# MAGIC   <p style="text-align:center;">Maintenance mode<br><b style="color:#F2B134;font-size:1.1em;">Avoid use</b></p>
# MAGIC </td>
# MAGIC <td style="width:50%;vertical-align:top;background:#1B5162;border-radius:8px;padding:20px;color:#fff;">
# MAGIC   <div style="text-align:center;font-size:1.3em;font-weight:800;color:#F2B134;">Spark ML</div>
# MAGIC   <p style="text-align:center;">&#8220;Spark ML&#8221; is not an official name but refers to the <b>MLlib DataFrame-based API</b></p>
# MAGIC   <ul style="line-height:1.8;"><li><code style="color:#fff;">pyspark.ml</code></li><li><code style="color:#fff;">org.apache.spark.ml</code></li></ul>
# MAGIC   <p style="text-align:center;"><b style="color:#F2B134;font-size:1.1em;">Recommended Use</b></p>
# MAGIC </td></tr></table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Spark ML
# MAGIC ### Core submodules of Spark ML
# MAGIC
# MAGIC <div style="max-width: 1240px; margin: 0 auto; overflow-x: auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1240 620" role="img" style="min-width:900px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC   <title>Core submodules of Spark ML</title>
# MAGIC   <desc>Seven core Spark ML submodules radiate from the centre: Pipelines; Extracting, transforming and selecting features; Classification and Regression; Clustering; Collaborative filtering; Frequent Pattern Mining; and Model selection and tuning.</desc>
# MAGIC   <rect x="40" y="222" width="342" height="176" fill="#FFFFFF" stroke="#1C2733" stroke-width="1.6"/>
# MAGIC   <foreignObject x="58" y="244" width="196" height="118"><div xmlns="http://www.w3.org/1999/xhtml" style="width:196px;height:118px;display:flex;align-items:center;justify-content:center;"><img src="https://files.training.databricks.com/binder/prod_main/machine-learning-at-scale-en_us-1.3.4/images/20261023T044612Z/Includes/images/Apache Spark.png" alt="Apache Spark" style="max-width:196px;max-height:118px;display:block;"/></div></foreignObject>
# MAGIC   <text x="272" y="329" font-size="62" font-weight="700" fill="#E8722C">ML</text>
# MAGIC   <path d="M475 32 C 414 32, 436 310, 404 310 C 436 310, 414 588, 475 588" fill="none" stroke="#8A9299" stroke-width="3" stroke-linecap="round"/>
# MAGIC   <path d="M-27 -14 L3 -14 L3 -25 L27 0 L3 25 L3 14 L-27 14 Z" fill="#A8ADB2" transform="translate(790.0 194.0) rotate(-90.0)"/>
# MAGIC   <path d="M-27 -14 L3 -14 L3 -25 L27 0 L3 25 L3 14 L-27 14 Z" fill="#A8ADB2" transform="translate(880.7 237.7) rotate(-38.6)"/>
# MAGIC   <path d="M-27 -14 L3 -14 L3 -25 L27 0 L3 25 L3 14 L-27 14 Z" fill="#A8ADB2" transform="translate(903.1 335.8) rotate(12.9)"/>
# MAGIC   <path d="M-27 -14 L3 -14 L3 -25 L27 0 L3 25 L3 14 L-27 14 Z" fill="#A8ADB2" transform="translate(840.3 414.5) rotate(64.3)"/>
# MAGIC   <path d="M-27 -14 L3 -14 L3 -25 L27 0 L3 25 L3 14 L-27 14 Z" fill="#A8ADB2" transform="translate(739.7 414.5) rotate(115.7)"/>
# MAGIC   <path d="M-27 -14 L3 -14 L3 -25 L27 0 L3 25 L3 14 L-27 14 Z" fill="#A8ADB2" transform="translate(676.9 335.8) rotate(167.1)"/>
# MAGIC   <path d="M-27 -14 L3 -14 L3 -25 L27 0 L3 25 L3 14 L-27 14 Z" fill="#A8ADB2" transform="translate(699.3 237.7) rotate(218.6)"/>
# MAGIC   <circle cx="790" cy="310" r="82" fill="#1B5162" stroke="#E8EBED" stroke-width="3"/>
# MAGIC   <text x="790" y="292" text-anchor="middle" font-size="16" font-weight="700" fill="#FFFFFF">Core</text>
# MAGIC   <text x="790" y="314" text-anchor="middle" font-size="16" font-weight="700" fill="#FFFFFF">Submodules</text>
# MAGIC   <text x="790" y="336" text-anchor="middle" font-size="16" font-weight="700" fill="#FFFFFF">of Spark ML</text>
# MAGIC   <circle cx="790.0" cy="85.0" r="76" fill="#1B5162" stroke="#E8EBED" stroke-width="3"/>
# MAGIC   <text x="790.0" y="90.0" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Pipelines</text>
# MAGIC   <circle cx="965.9" cy="169.7" r="76" fill="#1B5162" stroke="#E8EBED" stroke-width="3"/>
# MAGIC   <text x="965.9" y="141.7" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Extracting,</text>
# MAGIC   <text x="965.9" y="163.7" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">transforming</text>
# MAGIC   <text x="965.9" y="185.7" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">and selecting</text>
# MAGIC   <text x="965.9" y="207.7" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">features</text>
# MAGIC   <circle cx="1009.4" cy="360.1" r="76" fill="#1B5162" stroke="#E8EBED" stroke-width="3"/>
# MAGIC   <text x="1009.4" y="343.1" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Classification</text>
# MAGIC   <text x="1009.4" y="365.1" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">and</text>
# MAGIC   <text x="1009.4" y="387.1" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Regression</text>
# MAGIC   <circle cx="887.6" cy="512.7" r="76" fill="#1B5162" stroke="#E8EBED" stroke-width="3"/>
# MAGIC   <text x="887.6" y="517.7" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Clustering</text>
# MAGIC   <circle cx="692.4" cy="512.7" r="76" fill="#1B5162" stroke="#E8EBED" stroke-width="3"/>
# MAGIC   <text x="692.4" y="506.7" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Collaborative</text>
# MAGIC   <text x="692.4" y="528.7" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">filtering</text>
# MAGIC   <circle cx="570.6" cy="360.1" r="76" fill="#1B5162" stroke="#E8EBED" stroke-width="3"/>
# MAGIC   <text x="570.6" y="343.1" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Frequent</text>
# MAGIC   <text x="570.6" y="365.1" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Pattern</text>
# MAGIC   <text x="570.6" y="387.1" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Mining</text>
# MAGIC   <circle cx="614.1" cy="169.7" r="76" fill="#1B5162" stroke="#E8EBED" stroke-width="3"/>
# MAGIC   <text x="614.1" y="152.7" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Model</text>
# MAGIC   <text x="614.1" y="174.7" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">selection</text>
# MAGIC   <text x="614.1" y="196.7" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">and tuning</text>
# MAGIC   </svg>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ML Pipelines
# MAGIC ### `spark.ml import Pipeline`
# MAGIC
# MAGIC <div style="max-width: 1180px; margin: 0 auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
# MAGIC <p style="font-size: 1.02em; line-height: 1.6; color: #33414B; margin: 0 0 18px;">
# MAGIC <b>Purpose:</b> streamline the building of machine learning <b>workflows</b> by chaining multiple stages &mdash; data preprocessing, feature extraction, and model training &mdash; into a single pipeline that is fitted and applied as one object.
# MAGIC </p>
# MAGIC <div style="overflow-x: auto; margin-bottom: 20px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1140 240" role="img" style="display:block;min-width:900px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><title>A Spark ML Pipeline chaining three stages</title><desc>A training DataFrame enters a Pipeline whose stages are Tokenizer, HashingTF and LogisticRegression. Each stage writes the column the next one reads: text becomes words, words become features. Fitting the pipeline returns a PipelineModel.</desc><defs><marker id="pl-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></marker></defs><rect x="8" y="84" width="118" height="64" rx="8" fill="#FFFFFF" stroke="#1B5162" stroke-width="1.4"/><text x="67" y="120" text-anchor="middle" font-size="15" font-weight="600" fill="#0b2026">training</text><text x="67" y="138" text-anchor="middle" font-size="11" fill="#618794">DataFrame</text><path d="M132 116 H194" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#pl-ar)"/><text x="163" y="76" text-anchor="middle" font-size="11.5" fill="#618794">text</text><rect x="200" y="30" width="652" height="172" rx="14" fill="#F9F7F4" stroke="#1B5162" stroke-opacity="0.4" stroke-width="1.4"/><text x="218" y="55" font-size="14" font-weight="700" fill="#FF5F46">Pipeline</text><rect x="224" y="84" width="140" height="64" rx="8" fill="#FFFFFF" stroke="#1B5162" stroke-width="1.4"/><text x="294" y="124" text-anchor="middle" font-size="15" font-weight="600" fill="#0b2026">Tokenizer</text><path d="M368 116 H410" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#pl-ar)"/><text x="389" y="76" text-anchor="middle" font-size="11.5" fill="#618794">words</text><rect x="414" y="84" width="140" height="64" rx="8" fill="#FFFFFF" stroke="#1B5162" stroke-width="1.4"/><text x="484" y="124" text-anchor="middle" font-size="15" font-weight="600" fill="#0b2026">HashingTF</text><path d="M558 116 H600" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#pl-ar)"/><text x="579" y="76" text-anchor="middle" font-size="11.5" fill="#618794">features</text><rect x="604" y="84" width="232" height="64" rx="8" fill="#FFFFFF" stroke="#1B5162" stroke-width="1.4"/><text x="720" y="124" text-anchor="middle" font-size="15" font-weight="600" fill="#0b2026">LogisticRegression</text><text x="526" y="190" text-anchor="middle" font-size="11.5" fill="#618794">stages=[tokenizer, hashingTF, lr]</text><path d="M858 116 H916" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#pl-ar)"/><text x="887" y="76" text-anchor="middle" font-size="11.5" fill="#618794">.fit()</text><rect x="922" y="84" width="200" height="64" rx="8" fill="#FFFFFF" stroke="#1B5162" stroke-width="1.4"/><text x="1022" y="120" text-anchor="middle" font-size="15" font-weight="600" fill="#0b2026">PipelineModel</text><text x="1022" y="138" text-anchor="middle" font-size="11" fill="#618794">a fitted Transformer</text></svg>
# MAGIC </div>
# MAGIC <div style="background: #F4F5F6; border: 1px solid #C9CDD1; border-radius: 4px; padding: 14px 16px; overflow-x: auto;">
# MAGIC <pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">from</span> pyspark.ml <span style="color:#0B5FA5;">import</span> Pipeline
# MAGIC <span style="color:#0B5FA5;">from</span> pyspark.ml.classification <span style="color:#0B5FA5;">import</span> LogisticRegression
# MAGIC <span style="color:#0B5FA5;">from</span> pyspark.ml.feature <span style="color:#0B5FA5;">import</span> HashingTF, Tokenizer
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Configure an ML pipeline of three stages: tokenizer, hashingTF, and lr</span>
# MAGIC tokenizer = Tokenizer(inputCol=<span style="color:#2E8B57;">&quot;text&quot;</span>, outputCol=<span style="color:#2E8B57;">&quot;words&quot;</span>)
# MAGIC hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol=<span style="color:#2E8B57;">&quot;features&quot;</span>)
# MAGIC lr = LogisticRegression(maxIter=10, regParam=0.001)
# MAGIC pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Fit the pipeline to training documents</span>
# MAGIC model = pipeline.fit(training)</pre>
# MAGIC </div>
# MAGIC <div style="margin-top: 18px; font-size: 1.02em; line-height: 1.7; color: #33414B;">
# MAGIC <b>How it works:</b>
# MAGIC <ol style="margin: 8px 0 0; padding-left: 22px;">
# MAGIC <li style="margin:4px 0;">Each stage declares an <code>inputCol</code> and an <code>outputCol</code>. <code>tokenizer.getOutputCol()</code> wires <code>HashingTF</code> to whatever the tokenizer produced, so renaming a column never breaks the chain.</li><li style="margin:4px 0;"><code>Pipeline(stages=[...])</code> is itself an <b>Estimator</b> &mdash; nothing has run yet.</li><li style="margin:4px 0;"><code>pipeline.fit(training)</code> runs each stage in order and returns a <b>PipelineModel</b>, a <b>Transformer</b> you call <code>.transform()</code> on for new data.</li>
# MAGIC </ol>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Extracting, Transforming and Selecting Features
# MAGIC ### `spark.ml.feature`
# MAGIC
# MAGIC <div style="max-width: 1180px; margin: 0 auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
# MAGIC <p style="font-size: 1.02em; line-height: 1.65; color: #33414B; margin: 0 0 6px;">
# MAGIC <b>Purpose:</b> this submodule covers five kinds of feature work &mdash; <b>transformation</b>, <b>feature extraction</b>, <b>dimensionality reduction</b>, <b>selection</b>, and <b>locality sensitive hashing</b>. Each card below is one representative class for one of those five.
# MAGIC </p>
# MAGIC <div style="display: flex; flex-wrap: wrap; gap: 16px; margin-top: 18px;">
# MAGIC <div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="10" width="10" height="13" rx="2"/><rect x="20" y="10" width="10" height="13" rx="2"/><path d="M5 15 h4 M5 18.5 h4"/><path d="M23 15 h4 M23 18.5 h4"/><path d="M13.5 16.5 h4.5 M16.2 14.2 l2.3 2.3 l-2.3 2.3"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Transformation</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">StringIndexer</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Converts categorical labels into numerical ones, so string columns can be fed to algorithms that only accept numbers.</p></div>
# MAGIC <div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M2 11 h8 M2 16 h6 M2 21 h8"/><path d="M12.5 16 h3.5 M14.4 14 l2 2 l-2 2"/><circle cx="21" cy="16" r="1.9"/><circle cx="26" cy="16" r="1.9"/><circle cx="31" cy="16" r="1.4"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Feature extraction</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Word2Vec</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">An <b>Estimator</b> that takes sequences of words and trains a <code>Word2VecModel</code> mapping each word to a fixed-size vector.</p></div>
# MAGIC <div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M3 27 V6 M3 27 H29"/><path d="M7 24 L26 9"/><circle cx="10" cy="19" r="1.5"/><circle cx="14" cy="21" r="1.5"/><circle cx="17" cy="15" r="1.5"/><circle cx="21" cy="16" r="1.5"/><circle cx="24" cy="11" r="1.5"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Dimensionality reduction</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">PCA</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Principal Component Analysis &mdash; projects high-dimensional data into fewer dimensions while preserving as much variance as possible.</p></div>
# MAGIC <div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6 h26 l-9.5 11.5 v9.5 l-7 3.5 v-13 Z"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Selection</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">ChiSqSelector</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Keeps the most relevant features, ranked by the chi-square (&chi;&sup2;) test of association with the target.</p></div>
# MAGIC <div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="19" width="8" height="10" rx="1.5"/><rect x="12" y="19" width="8" height="10" rx="1.5"/><rect x="22" y="19" width="8" height="10" rx="1.5"/><circle cx="6" cy="24" r="1.6"/><circle cx="16" cy="22.5" r="1.6"/><circle cx="16" cy="26" r="1.6"/><path d="M6 15 v-4 M16 15 v-4 M26 15 v-4"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Locality sensitive hashing</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Approximate Nearest Neighbor Search</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Hashes data points so that similar points are more likely to land in the same bucket, making neighbour lookups cheap at scale.</p></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Classification and Regression
# MAGIC ### `spark.ml.classification` &middot; `spark.ml.regression`
# MAGIC
# MAGIC <div style="max-width: 1180px; margin: 0 auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
# MAGIC <p style="font-size: 1.02em; line-height: 1.65; color: #33414B; margin: 0 0 20px;">
# MAGIC <b>Purpose:</b> scalable, distributed algorithms for two kinds of supervised learning &mdash; <b>classification</b>, which predicts a discrete label, and <b>regression</b>, which predicts a <b>continuous</b> value. Both submodules share the same <code>Estimator</code> / <code>Transformer</code> API, so they drop into a pipeline the same way.
# MAGIC </p>
# MAGIC <div style="display: flex; flex-wrap: wrap; gap: 28px; align-items: flex-start;">
# MAGIC <div style="flex:1 1 440px;min-width:400px;"><div style="border-left:4px solid #FF5F46;padding-left:12px;margin-bottom:14px;"><div style="font-size:17px;font-weight:800;color:#1B3139;line-height:1.3;">Classification</div><code style="font-size:13px;color:#618794;">spark.ml.classification</code></div><div style="box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;margin-bottom:16px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M3 28 V4 M3 28 H29"/><path d="M5 25 C12 25, 13 9, 20 9 C25 9, 26 8, 28 7.5"/><circle cx="8" cy="25" r="1.4"/><circle cx="11" cy="25" r="1.4"/><circle cx="22" cy="9" r="1.4"/><circle cx="25" cy="9" r="1.4"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Linear model</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">LogisticRegression</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Predicts a categorical outcome by modelling the probability that an input belongs to a class. The workhorse for binary classification.</p></div><div style="box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;margin-bottom:16px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="12" y="3" width="8" height="6" rx="1.5"/><rect x="3" y="14" width="8" height="6" rx="1.5"/><rect x="21" y="14" width="8" height="6" rx="1.5"/><rect x="16" y="24" width="8" height="6" rx="1.5"/><path d="M14.5 9 L7 14 M17.5 9 L25 14 M25 20 L21.5 24"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Tree-based</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">DecisionTreeClassifier</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Recursively splits the data into subsets on feature values, producing a tree of decision rules that is easy to inspect.</p></div></div>
# MAGIC <div style="flex:1 1 440px;min-width:400px;"><div style="border-left:4px solid #FF5F46;padding-left:12px;margin-bottom:14px;"><div style="font-size:17px;font-weight:800;color:#1B3139;line-height:1.3;">Regression</div><code style="font-size:13px;color:#618794;">spark.ml.regression</code></div><div style="box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;margin-bottom:16px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M3 28 V4 M3 28 H29"/><path d="M6 25 L27 8"/><circle cx="9" cy="21" r="1.5"/><circle cx="13" cy="20" r="1.5"/><circle cx="17" cy="16" r="1.5"/><circle cx="21" cy="15" r="1.5"/><circle cx="25" cy="10" r="1.5"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Linear model</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">LinearRegression</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Models the relationship between a dependent variable and one or more independent variables with a linear equation.</p></div><div style="box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;margin-bottom:16px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M7 27 v-4 M7 23 l-4 0 M7 23 l4 0 M3 23 v-3 M11 23 v-3"/><path d="M16 29 v-5 M16 24 l-4 0 M16 24 l4 0 M12 24 v-3 M20 24 v-3"/><path d="M25 27 v-4 M25 23 l-4 0 M25 23 l4 0 M21 23 v-3 M29 23 v-3"/><path d="M7 20 v-4 M16 21 v-4 M25 20 v-4"/><circle cx="7" cy="13" r="2.6"/><circle cx="16" cy="14" r="2.6"/><circle cx="25" cy="13" r="2.6"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Ensemble</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">RandomForestRegressor</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Aggregates the predictions of many decision trees, which reduces the variance a single tree would suffer from.</p></div></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Clustering
# MAGIC ### `spark.ml.clustering`
# MAGIC
# MAGIC <div style="max-width: 1180px; margin: 0 auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
# MAGIC <p style="font-size: 1.02em; line-height: 1.65; color: #33414B; margin: 0 0 18px;">
# MAGIC <b>Purpose:</b> group similar data points together <b>without labelled data</b>. Clustering is <b>unsupervised</b> &mdash; it discovers natural groupings in a dataset rather than learning a mapping you supplied.
# MAGIC </p>
# MAGIC <div style="display: flex; flex-wrap: wrap; gap: 16px;">
# MAGIC <div style="flex:1 1 340px;min-width:320px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="10" cy="10" r="7.5" stroke-dasharray="2.5 2.5"/><circle cx="8" cy="8" r="1.4"/><circle cx="12.5" cy="8.5" r="1.4"/><circle cx="10" cy="13" r="1.4"/><circle cx="22" cy="22" r="7.5" stroke-dasharray="2.5 2.5"/><circle cx="20" cy="20" r="1.4"/><circle cx="24.5" cy="20.5" r="1.4"/><circle cx="22" cy="25" r="1.4"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Partitional</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">KMeans</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Partitions the data into <b>K</b> clusters by feature similarity. The default choice for customer segmentation and pattern recognition.</p></div>
# MAGIC <div style="flex:1 1 340px;min-width:320px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="16" cy="5" r="3"/><circle cx="8" cy="16" r="3"/><circle cx="24" cy="16" r="3"/><circle cx="18" cy="27" r="3"/><circle cx="30" cy="27" r="2.4"/><path d="M14 7.5 L10 13.5 M18 7.5 L22 13.5 M22.5 18.5 L19.5 24 M25.5 18.5 L28.8 24.6"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Hierarchical</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">BisectingKMeans</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Repeatedly splits clusters to build a <b>tree</b> of clusters, which makes the grouping easier to interpret and to cut at any depth.</p></div>
# MAGIC </div>
# MAGIC <div style="display: flex; flex-wrap: wrap; gap: 16px; align-items: stretch; margin-top: 20px;">
# MAGIC <div style="flex: 1 1 520px; min-width: 460px; background: #F4F5F6; border: 1px solid #C9CDD1; border-radius: 4px; padding: 14px 16px; overflow-x: auto;">
# MAGIC <div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div>
# MAGIC <pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">from</span> pyspark.ml.clustering <span style="color:#0B5FA5;">import</span> KMeans
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Train a k-means model</span>
# MAGIC kmeans = KMeans().setK(2).setSeed(1)
# MAGIC model = kmeans.fit(dataset)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Show the result</span>
# MAGIC centers = model.clusterCenters()
# MAGIC <span style="color:#0B5FA5;">print</span>(<span style="color:#2E8B57;">&quot;Cluster Centers: &quot;</span>)
# MAGIC <span style="color:#0B5FA5;">for</span> center <span style="color:#0B5FA5;">in</span> centers:
# MAGIC     <span style="color:#0B5FA5;">print</span>(center)</pre>
# MAGIC </div>
# MAGIC <div style="flex: 1 1 300px; min-width: 280px; background: #FFFFFF; border: 1px solid #C9CDD1; border-radius: 4px; padding: 14px 16px; overflow-x: auto;">
# MAGIC <div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Output</div>
# MAGIC <pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;">Cluster Centers:
# MAGIC [5.2, 3.0, 1.2, 0.2]
# MAGIC [6.7, 3.3, 5.7, 2.1]</pre>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Collaborative Filtering
# MAGIC ### `spark.ml.recommendation`
# MAGIC
# MAGIC <div style="max-width: 1180px; margin: 0 auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
# MAGIC <p style="font-size: 1.02em; line-height: 1.65; color: #33414B; margin: 0 0 4px;">
# MAGIC <b>Purpose:</b> predict a user's preferences by learning from the past interactions of <b>similar users or items</b> &mdash; no content features required, only the interaction history.
# MAGIC </p>
# MAGIC <p style="font-size: 1.02em; line-height: 1.65; color: #33414B; margin: 0 0 16px;">
# MAGIC <code>spark.ml</code> uses <b>alternating least squares (ALS)</b>: it factors the sparse ratings matrix into two dense <b>latent factor</b> matrices, alternately holding one fixed while solving for the other.
# MAGIC </p>
# MAGIC <div style="display: flex; flex-wrap: wrap; gap: 24px; align-items: flex-start;">
# MAGIC <div style="flex: 0 1 560px; min-width: 460px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 560 210" role="img" style="display:block;max-width:560px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><title>ALS matrix factorisation</title><desc>The sparse user-by-item ratings matrix is approximated by the product of a user factor matrix and an item factor matrix, each with k latent dimensions.</desc><rect x="24" y="24" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="46" y="24" width="22" height="22" fill="#FF5F46" fill-opacity="0.55" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="68" y="24" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="90" y="24" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="112" y="24" width="22" height="22" fill="#FF5F46" fill-opacity="0.55" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="134" y="24" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="24" y="46" width="22" height="22" fill="#FF5F46" fill-opacity="0.55" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="46" y="46" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="68" y="46" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="90" y="46" width="22" height="22" fill="#FF5F46" fill-opacity="0.55" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="112" y="46" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="134" y="46" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="24" y="68" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="46" y="68" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="68" y="68" width="22" height="22" fill="#FF5F46" fill-opacity="0.55" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="90" y="68" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="112" y="68" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="134" y="68" width="22" height="22" fill="#FF5F46" fill-opacity="0.55" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="24" y="90" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="46" y="90" width="22" height="22" fill="#FF5F46" fill-opacity="0.55" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="68" y="90" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="90" y="90" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="112" y="90" width="22" height="22" fill="#FF5F46" fill-opacity="0.55" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="134" y="90" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="24" y="112" width="22" height="22" fill="#FF5F46" fill-opacity="0.55" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="46" y="112" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="68" y="112" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="90" y="112" width="22" height="22" fill="#FF5F46" fill-opacity="0.55" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="112" y="112" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="134" y="112" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="24" y="134" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="46" y="134" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="68" y="134" width="22" height="22" fill="#FF5F46" fill-opacity="0.55" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="90" y="134" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="112" y="134" width="22" height="22" fill="#FF5F46" fill-opacity="0.55" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="134" y="134" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="24" y="24" width="132" height="132" fill="none" stroke="#1B5162" stroke-width="1.6"/><text x="90" y="176" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026">Ratings</text><text x="90" y="194" text-anchor="middle" font-size="11.5" fill="#618794">users &times; items (sparse)</text><text x="186" y="98" text-anchor="middle" font-size="24" fill="#0b2026">&#8776;</text><rect x="212" y="24" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="234" y="24" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="256" y="24" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="212" y="46" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="234" y="46" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="256" y="46" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="212" y="68" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="234" y="68" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="256" y="68" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="212" y="90" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="234" y="90" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="256" y="90" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="212" y="112" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="234" y="112" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="256" y="112" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="212" y="134" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="234" y="134" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="256" y="134" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="212" y="24" width="66" height="132" fill="none" stroke="#1B5162" stroke-width="1.6"/><text x="245" y="176" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026">User factors</text><text x="245" y="194" text-anchor="middle" font-size="11.5" fill="#618794">users &times; k</text><text x="304" y="98" text-anchor="middle" font-size="22" fill="#0b2026">&times;</text><rect x="330" y="57" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="352" y="57" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="374" y="57" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="396" y="57" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="418" y="57" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="440" y="57" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="330" y="79" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="352" y="79" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="374" y="79" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="396" y="79" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="418" y="79" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="440" y="79" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="330" y="101" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="352" y="101" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="374" y="101" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="396" y="101" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="418" y="101" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="440" y="101" width="22" height="22" fill="#FFFFFF" fill-opacity="1" stroke="#1B5162" stroke-width="0.9" stroke-opacity="0.55"/><rect x="330" y="57" width="132" height="66" fill="none" stroke="#1B5162" stroke-width="1.6"/><text x="396" y="176" text-anchor="middle" font-size="14" font-weight="700" fill="#0b2026">Item factors</text><text x="396" y="194" text-anchor="middle" font-size="11.5" fill="#618794">k &times; items</text></svg>
# MAGIC </div>
# MAGIC <div style="flex: 1 1 380px; min-width: 340px; display: flex; flex-wrap: wrap; gap: 14px;">
# MAGIC <div style="flex:1 1 320px;min-width:300px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:14px 16px 16px;"><div style="display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;"><code style="font-size:15px;font-weight:700;color:#0b2026;">maxIter</code><span style="font-size:11px;font-weight:700;letter-spacing:0.6px;text-transform:uppercase;color:#FF5F46;">default 10</span></div><p style="margin:9px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">The maximum number of alternating passes to run. More iterations refine the factors at the cost of runtime.</p></div>
# MAGIC <div style="flex:1 1 320px;min-width:300px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:14px 16px 16px;"><div style="display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;"><code style="font-size:15px;font-weight:700;color:#0b2026;">regParam</code><span style="font-size:11px;font-weight:700;letter-spacing:0.6px;text-transform:uppercase;color:#FF5F46;">default 1.0</span></div><p style="margin:9px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Regularization strength, balancing fit against simplicity. A <b>higher</b> value yields a simpler model that is less prone to overfitting.</p></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div style="margin-top: 20px; background: #F4F5F6; border: 1px solid #C9CDD1; border-radius: 4px; padding: 14px 16px; overflow-x: auto;">
# MAGIC <div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div>
# MAGIC <pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">from</span> pyspark.ml.recommendation <span style="color:#0B5FA5;">import</span> ALS
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Build the recommendation model using ALS on the training data</span>
# MAGIC als = ALS(maxIter=5, regParam=0.01, userCol=<span style="color:#2E8B57;">&quot;userId&quot;</span>,
# MAGIC           itemCol=<span style="color:#2E8B57;">&quot;movieId&quot;</span>, ratingCol=<span style="color:#2E8B57;">&quot;rating&quot;</span>)
# MAGIC model = als.fit(training)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Generate top 10 movie recommendations for each user</span>
# MAGIC userRecs = model.recommendForAllUsers(10)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Generate top 10 user recommendations for a specified set of movies</span>
# MAGIC movies = ratings.select(als.getItemCol()).distinct().limit(3)
# MAGIC movieSubSetRecs = model.recommendForItemSubset(movies, 10)</pre>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Frequent Pattern Mining
# MAGIC ### `spark.ml.fpm`
# MAGIC
# MAGIC <div style="max-width: 1180px; margin: 0 auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
# MAGIC <p style="font-size: 1.02em; line-height: 1.65; color: #33414B; margin: 0 0 18px;">
# MAGIC <b>Purpose:</b> identify recurring <b>patterns, associations and correlations</b> in large datasets. The two algorithms differ in one thing: whether the <b>order</b> of items matters.
# MAGIC </p>
# MAGIC <div style="display: flex; flex-wrap: wrap; gap: 16px;">
# MAGIC <div style="flex:1 1 340px;min-width:320px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="16" cy="5" r="2.6"/><circle cx="8" cy="15" r="2.6"/><circle cx="24" cy="15" r="2.6"/><circle cx="4" cy="26" r="2.4"/><circle cx="13" cy="26" r="2.4"/><circle cx="24" cy="26" r="2.4"/><path d="M14.2 7 L9.7 12.9 M17.8 7 L22.3 12.9 M6.7 17.2 L5.2 23.6 M9.6 17.2 L11.7 23.6 M24 17.6 V23.6"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Itemset mining &middot; order-free</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">FP-Growth</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Compresses the input into a prefix tree (<b>FP-tree</b>), then recursively mines the frequent patterns from it &mdash; no candidate generation needed.</p><div style="margin-top:12px;border-left:3px solid #FF5F46;padding:2px 0 2px 11px;font-size:13.5px;line-height:1.55;color:#618794;"><b style="color:#FF5F46;">Scenario</b> &mdash; retail &mdash; which items customers frequently buy <b>together</b> in one basket.</div></div>
# MAGIC <div style="flex:1 1 340px;min-width:320px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="12" width="8" height="8" rx="1.6"/><rect x="12" y="12" width="8" height="8" rx="1.6"/><rect x="22" y="12" width="8" height="8" rx="1.6"/><path d="M10.4 16 h1.2 M20.4 16 h1.2"/><path d="M6 9 V5 M16 9 V5 M26 9 V5"/><path d="M2 27 h28 M27 24.5 l3 2.5 l-3 2.5"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Sequential mining &middot; order matters</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">PrefixSpan</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Finds frequent <b>sequences</b> of items in transactional data, respecting the order in which they occurred.</p><div style="margin-top:12px;border-left:3px solid #FF5F46;padding:2px 0 2px 11px;font-size:13.5px;line-height:1.55;color:#618794;"><b style="color:#FF5F46;">Scenario</b> &mdash; purchasing behaviour <b>over time</b>, where what came first changes the meaning.</div></div>
# MAGIC </div>
# MAGIC <div style="margin-top:20px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">from</span> pyspark.ml.fpm <span style="color:#0B5FA5;">import</span> FPGrowth
# MAGIC &#8203;
# MAGIC df = spark.createDataFrame([
# MAGIC     (0, [1, 2, 5]), (1, [1, 2, 3, 5]), (2, [1, 2])], [<span style="color:#2E8B57;">&quot;id&quot;</span>, <span style="color:#2E8B57;">&quot;items&quot;</span>])
# MAGIC &#8203;
# MAGIC fpGrowth = FPGrowth(itemsCol=<span style="color:#2E8B57;">&quot;items&quot;</span>, minSupport=0.5, minConfidence=0.6)
# MAGIC model = fpGrowth.fit(df)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Display frequent itemsets</span>
# MAGIC model.freqItemsets.show()
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># transform examines the input items against all association rules</span>
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># and summarizes the consequents as prediction</span>
# MAGIC model.transform(df).show()</pre></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ML Tuning: Model Selection and Hyperparameter Tuning
# MAGIC ### `spark.ml.evaluation` &middot; `spark.ml.tuning`
# MAGIC
# MAGIC <div style="max-width: 1180px; margin: 0 auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
# MAGIC <p style="font-size: 1.02em; line-height: 1.65; color: #33414B; margin: 0 0 6px;">
# MAGIC <b>Purpose:</b> <b>model selection</b> and hyperparameter optimization. You supply an estimator, a grid of parameters, and an <b>evaluator</b>; the tuner searches the grid and keeps the best model.
# MAGIC </p>
# MAGIC <p style="font-size: 1.02em; line-height: 1.65; color: #33414B; margin: 0 0 18px;">
# MAGIC The two search strategies differ in how carefully they estimate performance &mdash; and in how much compute that costs.
# MAGIC </p>
# MAGIC <div style="overflow-x: auto; margin-bottom: 22px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 800 190" role="img" style="display:block;min-width:700px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><title>Cross-validation versus a single train/validation split</title><desc>CrossValidator holds out each of k folds in turn and trains k models; TrainValidationSplit makes one train and validation split and trains a single model.</desc><text x="16" y="20" font-size="13" font-weight="700" fill="#0b2026">CrossValidator &mdash; k-fold (k=4)</text><rect x="16" y="34" width="86" height="26" rx="3" fill="#FF5F46" fill-opacity="0.85"/><rect x="106" y="34" width="86" height="26" rx="3" fill="#E3E7E9" fill-opacity="1"/><rect x="196" y="34" width="86" height="26" rx="3" fill="#E3E7E9" fill-opacity="1"/><rect x="286" y="34" width="86" height="26" rx="3" fill="#E3E7E9" fill-opacity="1"/><text x="386" y="52" font-size="12" fill="#618794">model 1</text><rect x="16" y="66" width="86" height="26" rx="3" fill="#E3E7E9" fill-opacity="1"/><rect x="106" y="66" width="86" height="26" rx="3" fill="#FF5F46" fill-opacity="0.85"/><rect x="196" y="66" width="86" height="26" rx="3" fill="#E3E7E9" fill-opacity="1"/><rect x="286" y="66" width="86" height="26" rx="3" fill="#E3E7E9" fill-opacity="1"/><text x="386" y="84" font-size="12" fill="#618794">model 2</text><rect x="16" y="98" width="86" height="26" rx="3" fill="#E3E7E9" fill-opacity="1"/><rect x="106" y="98" width="86" height="26" rx="3" fill="#E3E7E9" fill-opacity="1"/><rect x="196" y="98" width="86" height="26" rx="3" fill="#FF5F46" fill-opacity="0.85"/><rect x="286" y="98" width="86" height="26" rx="3" fill="#E3E7E9" fill-opacity="1"/><text x="386" y="116" font-size="12" fill="#618794">model 3</text><rect x="16" y="130" width="86" height="26" rx="3" fill="#E3E7E9" fill-opacity="1"/><rect x="106" y="130" width="86" height="26" rx="3" fill="#E3E7E9" fill-opacity="1"/><rect x="196" y="130" width="86" height="26" rx="3" fill="#E3E7E9" fill-opacity="1"/><rect x="286" y="130" width="86" height="26" rx="3" fill="#FF5F46" fill-opacity="0.85"/><text x="386" y="148" font-size="12" fill="#618794">model 4</text><text x="16" y="178" font-size="12" fill="#618794">every fold is held out once &mdash; <tspan font-weight="700">k models trained</tspan></text><text x="470" y="20" font-size="13" font-weight="700" fill="#0b2026">TrainValidationSplit</text><rect x="470" y="34" width="222" height="26" rx="3" fill="#E3E7E9"/><rect x="696" y="34" width="74" height="26" rx="3" fill="#FF5F46" fill-opacity="0.85"/><text x="581" y="52" text-anchor="middle" font-size="12" fill="#0b2026">train (75%)</text><text x="733" y="52" text-anchor="middle" font-size="12" fill="#FFFFFF">valid.</text><text x="470" y="86" font-size="12" fill="#618794">one split, evaluated once &mdash; <tspan font-weight="700">1 model trained</tspan></text><text x="470" y="108" font-size="12" fill="#618794">faster, but the estimate depends on which rows</text><text x="470" y="126" font-size="12" fill="#618794">happened to land in the validation set.</text></svg>
# MAGIC </div>
# MAGIC <div style="display: flex; flex-wrap: wrap; gap: 16px;">
# MAGIC <div style="flex:1 1 360px;min-width:320px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M4 28 V4 M4 28 H29"/><path d="M4 28 C 9 12, 17 7, 28 6"/><path d="M4 28 L28 6" stroke-dasharray="2.5 2.5" stroke-opacity="0.6"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Evaluator &middot; what &ldquo;best&rdquo; means</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">BinaryClassificationEvaluator</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Scores binary classifiers on <b>area under the ROC curve (AUC)</b> or <b>area under the precision-recall curve (AUPRC)</b>. The tuner optimizes whichever metric you pick here.</p></div>
# MAGIC <div style="flex:1 1 300px;min-width:320px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="6" width="26" height="5" rx="1.4"/><rect x="3" y="14" width="26" height="5" rx="1.4"/><rect x="3" y="22" width="26" height="5" rx="1.4"/><rect x="3" y="6" width="7" height="5" rx="1.4" fill="#FF5F46" stroke="none"/><rect x="10" y="14" width="7" height="5" rx="1.4" fill="#FF5F46" stroke="none"/><rect x="17" y="22" width="7" height="5" rx="1.4" fill="#FF5F46" stroke="none"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Search &middot; k-fold</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">CrossValidator</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Splits the data into <b>k</b> folds, holds each out in turn, and averages the k scores. The most reliable estimate &mdash; and <b>k&times;</b> the training cost.</p></div>
# MAGIC <div style="flex:1 1 300px;min-width:320px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="12" width="26" height="8" rx="1.6"/><rect x="22" y="12" width="7" height="8" rx="1.6" fill="#FF5F46" stroke="none"/><path d="M22 8 V5 M22 27 V24"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Search &middot; single split</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">TrainValidationSplit</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Evaluates each parameter combination against <b>one</b> train/validation split. Much cheaper than k-fold, but a noisier estimate.</p></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background:#0B2027;border-radius:8px;padding:46px 24px;text-align:center;margin:6px 0;">
# MAGIC   <span style="color:#ffffff;font-size:30px;font-weight:800;letter-spacing:.3px;">Data Preparation &amp; Feature Engineering with Spark</span>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Model Development Scenario
# MAGIC
# MAGIC <div style="background:#EDEDED;border-radius:6px;padding:10px 16px;font-size:0.95em;">
# MAGIC &#8226; <b>6.3 million</b> animals enter U.S. shelters annually &#8226; <b>920,000</b> shelter animals are euthanized each year &#8226; <b>4.1 million</b> shelter animals are adopted annually. <i>(ASPCA)</i>
# MAGIC </div>
# MAGIC <div style="border:1px solid #1B3139;border-radius:4px;padding:10px 16px;margin:10px 0;font-size:1.05em;">
# MAGIC <b><u>PawMatch:</u></b> An animal shelter dedicated to finding forever homes for pets.
# MAGIC </div>
# MAGIC <div style="max-width:1320px;margin:0 auto;">
# MAGIC <svg width="100%" viewBox="0 0 1320 250" role="img" style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC <title>Machine learning lifecycle</title><desc>The PawMatch machine learning lifecycle from business problem through model monitoring, with the current stage highlighted.</desc>
# MAGIC <defs><marker id="lc-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-linecap="round"/></marker><marker id="lc-fb" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B5162" stroke-width="1.6" stroke-linecap="round"/></marker></defs>
# MAGIC <rect x="8" y="44" width="142" height="120" rx="10" fill="#1B3139" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="79" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">Business</text>
# MAGIC <text x="79" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">Problem</text>
# MAGIC <path d="M153 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="174" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="245" y="84" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Define</text>
# MAGIC <text x="245" y="104" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Success</text>
# MAGIC <text x="245" y="124" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Criteria</text>
# MAGIC <path d="M319 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="340" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="411" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Data</text>
# MAGIC <text x="411" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Collection</text>
# MAGIC <path d="M485 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="506" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="577" y="74" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Data</text>
# MAGIC <text x="577" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Preprocessing/</text>
# MAGIC <text x="577" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Feature</text>
# MAGIC <text x="577" y="134" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Engineering</text>
# MAGIC <path d="M651 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="672" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="743" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model Training</text>
# MAGIC <text x="743" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">& Tuning</text>
# MAGIC <path d="M817 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="838" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="909" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="909" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Evaluation</text>
# MAGIC <path d="M983 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="1004" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="1075" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="1075" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Deployment</text>
# MAGIC <path d="M909 164 C 869 219, 617 219, 577 164" stroke="#1B5162" stroke-width="1.8" fill="none" stroke-dasharray="7 5" marker-end="url(#lc-fb)"/>
# MAGIC <path d="M909 164 C 829 254, 451 254, 411 164" stroke="#1B5162" stroke-width="1.8" fill="none" stroke-dasharray="7 5" marker-end="url(#lc-fb)"/>
# MAGIC </svg>
# MAGIC </div>
# MAGIC <div style="background:#1B5162;color:#fff;border-radius:4px;padding:12px 16px;margin:10px 0;">
# MAGIC <b>Business Problem:</b> PawMatch aims to <u>increase adoption rates</u> by predicting which animals are <u>at risk of longer shelter stays</u> to allow for <u>targeted interventions</u>.
# MAGIC </div>
# MAGIC
# MAGIC <table><colgroup><col style="width:50%"><col style="width:50%"></colgroup><tr>
# MAGIC <td style="vertical-align:top;padding-right:12px;">
# MAGIC <b>Example Success Criteria:</b>
# MAGIC <ul><li><b>Increased Adoption Rates:</b> more animals adopted within a shorter timeframe.</li>
# MAGIC <li><b>Shorter Shelter Stays:</b> fewer average days in the shelter before adoption.</li></ul>
# MAGIC </td>
# MAGIC <td style="vertical-align:top;padding-left:12px;">
# MAGIC <b>Example Data Collection:</b>
# MAGIC <ul><li><b>Animal Demographics:</b> age, breed, size, color, gender.</li>
# MAGIC <li><b>Behavioral Assessments:</b> temperament evaluations.</li>
# MAGIC <li><b>Shelter History:</b> length of stay, adoption attempts.</li>
# MAGIC <li><b>Health Information:</b> medical history, spay/neuter status.</li></ul>
# MAGIC </td></tr></table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Data Preprocessing &amp; Feature Engineering
# MAGIC
# MAGIC <div style="max-width:1320px;margin:0 auto;">
# MAGIC <svg width="100%" viewBox="0 0 1320 250" role="img" style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC <title>Machine learning lifecycle</title><desc>The PawMatch machine learning lifecycle from business problem through model monitoring, with the current stage highlighted.</desc>
# MAGIC <defs><marker id="lc-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-linecap="round"/></marker><marker id="lc-fb" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B5162" stroke-width="1.6" stroke-linecap="round"/></marker></defs>
# MAGIC <rect x="8" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="79" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Business</text>
# MAGIC <text x="79" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Problem</text>
# MAGIC <path d="M153 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="174" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="245" y="84" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Define</text>
# MAGIC <text x="245" y="104" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Success</text>
# MAGIC <text x="245" y="124" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Criteria</text>
# MAGIC <path d="M319 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="340" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="411" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Data</text>
# MAGIC <text x="411" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Collection</text>
# MAGIC <path d="M485 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="506" y="44" width="142" height="120" rx="10" fill="#1B3139" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="577" y="74" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">Data</text>
# MAGIC <text x="577" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">Preprocessing/</text>
# MAGIC <text x="577" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">Feature</text>
# MAGIC <text x="577" y="134" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">Engineering</text>
# MAGIC <path d="M651 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="672" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="743" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model Training</text>
# MAGIC <text x="743" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">& Tuning</text>
# MAGIC <path d="M817 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="838" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="909" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="909" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Evaluation</text>
# MAGIC <path d="M983 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="1004" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="1075" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="1075" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Deployment</text>
# MAGIC <path d="M1149 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="1170" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="1241" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="1241" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Monitoring</text>
# MAGIC <path d="M909 164 C 869 219, 617 219, 577 164" stroke="#1B5162" stroke-width="1.8" fill="none" stroke-dasharray="7 5" marker-end="url(#lc-fb)"/>
# MAGIC <path d="M909 164 C 829 254, 451 254, 411 164" stroke="#1B5162" stroke-width="1.8" fill="none" stroke-dasharray="7 5" marker-end="url(#lc-fb)"/>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC - **Objective:** identify the most effective data preprocessing and model training techniques for predicting animal adoption likelihood.
# MAGIC - **Action — Select Transformers:** choose appropriate transformers for preprocessing the data, considering categorical and continuous features.
# MAGIC
# MAGIC <div style="background:#1B5162;color:#fff;border-radius:4px;padding:10px 16px;margin-top:10px;text-align:center;font-weight:700;">What Transformers Might be Useful?</div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Evaluating Preprocessing Methods
# MAGIC ### Compare and contrast Spark ML Transformers and Estimators
# MAGIC
# MAGIC <div style="overflow-x: auto; margin: 16px 0;">
# MAGIC <div style="min-width: 900px; max-width: 1300px; margin: 0 auto; display: flex; gap: 3%; align-items: flex-start; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
# MAGIC <div style="flex: 0 0 48.5%; max-width: 48.5%;">
# MAGIC <div style="margin-bottom:30px;"><div style="text-align:center;font-size:21px;font-weight:800;color:#1B3139;">StringIndexer</div><div style="text-align:center;font-size:14.5px;line-height:1.45;color:#33414B;margin:6px 0 12px;">Converts categorical features such as breed, color, and gender into numerical indices.</div><div style="position:relative;"><div style="background:#EEF0F1;border:1px solid #C9CDD1;padding:10px 12px 30px;overflow-x:auto;"><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12px;line-height:1.5;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">from</span> pyspark.ml.feature <span style="color:#0B5FA5;">import</span> StringIndexer
# MAGIC indexer = StringIndexer(inputCol=<span style="color:#2E8B57;">&quot;color&quot;</span>, outputCol=<span style="color:#2E8B57;">&quot;colorIndex&quot;</span>)
# MAGIC indexed = indexer.fit(df).transform(df)</pre></div><div style="width:84%;margin-left:auto;margin-top:-20px;position:relative;background:#FFFFFF;border:1px solid #C9CDD1;box-shadow:0 1px 4px rgba(0,0,0,0.10);padding:8px 10px;overflow-x:auto;"><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:11.5px;line-height:1.45;color:#1C2733;white-space:pre;">+----------+-------+------------+-----+--------+---------+
# MAGIC | AnimalID | color | colorIndex | Age | Weight | Adopted |
# MAGIC +----------+-------+------------+-----+--------+---------+
# MAGIC |        1 | Black |        0.0 |   3 |     30 |       1 |
# MAGIC |        2 | Brown |        1.0 |   5 |     10 |       0 |
# MAGIC |        3 | White |        2.0 |   2 |     20 |       1 |
# MAGIC +----------+-------+------------+-----+--------+---------+</pre></div></div></div><div style="margin-bottom:30px;"><div style="text-align:center;font-size:21px;font-weight:800;color:#1B3139;">StandardScaler</div><div style="text-align:center;font-size:14.5px;line-height:1.45;color:#33414B;margin:6px 0 12px;">Normalizes continuous features like age, weight, and length of stay to have a mean of 0 and a standard deviation of 1.</div><div style="position:relative;"><div style="background:#EEF0F1;border:1px solid #C9CDD1;padding:10px 12px 30px;overflow-x:auto;"><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12px;line-height:1.5;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">from</span> pyspark.ml.feature <span style="color:#0B5FA5;">import</span> StandardScaler
# MAGIC scaler = StandardScaler(inputCol=<span style="color:#2E8B57;">&quot;age&quot;</span>, outputCol=<span style="color:#2E8B57;">&quot;scaledAge&quot;</span>,
# MAGIC                         withMean=<span style="color:#0B5FA5;">True</span>, withStd=<span style="color:#0B5FA5;">True</span>)
# MAGIC scaled = scaler.fit(df).transform(df)</pre></div><div style="width:84%;margin-left:auto;margin-top:-20px;position:relative;background:#FFFFFF;border:1px solid #C9CDD1;box-shadow:0 1px 4px rgba(0,0,0,0.10);padding:8px 10px;overflow-x:auto;"><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:11.5px;line-height:1.45;color:#1C2733;white-space:pre;">+----------+-----+-----------+
# MAGIC | AnimalID | Age | scaledAge |
# MAGIC +----------+-----+-----------+
# MAGIC |        1 |   3 |      -0.5 |
# MAGIC |        2 |   5 |       1.0 |
# MAGIC |        3 |   2 |      -1.5 |
# MAGIC +----------+-----+-----------+</pre></div></div></div>
# MAGIC </div>
# MAGIC <div style="flex: 0 0 48.5%; max-width: 48.5%;">
# MAGIC <div style="margin-bottom:30px;"><div style="text-align:center;font-size:21px;font-weight:800;color:#1B3139;">Imputer</div><div style="text-align:center;font-size:14.5px;line-height:1.45;color:#33414B;margin:6px 0 12px;">Handles missing values by imputing them with mean, median, or mode values.</div><div style="position:relative;"><div style="background:#EEF0F1;border:1px solid #C9CDD1;padding:10px 12px 30px;overflow-x:auto;"><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12px;line-height:1.5;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">from</span> pyspark.ml.feature <span style="color:#0B5FA5;">import</span> Imputer
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Fill missing values in the &quot;Age&quot; column</span>
# MAGIC imputer = Imputer(inputCols=[<span style="color:#2E8B57;">&quot;Age&quot;</span>],
# MAGIC                   outputCols=[<span style="color:#2E8B57;">&quot;imputedAge&quot;</span>]).setStrategy(<span style="color:#2E8B57;">&quot;mean&quot;</span>)
# MAGIC imputed_df = imputer.fit(df).transform(df)</pre></div><div style="width:84%;margin-left:auto;margin-top:-20px;position:relative;background:#FFFFFF;border:1px solid #C9CDD1;box-shadow:0 1px 4px rgba(0,0,0,0.10);padding:8px 10px;overflow-x:auto;"><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:11.5px;line-height:1.45;color:#1C2733;white-space:pre;">+----------+------+------------+
# MAGIC | AnimalID |  Age | imputedAge |
# MAGIC +----------+------+------------+
# MAGIC |        1 |    3 |        3.0 |
# MAGIC |        2 | None |        2.5 |
# MAGIC |        3 |    2 |        2.0 |
# MAGIC +----------+------+------------+</pre></div></div></div><div style="margin-bottom:30px;"><div style="text-align:center;font-size:21px;font-weight:800;color:#1B3139;">OneHotEncoder</div><div style="text-align:center;font-size:14.5px;line-height:1.45;color:#33414B;margin:6px 0 12px;">Encodes categorical features as binary vectors, avoiding ordinality.</div><div style="position:relative;"><div style="background:#EEF0F1;border:1px solid #C9CDD1;padding:10px 12px 30px;overflow-x:auto;"><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12px;line-height:1.5;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">from</span> pyspark.ml.feature <span style="color:#0B5FA5;">import</span> StringIndexer, OneHotEncoder
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># StringIndexer and OneHotEncoder</span>
# MAGIC indexer = StringIndexer(inputCol=<span style="color:#2E8B57;">&quot;AnimalType&quot;</span>,
# MAGIC                         outputCol=<span style="color:#2E8B57;">&quot;AnimalTypeIndex&quot;</span>)
# MAGIC encoder = OneHotEncoder(inputCol=<span style="color:#2E8B57;">&quot;AnimalTypeIndex&quot;</span>,
# MAGIC                         outputCol=<span style="color:#2E8B57;">&quot;AnimalTypeVec&quot;</span>)
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Apply transformations</span>
# MAGIC df_indexed = indexer.fit(df).transform(df)
# MAGIC df_encoded = encoder.fit(df_indexed).transform(df_indexed)</pre></div><div style="width:84%;margin-left:auto;margin-top:-20px;position:relative;background:#FFFFFF;border:1px solid #C9CDD1;box-shadow:0 1px 4px rgba(0,0,0,0.10);padding:8px 10px;overflow-x:auto;"><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:11.5px;line-height:1.45;color:#1C2733;white-space:pre;">+----------+------------+---------------+
# MAGIC | AnimalID | AnimalType | AnimalTypeVec |
# MAGIC +----------+------------+---------------+
# MAGIC |        1 | Dog        |     (1,[],[]) |
# MAGIC |        2 | Cat        | (1,[0],[1.0]) |
# MAGIC |        3 | Dog        |     (1,[],[]) |
# MAGIC +----------+------------+---------------+</pre></div></div></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## VectorAssembler
# MAGIC ### Assembling features into a single vector column
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;">
# MAGIC <b>Purpose:</b> most Spark ML algorithms expect every predictor in <b>one vector column</b>. <code>VectorAssembler</code> is the transformer that merges several individual columns (e.g. age, weight, breed) into a single <code>features</code> vector &mdash; the standard last step before training.
# MAGIC </p>
# MAGIC <div style="overflow-x:auto;margin-bottom:22px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1040 200" role="img" style="display:block;min-width:860px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><title>VectorAssembler combines feature columns into one vector column</title><desc>Three separate columns Age, Weight and BreedIndex are fed into VectorAssembler, which outputs a single features column holding one dense vector per row.</desc><defs><marker id="va-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></marker></defs>
# MAGIC <text x="20" y="24" font-size="12" font-weight="700" letter-spacing="0.6px" text-transform="uppercase" fill="#618794">FEATURE COLUMNS</text>
# MAGIC <g font-family="ui-monospace,SFMono-Regular,Menlo,Consolas,monospace">
# MAGIC <rect x="20" y="34" width="84" height="28" fill="#1B5162"/><text x="62" y="53" text-anchor="middle" font-size="13" font-weight="700" fill="#FFFFFF">Age</text>
# MAGIC <rect x="20" y="62" width="84" height="30" fill="#FFFFFF" stroke="#C9CDD1"/><text x="62" y="82" text-anchor="middle" font-size="13" fill="#1C2733">3</text>
# MAGIC <rect x="20" y="92" width="84" height="30" fill="#FFFFFF" stroke="#C9CDD1"/><text x="62" y="112" text-anchor="middle" font-size="13" fill="#1C2733">5</text>
# MAGIC <rect x="20" y="122" width="84" height="30" fill="#FFFFFF" stroke="#C9CDD1"/><text x="62" y="142" text-anchor="middle" font-size="13" fill="#1C2733">2</text>
# MAGIC <rect x="108" y="34" width="84" height="28" fill="#1B5162"/><text x="150" y="53" text-anchor="middle" font-size="13" font-weight="700" fill="#FFFFFF">Weight</text>
# MAGIC <rect x="108" y="62" width="84" height="30" fill="#FFFFFF" stroke="#C9CDD1"/><text x="150" y="82" text-anchor="middle" font-size="13" fill="#1C2733">30</text>
# MAGIC <rect x="108" y="92" width="84" height="30" fill="#FFFFFF" stroke="#C9CDD1"/><text x="150" y="112" text-anchor="middle" font-size="13" fill="#1C2733">10</text>
# MAGIC <rect x="108" y="122" width="84" height="30" fill="#FFFFFF" stroke="#C9CDD1"/><text x="150" y="142" text-anchor="middle" font-size="13" fill="#1C2733">20</text>
# MAGIC <rect x="196" y="34" width="118" height="28" fill="#1B5162"/><text x="255" y="53" text-anchor="middle" font-size="13" font-weight="700" fill="#FFFFFF">BreedIndex</text>
# MAGIC <rect x="196" y="62" width="118" height="30" fill="#FFFFFF" stroke="#C9CDD1"/><text x="255" y="82" text-anchor="middle" font-size="13" fill="#1C2733">0.0</text>
# MAGIC <rect x="196" y="92" width="118" height="30" fill="#FFFFFF" stroke="#C9CDD1"/><text x="255" y="112" text-anchor="middle" font-size="13" fill="#1C2733">1.0</text>
# MAGIC <rect x="196" y="122" width="118" height="30" fill="#FFFFFF" stroke="#C9CDD1"/><text x="255" y="142" text-anchor="middle" font-size="13" fill="#1C2733">0.0</text>
# MAGIC </g>
# MAGIC <g stroke="#1B3139" stroke-width="1.8" fill="none">
# MAGIC <path d="M318 92 C 372 92, 380 101, 424 104" marker-end="url(#va-ar)"/>
# MAGIC <path d="M318 107 H 424" marker-end="url(#va-ar)"/>
# MAGIC <path d="M318 122 C 372 122, 380 113, 424 110" marker-end="url(#va-ar)"/>
# MAGIC </g>
# MAGIC <rect x="428" y="79" width="200" height="58" rx="10" fill="#FF5F46"/><text x="528" y="105" text-anchor="middle" font-size="16" font-weight="700" fill="#FFFFFF">VectorAssembler</text><text x="528" y="124" text-anchor="middle" font-size="12" font-style="italic" fill="#FFE7E1">transformer</text>
# MAGIC <path d="M628 107 H 704" stroke="#1B3139" stroke-width="1.8" fill="none" marker-end="url(#va-ar)"/>
# MAGIC <text x="712" y="24" font-size="12" font-weight="700" letter-spacing="0.6px" fill="#618794">ONE VECTOR COLUMN</text>
# MAGIC <g font-family="ui-monospace,SFMono-Regular,Menlo,Consolas,monospace">
# MAGIC <rect x="712" y="34" width="300" height="28" fill="#1B3139"/><text x="862" y="53" text-anchor="middle" font-size="13" font-weight="700" fill="#FFFFFF">features</text>
# MAGIC <rect x="712" y="62" width="300" height="30" fill="#FFFFFF" stroke="#C9CDD1"/><text x="862" y="82" text-anchor="middle" font-size="13" fill="#1C2733">[3.0, 30.0, 0.0]</text>
# MAGIC <rect x="712" y="92" width="300" height="30" fill="#FFFFFF" stroke="#C9CDD1"/><text x="862" y="112" text-anchor="middle" font-size="13" fill="#1C2733">[5.0, 10.0, 1.0]</text>
# MAGIC <rect x="712" y="122" width="300" height="30" fill="#FFFFFF" stroke="#C9CDD1"/><text x="862" y="142" text-anchor="middle" font-size="13" fill="#1C2733">[2.0, 20.0, 0.0]</text>
# MAGIC </g>
# MAGIC <text x="167" y="176" text-anchor="middle" font-size="12.5" fill="#618794">3 separate columns</text>
# MAGIC <text x="862" y="176" text-anchor="middle" font-size="12.5" fill="#618794">1 dense vector per row</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;">
# MAGIC <div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9 h7 M3 16 h7 M3 23 h7"/><path d="M12.5 16 h3.5 M14.4 14 l2 2 l-2 2"/><rect x="20" y="7" width="9" height="18" rx="2"/><path d="M24.5 10 v12"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Algorithm requirement</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">One vector column</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Most Spark ML estimators expect all predictors in a single <code>features</code> vector rather than separate columns.</p></div>
# MAGIC <div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="7" y="10" width="18" height="12" rx="2"/><rect x="12" y="14" width="8" height="4" rx="1"/><path d="M11 10 v-3 M16 10 v-3 M21 10 v-3 M11 22 v3 M16 22 v3 M21 22 v3"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Memory-optimized</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Efficient representation</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Represents each feature row compactly, so large datasets stay memory-efficient across the cluster.</p></div>
# MAGIC <div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="5" width="22" height="22" rx="2"/><path d="M5 12.3 h22 M5 19.6 h22 M12.3 5 v22 M19.6 5 v22"/></g><g fill="#FF5F46" stroke="none"><rect x="5.6" y="5.6" width="6" height="6" rx="1"/><rect x="20.2" y="12.9" width="6" height="6" rx="1"/><rect x="12.9" y="20.2" width="6" height="6" rx="1"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Dense or sparse</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Chosen automatically</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Emits a dense or sparse vector per row &mdash; whichever uses less memory for that row's values.</p></div>
# MAGIC </div>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:20px;">
# MAGIC <div style="flex:1 1 540px;min-width:460px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">from</span> pyspark.ml.feature <span style="color:#0B5FA5;">import</span> VectorAssembler
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Combine separate feature columns into one vector</span>
# MAGIC assembler = VectorAssembler(inputCols=[<span style="color:#2E8B57;">&quot;Age&quot;</span>, <span style="color:#2E8B57;">&quot;Weight&quot;</span>, <span style="color:#2E8B57;">&quot;BreedIndex&quot;</span>],
# MAGIC                             outputCol=<span style="color:#2E8B57;">&quot;features&quot;</span>)
# MAGIC df_assembled = assembler.transform(df)
# MAGIC &#8203;
# MAGIC df_assembled.select(<span style="color:#2E8B57;">&quot;AnimalID&quot;</span>, <span style="color:#2E8B57;">&quot;Age&quot;</span>, <span style="color:#2E8B57;">&quot;Weight&quot;</span>, <span style="color:#2E8B57;">&quot;BreedIndex&quot;</span>, <span style="color:#2E8B57;">&quot;features&quot;</span>).show()</pre></div>
# MAGIC <div style="flex:1 1 360px;min-width:340px;background:#FFFFFF;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Output</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12.5px;line-height:1.5;color:#1C2733;white-space:pre;">+--------+---+------+----------+----------------+
# MAGIC |AnimalID|Age|Weight|BreedIndex|features        |
# MAGIC +--------+---+------+----------+----------------+
# MAGIC |       1|  3|    30|       0.0|[3.0, 30.0, 0.0]|
# MAGIC |       2|  5|    10|       1.0|[5.0, 10.0, 1.0]|
# MAGIC |       3|  2|    20|       0.0|[2.0, 20.0, 0.0]|
# MAGIC +--------+---+------+----------+----------------+</pre></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Reproducible Train-Test Split
# MAGIC ### Consistent, repeatable evaluation with a fixed seed
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> split the dataset into training, validation and test sets so that the split is <b>identical on every run</b>. Spark ML's <code>randomSplit</code> with a fixed <code>seed</code> makes evaluation reproducible &mdash; essential for comparing models over time.</p>
# MAGIC <div style="overflow-x:auto;margin-bottom:20px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1000 120" role="img" style="display:block;min-width:720px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><title>Reproducible train / validation / test split</title><desc>A fixed seed splits the dataset into 60 percent train, 20 percent validation and 20 percent test, identically on every run.</desc>
# MAGIC <text x="20" y="26" font-size="12" font-weight="700" letter-spacing="0.6px" fill="#618794">DATASET &#8212; split with a fixed seed</text>
# MAGIC <rect x="20" y="40" width="576" height="46" rx="4" fill="#1B5162"/><text x="308" y="69" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Train &#8212; 60%</text>
# MAGIC <rect x="600" y="40" width="192" height="46" rx="4" fill="#5F8797"/><text x="696" y="69" text-anchor="middle" font-size="14" font-weight="700" fill="#FFFFFF">Validation &#8212; 20%</text>
# MAGIC <rect x="796" y="40" width="184" height="46" rx="4" fill="#FF5F46"/><text x="888" y="69" text-anchor="middle" font-size="14" font-weight="700" fill="#FFFFFF">Test &#8212; 20%</text>
# MAGIC <text x="20" y="106" font-size="12.5" fill="#618794">The same rows land in the same split every run &#8212; a prerequisite for comparing models fairly.</text>
# MAGIC </svg></div><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:0;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#6E8B6E;font-style:italic;"># Reproducible train / validation / test split with a fixed seed</span>
# MAGIC train_df, validation_df, test_df = df.randomSplit([0.6, 0.2, 0.2], seed=42)
# MAGIC &#8203;
# MAGIC <span style="color:#0B5FA5;">print</span>(<span style="color:#2E8B57;">&quot;Training Data:&quot;</span>);   train_df.show()
# MAGIC <span style="color:#0B5FA5;">print</span>(<span style="color:#2E8B57;">&quot;Validation Data:&quot;</span>); validation_df.show()
# MAGIC <span style="color:#0B5FA5;">print</span>(<span style="color:#2E8B57;">&quot;Testing Data:&quot;</span>);    test_df.show()</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ML Training and Tuning on Spark
# MAGIC ### Three ways to parallelize on a Spark cluster
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;">
# MAGIC <b>Purpose:</b> Spark accelerates training in three distinct ways. Which one fits depends on whether the <b>model itself</b> is distributed, each <b>dataset</b> fits on one node, or you are searching many <b>hyperparameter</b> configurations at once.
# MAGIC </p>
# MAGIC <div style="overflow-x:auto;max-width:900px;margin:0 auto 20px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 900 580" role="img" style="min-width:820px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC   <title>Three approaches to training and tuning on Spark</title>
# MAGIC   <desc>Data parallel training produces one model across a distributed dataset; parallel model training trains many single-node models in parallel; parallel hyperparameter optimization fits many configurations in parallel.</desc>
# MAGIC   <defs><marker id="tt-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#F26B4E" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></marker></defs>
# MAGIC   <line x1="18" y1="212" x2="874" y2="212" stroke="#E0E0E0"/>
# MAGIC   <line x1="18" y1="396" x2="874" y2="396" stroke="#E0E0E0"/>
# MAGIC   <line x1="52" y1="24" x2="52" y2="556" stroke="#F26B4E" stroke-width="1.6"/>
# MAGIC   <text transform="translate(24 118) rotate(-90)" text-anchor="middle" font-size="15" font-weight="700" fill="#1B3139"><tspan x="0" dy="0">Distributed</tspan><tspan x="0" dy="17">ML Library</tspan></text>
# MAGIC   <text x="235" y="98" text-anchor="middle" font-size="21" font-weight="700" fill="#1B3139">Data Parallel</text>
# MAGIC   <text x="235" y="126" text-anchor="middle" font-size="21" font-weight="700" fill="#1B3139">Training</text>
# MAGIC   <text x="470" y="44" text-anchor="middle" font-size="15" fill="#1C2733">Training Data</text><text x="644" y="44" text-anchor="middle" font-size="15" fill="#1C2733">Nodes</text><text x="800" y="44" text-anchor="middle" font-size="15" fill="#1C2733">Model(s)</text>
# MAGIC   <path d="M422 72 V128 A 48 15 0 0 0 518 128 V72 Z" fill="#EFEFEF" stroke="#C7CDD2"/><ellipse cx="470" cy="72" rx="48" ry="15" fill="#EFEFEF" stroke="#C7CDD2"/>
# MAGIC   <rect x="612" y="56" width="32" height="32" rx="5" fill="#EFEFEF" stroke="#C7CDD2"/>
# MAGIC   <rect x="612" y="96" width="32" height="32" rx="5" fill="#EFEFEF" stroke="#C7CDD2"/>
# MAGIC   <rect x="612" y="136" width="32" height="32" rx="5" fill="#EFEFEF" stroke="#C7CDD2"/>
# MAGIC   <circle cx="800" cy="66" r="15" fill="#F2A93B"/>
# MAGIC   <circle cx="800" cy="106" r="15" fill="#F2A93B"/>
# MAGIC   <circle cx="800" cy="146" r="15" fill="#F2A93B"/>
# MAGIC   <path d="M518 96 L606 73" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M518 106 L606 113" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M518 118 L606 152" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M646 72 L780 66" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M646 112 L780 106" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M646 152 L780 146" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M822 58 Q834 58 834 72 L834 96 Q834 106 842 106 Q834 106 834 116 L834 140 Q834 154 822 154" fill="none" stroke="#F26B4E" stroke-width="1.6"/>
# MAGIC   <circle cx="874" cy="106" r="15" fill="#4CAF7D"/>
# MAGIC   <text transform="translate(24 304) rotate(-90)" text-anchor="middle" font-size="15" font-weight="700" fill="#1B3139"><tspan x="0" dy="0">Single Node</tspan><tspan x="0" dy="17">ML Library</tspan></text>
# MAGIC   <text x="235" y="282" text-anchor="middle" font-size="21" font-weight="700" fill="#1B3139">Parallel Model</text>
# MAGIC   <text x="235" y="310" text-anchor="middle" font-size="21" font-weight="700" fill="#1B3139">Training</text>
# MAGIC   <text x="470" y="240" text-anchor="middle" font-size="15" fill="#1C2733">Training Data</text><text x="644" y="240" text-anchor="middle" font-size="15" fill="#1C2733">Nodes</text><text x="800" y="240" text-anchor="middle" font-size="15" fill="#1C2733">Model(s)</text>
# MAGIC   <path d="M426 252 V266 A 44 10 0 0 0 514 266 V252 Z" fill="#EFEFEF" stroke="#C7CDD2"/><ellipse cx="470" cy="252" rx="44" ry="10" fill="#EFEFEF" stroke="#C7CDD2"/>
# MAGIC   <path d="M426 292 V306 A 44 10 0 0 0 514 306 V292 Z" fill="#EFEFEF" stroke="#C7CDD2"/><ellipse cx="470" cy="292" rx="44" ry="10" fill="#EFEFEF" stroke="#C7CDD2"/>
# MAGIC   <path d="M426 332 V346 A 44 10 0 0 0 514 346 V332 Z" fill="#EFEFEF" stroke="#C7CDD2"/><ellipse cx="470" cy="332" rx="44" ry="10" fill="#EFEFEF" stroke="#C7CDD2"/>
# MAGIC   <rect x="614" y="250" width="30" height="30" rx="5" fill="#EFEFEF" stroke="#C7CDD2"/>
# MAGIC   <rect x="614" y="290" width="30" height="30" rx="5" fill="#EFEFEF" stroke="#C7CDD2"/>
# MAGIC   <rect x="614" y="330" width="30" height="30" rx="5" fill="#EFEFEF" stroke="#C7CDD2"/>
# MAGIC   <circle cx="795" cy="265" r="14" fill="#F2A93B"/><text x="795" y="270" text-anchor="middle" font-size="13" font-weight="700" fill="#FFFFFF">A</text>
# MAGIC   <circle cx="795" cy="305" r="14" fill="#F2A93B"/><text x="795" y="310" text-anchor="middle" font-size="13" font-weight="700" fill="#FFFFFF">B</text>
# MAGIC   <circle cx="795" cy="345" r="14" fill="#F2A93B"/><text x="795" y="350" text-anchor="middle" font-size="13" font-weight="700" fill="#FFFFFF">C</text>
# MAGIC   <path d="M514 266 L608 265" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M644 265 L777 265" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M514 306 L608 305" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M644 305 L777 305" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M514 346 L608 345" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M644 345 L777 345" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <text transform="translate(24 484) rotate(-90)" text-anchor="middle" font-size="15" font-weight="700" fill="#1B3139"><tspan x="0" dy="0">Combination</tspan></text>
# MAGIC   <text x="235" y="452" text-anchor="middle" font-size="21" font-weight="700" fill="#1B3139">Parallel</text>
# MAGIC   <text x="235" y="480" text-anchor="middle" font-size="21" font-weight="700" fill="#1B3139">Hyperparameter</text>
# MAGIC   <text x="235" y="508" text-anchor="middle" font-size="21" font-weight="700" fill="#1B3139">Optimization</text>
# MAGIC   <text x="470" y="424" text-anchor="middle" font-size="15" fill="#1C2733">Training Data</text><text x="644" y="424" text-anchor="middle" font-size="15" fill="#1C2733">Nodes</text><text x="800" y="424" text-anchor="middle" font-size="15" fill="#1C2733">Model(s)</text>
# MAGIC   <path d="M422 452 V508 A 48 15 0 0 0 518 508 V452 Z" fill="#EFEFEF" stroke="#C7CDD2"/><ellipse cx="470" cy="452" rx="48" ry="15" fill="#EFEFEF" stroke="#C7CDD2"/>
# MAGIC   <rect x="614" y="436" width="30" height="30" rx="5" fill="#EFEFEF" stroke="#C7CDD2"/>
# MAGIC   <rect x="614" y="472" width="30" height="30" rx="5" fill="#EFEFEF" stroke="#C7CDD2"/>
# MAGIC   <rect x="614" y="508" width="30" height="30" rx="5" fill="#EFEFEF" stroke="#C7CDD2"/>
# MAGIC   <circle cx="795" cy="451" r="14" fill="#F2A93B"/><text x="795" y="456" text-anchor="middle" font-size="13" font-weight="700" fill="#FFFFFF">X</text>
# MAGIC   <circle cx="795" cy="487" r="14" fill="#F2A93B"/><text x="795" y="492" text-anchor="middle" font-size="13" font-weight="700" fill="#FFFFFF">Y</text>
# MAGIC   <circle cx="795" cy="523" r="14" fill="#F2A93B"/><text x="795" y="528" text-anchor="middle" font-size="13" font-weight="700" fill="#FFFFFF">Z</text>
# MAGIC   <path d="M518 476 L608 451" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M518 488 L608 487" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M518 500 L608 523" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M644 451 L777 451" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M644 487 L777 487" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   <path d="M644 523 L777 523" stroke="#F26B4E" stroke-width="1.6" fill="none" marker-end="url(#tt-ar)"/>
# MAGIC   </svg>
# MAGIC </div>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;">
# MAGIC <div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="6" width="6" height="5" rx="1"/><rect x="3" y="13.5" width="6" height="5" rx="1"/><rect x="3" y="21" width="6" height="5" rx="1"/><path d="M9.5 8.5 L19 15 M9.5 16 H19 M9.5 23.5 L19 17"/></g><circle cx="25" cy="16" r="4.2" fill="#FF5F46"/></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Distributed ML library</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Data Parallel Training</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Trains <b>one</b> model across a distributed dataset &mdash; needs a distributed library such as Spark ML. Great for large data with no downsampling, but not every algorithm scales this way (e.g. KNN, where the model is the whole dataset).</p></div>
# MAGIC <div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="6" height="5" rx="1"/><path d="M9 6.5 H20"/><circle cx="24" cy="6.5" r="3"/><rect x="3" y="13.5" width="6" height="5" rx="1"/><path d="M9 16 H20"/><circle cx="24" cy="16" r="3"/><rect x="3" y="23" width="6" height="5" rx="1"/><path d="M9 25.5 H20"/><circle cx="24" cy="25.5" r="3"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Single-node ML library</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Parallel Model Training</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Trains <b>many independent single-node models</b> in parallel; each dataset must fit on one worker. Scales out training with any ML library &mdash; ideal for per-entity models such as one per IoT device.</p><div style="margin-top:10px;font-size:12.5px;font-style:italic;color:#618794;">Discussed later in the course.</div></div>
# MAGIC <div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="7" width="26" height="18" rx="2"/><path d="M7 12 H25 M7 16 H25 M7 20 H25"/></g><g fill="#FF5F46"><circle cx="12" cy="12" r="2"/><circle cx="20" cy="16" r="2"/><circle cx="16" cy="20" r="2"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Combination</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Parallel Hyperparameter Optimization</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Fits many <b>hyperparameter configurations</b> in parallel, for single-node or distributed training. On Databricks, use Optuna with the Joblib Spark backend, or Spark ML <code>CrossValidator</code>.</p></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background:#0B2027;border-radius:8px;padding:46px 24px;text-align:center;margin:6px 0;">
# MAGIC   <span style="color:#ffffff;font-size:30px;font-weight:800;letter-spacing:.3px;">Model Training and Tuning with Spark</span>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Model Training and Tuning
# MAGIC
# MAGIC <div style="max-width:1320px;margin:0 auto;">
# MAGIC <svg width="100%" viewBox="0 0 1320 250" role="img" style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC <title>Machine learning lifecycle</title><desc>The PawMatch machine learning lifecycle from business problem through model monitoring, with the current stage highlighted.</desc>
# MAGIC <defs><marker id="lc-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-linecap="round"/></marker><marker id="lc-fb" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B5162" stroke-width="1.6" stroke-linecap="round"/></marker></defs>
# MAGIC <rect x="8" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="79" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Business</text>
# MAGIC <text x="79" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Problem</text>
# MAGIC <path d="M153 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="174" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="245" y="84" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Define</text>
# MAGIC <text x="245" y="104" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Success</text>
# MAGIC <text x="245" y="124" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Criteria</text>
# MAGIC <path d="M319 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="340" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="411" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Data</text>
# MAGIC <text x="411" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Collection</text>
# MAGIC <path d="M485 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="506" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="577" y="74" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Data</text>
# MAGIC <text x="577" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Preprocessing/</text>
# MAGIC <text x="577" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Feature</text>
# MAGIC <text x="577" y="134" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Engineering</text>
# MAGIC <path d="M651 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="672" y="44" width="142" height="120" rx="10" fill="#1B3139" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="743" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">Model Training</text>
# MAGIC <text x="743" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">& Tuning</text>
# MAGIC <path d="M817 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="838" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="909" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="909" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Evaluation</text>
# MAGIC <path d="M983 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="1004" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="1075" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="1075" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Deployment</text>
# MAGIC <path d="M1149 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="1170" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="1241" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="1241" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Monitoring</text>
# MAGIC <path d="M909 164 C 869 219, 617 219, 577 164" stroke="#1B5162" stroke-width="1.8" fill="none" stroke-dasharray="7 5" marker-end="url(#lc-fb)"/>
# MAGIC <path d="M909 164 C 829 254, 451 254, 411 164" stroke="#1B5162" stroke-width="1.8" fill="none" stroke-dasharray="7 5" marker-end="url(#lc-fb)"/>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC - **Objective:** train a machine learning model on the training dataset using Spark ML to predict outcomes, such as animal adoption likelihood.
# MAGIC - **Action:** select a suitable model algorithm (or algorithms) to fit a model on the training dataset.
# MAGIC
# MAGIC <div style="background:#1B5162;color:#fff;border-radius:4px;padding:10px 16px;margin-top:10px;text-align:center;font-weight:700;">What Model Algorithm(s) Might be Useful?</div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Model Training Scenario: LogisticRegression
# MAGIC ### Binary classification: long vs. short adoption timeframe
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> for PawMatch we frame adoption as a <b>binary classification</b> problem &mdash; will an animal have a long or short adoption timeframe? <code>LogisticRegression</code> predicts the probability of each outcome, giving both a label and a confidence.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:0;"><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="7" width="24" height="18" rx="2"/><path d="M9 16 l4 4 l10 -10"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Method</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">fit &middot; explainParams</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;"><code>fit()</code> trains the model on the input DataFrame; <code>explainParams()</code> returns every parameter with its default and user-supplied value.</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="8" width="24" height="16" rx="2"/><path d="M8 13 h16 M8 17 h16"/></g><g fill="#FF5F46"><circle cx="11" cy="13" r="2"/><circle cx="19" cy="17" r="2"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Hyperparameters</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">maxIter &middot; threshold</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;"><code>maxIter</code> caps the training iterations (default 100); <code>threshold</code> is the probability cut-off that turns a score into a predicted label (default 0.5).</p></div></div><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">from</span> pyspark.sql <span style="color:#0B5FA5;">import</span> SparkSession
# MAGIC <span style="color:#0B5FA5;">from</span> pyspark.ml.classification <span style="color:#0B5FA5;">import</span> LogisticRegression
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Split the data into train and test sets</span>
# MAGIC train_df, test_df = df_assembled.randomSplit([0.8, 0.2], seed=42)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Initialize and fit Logistic Regression with specific hyperparameters</span>
# MAGIC lr = LogisticRegression(featuresCol=<span style="color:#2E8B57;">&quot;features&quot;</span>, labelCol=<span style="color:#2E8B57;">&quot;Adopted&quot;</span>,
# MAGIC                         maxIter=10, regParam=0.01, threshold=0.3)
# MAGIC lr_model = lr.fit(train_df)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Explain the parameters of the model</span>
# MAGIC <span style="color:#0B5FA5;">print</span>(lr_model.explainParams())
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Make predictions on the test data</span>
# MAGIC predictions = lr_model.transform(test_df)
# MAGIC predictions.select(<span style="color:#2E8B57;">&quot;AnimalID&quot;</span>, <span style="color:#2E8B57;">&quot;prediction&quot;</span>, <span style="color:#2E8B57;">&quot;probability&quot;</span>).show()</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Databricks provides Managed MLflow
# MAGIC ### Manage the full machine learning lifecycle
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;">
# MAGIC <b>Purpose:</b> MLflow is the open-source platform for managing the end-to-end ML lifecycle. On Databricks it is <b>fully managed</b> &mdash; its Tracking, Projects, Models and Model Registry components wrap the everyday cycle of preparing data, building a model, and deploying it.
# MAGIC </p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:28px;align-items:center;">
# MAGIC <div style="flex:1 1 420px;min-width:360px;">
# MAGIC <div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;margin-bottom:6px;">Key advantages</div>
# MAGIC <ul style="margin:0;padding-left:20px;font-size:15px;line-height:1.55;color:#33414B;">
# MAGIC <li style="margin:8px 0;"><b style="color:#0b2026;">Maintenance-free:</b> a fully managed service removes operational overhead.</li><li style="margin:8px 0;"><b style="color:#0b2026;">Pre-installed:</b> ships with the Databricks Runtime for ML.</li><li style="margin:8px 0;"><b style="color:#0b2026;">Built-in tracking server:</b> logs every experiment and model with no extra setup.</li><li style="margin:8px 0;"><b style="color:#0b2026;">Feature Store integration:</b> centralizes features for consistency across training and inference.</li><li style="margin:8px 0;"><b style="color:#0b2026;">Enhanced security:</b> model-level access control and secure transactions.</li><li style="margin:8px 0;"><b style="color:#0b2026;">Streamlined operations:</b> automated tracking of each ML step to analyze and optimize.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC <div style="flex:1 1 420px;min-width:360px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 560 430" role="img" style="display:block;max-width:520px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <title>Managed MLflow across the ML lifecycle</title>
# MAGIC <desc>MLflow Tracking, Projects, Models and the Model Registry wrap a central lifecycle cycle of Prep Data, Build Model and Deploy Model.</desc>
# MAGIC <defs><marker id="ml-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#5A6B73" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></marker></defs>
# MAGIC <!-- connectors from components to centre -->
# MAGIC <g stroke="#C7CDD2" stroke-width="1.4" stroke-dasharray="4 4" fill="none"><path d="M95 44 L235 120"/><path d="M465 44 L410 150"/><path d="M465 386 L400 300"/><path d="M95 386 L160 300"/></g>
# MAGIC <!-- cyclic arrows -->
# MAGIC <g stroke="#5A6B73" stroke-width="2" fill="none">
# MAGIC <path d="M338 82 C 430 108, 452 190, 440 244" marker-end="url(#ml-ar)"/>
# MAGIC <path d="M356 292 C 300 322, 232 322, 196 296" marker-end="url(#ml-ar)"/>
# MAGIC <path d="M118 244 C 108 186, 140 116, 220 88" marker-end="url(#ml-ar)"/>
# MAGIC </g>
# MAGIC <text x="280" y="196" text-anchor="middle" font-size="13" font-style="italic" fill="#8A97A3">ML lifecycle</text>
# MAGIC <!-- cycle boxes -->
# MAGIC <rect x="212" y="42" width="136" height="52" rx="10" fill="#4CAE7A"/><text x="280" y="74" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Prep Data</text>
# MAGIC <rect x="360" y="248" width="148" height="52" rx="10" fill="#1B5162"/><text x="434" y="280" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Build Model</text>
# MAGIC <rect x="52" y="248" width="148" height="52" rx="10" fill="#FF5F46"/><text x="126" y="280" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Deploy Model</text>
# MAGIC <!-- MLflow component pills -->
# MAGIC <g font-size="13" font-weight="700" text-anchor="middle">
# MAGIC <rect x="16" y="10" width="164" height="36" rx="18" fill="#0B2027"/><text x="98" y="33" fill="#FFFFFF">MLflow Tracking</text>
# MAGIC <rect x="380" y="10" width="164" height="36" rx="18" fill="#0B2027"/><text x="462" y="33" fill="#FFFFFF">MLflow Projects</text>
# MAGIC <rect x="380" y="384" width="164" height="36" rx="18" fill="#0B2027"/><text x="462" y="407" fill="#FFFFFF">MLflow Models</text>
# MAGIC <rect x="16" y="384" width="164" height="36" rx="18" fill="#0B2027"/><text x="98" y="407" fill="#FFFFFF">Model Registry</text>
# MAGIC </g>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## MLflow Tracking
# MAGIC ### Record every run with the MLflow Tracking API
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> the MLflow Tracking API records the parameters, metrics, artifacts and model of each run so experiments are reproducible and comparable. You can log everything automatically with one line, or log specific values by hand.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:0;"><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="6" width="22" height="20" rx="2"/><path d="M5 11 h22 M10 6 v-2 M22 6 v-2"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Organize</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">set_experiment</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Groups runs under a named experiment; creates it if it does not exist and makes it active.</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="16" cy="16" r="12"/><path d="M13 11 l8 5 l-8 5 Z"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Scope a run</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">start_run</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Opens a new run (an <code>ActiveRun</code>); ends the current one first, or use <code>nested=True</code>.</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M25 7 a11 11 0 1 0 3 8"/><path d="M25 4 v6 h-6"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">One-line logging</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">autolog</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Logs params, metrics, data lineage, the model and its environment automatically.</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M6 26 V10 M6 26 H26"/><rect x="10" y="17" width="4" height="7"/><rect x="17" y="12" width="4" height="12"/><rect x="24" y="8" width="0.5" height="16"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Manual logging</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">log_metric / log_param</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Record specific values yourself with <code>log_metric()</code> and <code>log_param()</code> calls.</p></div></div><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 540px;min-width:460px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Autologging</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">import</span> mlflow
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Set an experiment name</span>
# MAGIC experiment_name = <span style="color:#2E8B57;">&quot;Wine Quality Model&quot;</span>
# MAGIC mlflow.set_experiment(experiment_name)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Start a run under the set experiment</span>
# MAGIC <span style="color:#0B5FA5;">with</span> mlflow.start_run():
# MAGIC     mlflow.spark.autolog()   <span style="color:#6E8B6E;font-style:italic;"># Enable autologging</span>
# MAGIC     ...                      <span style="color:#6E8B6E;font-style:italic;"># Your ML code</span></pre></div><div style="flex:1 1 460px;min-width:460px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Manual logging</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">import</span> mlflow
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Create a new experiment (name must be unique)</span>
# MAGIC experiment_name = <span style="color:#2E8B57;">&quot;Wine Quality Model&quot;</span>
# MAGIC experiment_id = mlflow.create_experiment(experiment_name)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Start a run and log manually</span>
# MAGIC <span style="color:#0B5FA5;">with</span> mlflow.start_run(experiment_id=experiment_id):
# MAGIC     ...
# MAGIC     mlflow.log_metric(<span style="color:#2E8B57;">&quot;rmse&quot;</span>, rmse)
# MAGIC     mlflow.log_param(<span style="color:#2E8B57;">&quot;numTrees&quot;</span>, 100)</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background:#0B2027;border-radius:8px;padding:46px 24px;text-align:center;margin:6px 0;">
# MAGIC   <span style="color:#ffffff;font-size:30px;font-weight:800;letter-spacing:.3px;">Streamlining Model Development with Spark</span>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Streamlining Model Development
# MAGIC
# MAGIC <div style="max-width:1320px;margin:0 auto;">
# MAGIC <svg width="100%" viewBox="0 0 1320 250" role="img" style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC <title>Machine learning lifecycle</title><desc>The PawMatch machine learning lifecycle from business problem through model monitoring, with the current stage highlighted.</desc>
# MAGIC <defs><marker id="lc-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-linecap="round"/></marker><marker id="lc-fb" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B5162" stroke-width="1.6" stroke-linecap="round"/></marker></defs>
# MAGIC <rect x="8" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="79" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Business</text>
# MAGIC <text x="79" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Problem</text>
# MAGIC <path d="M153 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="174" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="245" y="84" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Define</text>
# MAGIC <text x="245" y="104" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Success</text>
# MAGIC <text x="245" y="124" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Criteria</text>
# MAGIC <path d="M319 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="340" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="411" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Data</text>
# MAGIC <text x="411" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Collection</text>
# MAGIC <path d="M485 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="506" y="44" width="142" height="120" rx="10" fill="#1B3139" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="577" y="74" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">Data</text>
# MAGIC <text x="577" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">Preprocessing/</text>
# MAGIC <text x="577" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">Feature</text>
# MAGIC <text x="577" y="134" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">Engineering</text>
# MAGIC <path d="M651 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="672" y="44" width="142" height="120" rx="10" fill="#1B3139" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="743" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">Model Training</text>
# MAGIC <text x="743" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">& Tuning</text>
# MAGIC <path d="M817 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="838" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="909" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="909" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Evaluation</text>
# MAGIC <path d="M983 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="1004" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="1075" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="1075" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Deployment</text>
# MAGIC <path d="M1149 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="1170" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="1241" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="1241" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Monitoring</text>
# MAGIC <path d="M909 164 C 869 219, 617 219, 577 164" stroke="#1B5162" stroke-width="1.8" fill="none" stroke-dasharray="7 5" marker-end="url(#lc-fb)"/>
# MAGIC <path d="M909 164 C 829 254, 451 254, 411 164" stroke="#1B5162" stroke-width="1.8" fill="none" stroke-dasharray="7 5" marker-end="url(#lc-fb)"/>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC - **Objective:** optimize the entire model development process — spanning data preprocessing, feature engineering, model training, and tuning — by making it more efficient and repeatable.
# MAGIC - **Action:** construct a Pipeline using Spark ML that integrates all stages of the machine learning workflow.
# MAGIC
# MAGIC <div style="background:#1B5162;color:#fff;border-radius:4px;padding:10px 16px;margin-top:10px;text-align:center;font-weight:700;">What will be part of the Pipeline?</div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Advantages of Using Spark ML Pipelines
# MAGIC ### Streamlining machine learning workflows
# MAGIC
# MAGIC <div style="overflow-x: auto; margin: 16px 0;">
# MAGIC <div style="min-width: 1050px; max-width: 1300px; margin: 0 auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1180 430" role="img" style="display:block;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><title>Advantages of Using Spark ML Pipelines</title><desc>Five overlapping arrow banners list the advantages of Spark ML Pipelines: consistency, reusability, scalability, simplified workflow, and hyperparameter tuning. Beside them, a cycle of three arrows marks the Spark ML Pipeline.</desc><path d="M18 34 H750 V10 L804 64 L750 118 V92 H18 Z" fill="#1B5162" stroke="#FFFFFF" stroke-width="1.5"/><text x="24" y="59" text-anchor="start" font-size="15.5" font-weight="800" fill="#FFFFFF">Consistency:</text><rect x="18" y="92" width="148" height="210" fill="#F4F2EF" stroke="#1B5162" stroke-width="1.5"/><text x="24" y="113" font-size="14.5" fill="#111111"><tspan x="24" dy="0">Ensures that the</tspan><tspan x="24" dy="17">same data</tspan><tspan x="24" dy="17">transformations</tspan><tspan x="24" dy="17">and model training</tspan><tspan x="24" dy="17">steps are applied</tspan><tspan x="24" dy="17">uniformly,</tspan><tspan x="24" dy="17">reducing the risk</tspan><tspan x="24" dy="17">of errors and</tspan><tspan x="24" dy="17">inconsistencies.</tspan></text><path d="M166 72 H750 V48 L804 102 L750 156 V130 H166 Z" fill="#1B5162" stroke="#FFFFFF" stroke-width="1.5"/><text x="172" y="97" text-anchor="start" font-size="15.5" font-weight="800" fill="#FFFFFF">Reusability:</text><rect x="166" y="130" width="150" height="210" fill="#F4F2EF" stroke="#1B5162" stroke-width="1.5"/><text x="172" y="151" font-size="14.5" fill="#111111"><tspan x="172" dy="0">Pipelines can be</tspan><tspan x="172" dy="17">saved and reused</tspan><tspan x="172" dy="17">across different</tspan><tspan x="172" dy="17">datasets or</tspan><tspan x="172" dy="17">projects,</tspan><tspan x="172" dy="17">significantly</tspan><tspan x="172" dy="17">reducing the time</tspan><tspan x="172" dy="17">needed for model</tspan><tspan x="172" dy="17">development.</tspan></text><path d="M316 110 H750 V86 L804 140 L750 194 V168 H316 Z" fill="#1B5162" stroke="#FFFFFF" stroke-width="1.5"/><text x="322" y="135" text-anchor="start" font-size="15.5" font-weight="800" fill="#FFFFFF">Scalability:</text><rect x="316" y="168" width="150" height="210" fill="#F4F2EF" stroke="#1B5162" stroke-width="1.5"/><text x="322" y="189" font-size="14.5" fill="#111111"><tspan x="322" dy="0">Designed to</tspan><tspan x="322" dy="17">efficiently handle</tspan><tspan x="322" dy="17">large-scale</tspan><tspan x="322" dy="17">datasets, Pipelines</tspan><tspan x="322" dy="17">in Spark ML are</tspan><tspan x="322" dy="17">optimized for</tspan><tspan x="322" dy="17">distributed</tspan><tspan x="322" dy="17">processing,</tspan><tspan x="322" dy="17">making them ideal</tspan><tspan x="322" dy="17">for big data</tspan><tspan x="322" dy="17">applications.</tspan></text><path d="M466 148 H750 V124 L804 178 L750 232 V206 H466 Z" fill="#1B5162" stroke="#FFFFFF" stroke-width="1.5"/><text x="472" y="173" text-anchor="start" font-size="15.5" font-weight="800" fill="#FFFFFF">Simplified Workflow:</text><rect x="466" y="206" width="150" height="210" fill="#F4F2EF" stroke="#1B5162" stroke-width="1.5"/><text x="472" y="227" font-size="14.5" fill="#111111"><tspan x="472" dy="0">By chaining</tspan><tspan x="472" dy="17">together multiple</tspan><tspan x="472" dy="17">stages, Pipelines</tspan><tspan x="472" dy="17">streamline the</tspan><tspan x="472" dy="17">process, making</tspan><tspan x="472" dy="17">complex workflows</tspan><tspan x="472" dy="17">easier to</tspan><tspan x="472" dy="17">manage and</tspan><tspan x="472" dy="17">maintain.</tspan></text><path d="M616 186 H750 V162 L804 216 L750 270 V244 H616 Z" fill="#1B5162" stroke="#FFFFFF" stroke-width="1.5"/><text x="622" y="205" font-size="14.5" font-weight="800" fill="#FFFFFF"><tspan x="622" dy="0">Hyperparameter</tspan><tspan x="622" dy="16">Tuning:</tspan></text><rect x="616" y="244" width="150" height="174" fill="#F4F2EF" stroke="#1B5162" stroke-width="1.5"/><text x="622" y="265" font-size="14.5" fill="#111111"><tspan x="622" dy="0">Integrates</tspan><tspan x="622" dy="17">seamlessly with</tspan><tspan x="622" dy="17">cross-validation</tspan><tspan x="622" dy="17">and hyperparameter</tspan><tspan x="622" dy="17">tuning methods,</tspan><tspan x="622" dy="17">allowing for more</tspan><tspan x="622" dy="17">efficient and</tspan><tspan x="622" dy="17">automated model</tspan><tspan x="622" dy="17">optimization.</tspan></text><foreignObject x="826" y="18" width="196" height="86"><div xmlns="http://www.w3.org/1999/xhtml" style="width:196px;height:86px;display:flex;align-items:center;justify-content:center;"><img src="https://files.training.databricks.com/binder/prod_main/machine-learning-at-scale-en_us-1.3.4/images/20261023T044612Z/Includes/images/Apache Spark.png" alt="Apache Spark" style="max-width:196px;max-height:86px;display:block;"/></div></foreignObject><text x="1032" y="92" text-anchor="start" font-size="56" font-weight="500" fill="#F57C20">ML</text><text x="962" y="168" text-anchor="middle" font-size="42" font-weight="800" fill="#071E26">Pipeline</text><g transform="translate(962 316) scale(1.22)" aria-hidden="true"><path d="M-10.51 -32.34 A34 34 0 0 1 22.75 -25.27 L13.38 -14.86 L39.87 -16.11 L44.16 -49.05 L34.79 -38.64 A52 52 0 0 0 -16.07 -49.45 Z" fill="#FFFFFF" stroke="#111111" stroke-width="5" stroke-linejoin="round"/><path d="M33.26 7.07 A34 34 0 0 1 10.51 32.34 L6.18 19.02 L-5.98 42.58 L20.40 62.77 L16.07 49.45 A52 52 0 0 0 50.86 10.81 Z" fill="#FFFFFF" stroke="#111111" stroke-width="5" stroke-linejoin="round"/><path d="M-22.75 25.27 A34 34 0 0 1 -33.26 -7.07 L-19.56 -4.16 L-33.88 -26.47 L-64.56 -13.72 L-50.86 -10.81 A52 52 0 0 0 -34.79 38.64 Z" fill="#FFFFFF" stroke="#111111" stroke-width="5" stroke-linejoin="round"/></g></svg>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Creating and Fitting a Pipeline in Spark ML
# MAGIC ### One reusable object for the whole workflow
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> a <code>Pipeline</code> chains feature engineering, scaling and model training into a <b>single object</b>. Fitting it runs every stage in order and returns a <code>PipelineModel</code> you can save, reload and apply to new data &mdash; a streamlined, reproducible ML workflow.</p>
# MAGIC <div style="overflow-x:auto;margin:4px 0 20px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1140 190" role="img" style="display:block;min-width:900px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><title>A three-stage Spark ML Pipeline</title><desc>VectorAssembler feeds StandardScaler feeds LogisticRegression inside a Pipeline; fitting it returns a PipelineModel.</desc><defs><marker id="pf-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></marker></defs>
# MAGIC <rect x="20" y="70" width="120" height="60" rx="8" fill="#FFFFFF" stroke="#1B5162" stroke-width="1.4"/><text x="80" y="100" text-anchor="middle" font-size="14" font-weight="600" fill="#0b2026">train_df</text><text x="80" y="118" text-anchor="middle" font-size="11" fill="#618794">DataFrame</text>
# MAGIC <path d="M146 100 H190" stroke="#1B3139" stroke-width="1.8" fill="none" marker-end="url(#pf-ar)"/>
# MAGIC <rect x="196" y="36" width="668" height="128" rx="14" fill="#F9F7F4" stroke="#1B5162" stroke-opacity="0.4" stroke-width="1.4"/><text x="214" y="60" font-size="14" font-weight="700" fill="#FF5F46">Pipeline</text>
# MAGIC <rect x="220" y="76" width="180" height="60" rx="8" fill="#FFFFFF" stroke="#1B5162" stroke-width="1.4"/><text x="310" y="102" text-anchor="middle" font-size="14" font-weight="600" fill="#0b2026">VectorAssembler</text><text x="310" y="120" text-anchor="middle" font-size="11" fill="#618794">&#8594; features</text>
# MAGIC <path d="M404 106 H436" stroke="#1B3139" stroke-width="1.8" fill="none" marker-end="url(#pf-ar)"/>
# MAGIC <rect x="440" y="76" width="180" height="60" rx="8" fill="#FFFFFF" stroke="#1B5162" stroke-width="1.4"/><text x="530" y="102" text-anchor="middle" font-size="14" font-weight="600" fill="#0b2026">StandardScaler</text><text x="530" y="120" text-anchor="middle" font-size="11" fill="#618794">&#8594; scaledFeatures</text>
# MAGIC <path d="M624 106 H656" stroke="#1B3139" stroke-width="1.8" fill="none" marker-end="url(#pf-ar)"/>
# MAGIC <rect x="660" y="76" width="188" height="60" rx="8" fill="#FFFFFF" stroke="#1B5162" stroke-width="1.4"/><text x="754" y="106" text-anchor="middle" font-size="14" font-weight="600" fill="#0b2026">LogisticRegression</text>
# MAGIC <path d="M870 100 H914" stroke="#1B3139" stroke-width="1.8" fill="none" marker-end="url(#pf-ar)"/><text x="892" y="90" text-anchor="middle" font-size="11" fill="#618794">.fit()</text>
# MAGIC <rect x="920" y="70" width="200" height="60" rx="8" fill="#FFFFFF" stroke="#1B5162" stroke-width="1.4"/><text x="1020" y="100" text-anchor="middle" font-size="14" font-weight="600" fill="#0b2026">PipelineModel</text><text x="1020" y="118" text-anchor="middle" font-size="11" fill="#618794">a fitted Transformer</text>
# MAGIC </svg></div><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">from</span> pyspark.ml <span style="color:#0B5FA5;">import</span> Pipeline
# MAGIC <span style="color:#0B5FA5;">from</span> pyspark.ml.feature <span style="color:#0B5FA5;">import</span> VectorAssembler, StandardScaler
# MAGIC <span style="color:#0B5FA5;">from</span> pyspark.ml.classification <span style="color:#0B5FA5;">import</span> LogisticRegression
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Define the stages</span>
# MAGIC assembler = VectorAssembler(inputCols=[<span style="color:#2E8B57;">&quot;Age&quot;</span>, <span style="color:#2E8B57;">&quot;Weight&quot;</span>, <span style="color:#2E8B57;">&quot;BreedIndex&quot;</span>],
# MAGIC                             outputCol=<span style="color:#2E8B57;">&quot;features&quot;</span>)
# MAGIC scaler = StandardScaler(inputCol=<span style="color:#2E8B57;">&quot;features&quot;</span>, outputCol=<span style="color:#2E8B57;">&quot;scaledFeatures&quot;</span>)
# MAGIC lr = LogisticRegression(featuresCol=<span style="color:#2E8B57;">&quot;scaledFeatures&quot;</span>, labelCol=<span style="color:#2E8B57;">&quot;Adopted&quot;</span>)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Chain the stages into one Pipeline and fit it</span>
# MAGIC pipeline = Pipeline(stages=[assembler, scaler, lr])
# MAGIC pipeline_model = pipeline.fit(train_df)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Save and reload the fitted Pipeline</span>
# MAGIC pipeline_model.save(<span style="color:#2E8B57;">&quot;/path/to/pipeline_model&quot;</span>)
# MAGIC loaded_pipeline_model = PipelineModel.load(<span style="color:#2E8B57;">&quot;/path/to/pipeline_model&quot;</span>)</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background:#0B2027;border-radius:8px;padding:46px 24px;text-align:center;margin:6px 0;">
# MAGIC   <span style="color:#ffffff;font-size:30px;font-weight:800;letter-spacing:.3px;">Model Evaluation and Model Metadata with Spark</span>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Model Evaluation
# MAGIC
# MAGIC <div style="max-width:1320px;margin:0 auto;">
# MAGIC <svg width="100%" viewBox="0 0 1320 250" role="img" style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC <title>Machine learning lifecycle</title><desc>The PawMatch machine learning lifecycle from business problem through model monitoring, with the current stage highlighted.</desc>
# MAGIC <defs><marker id="lc-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-linecap="round"/></marker><marker id="lc-fb" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B5162" stroke-width="1.6" stroke-linecap="round"/></marker></defs>
# MAGIC <rect x="8" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="79" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Business</text>
# MAGIC <text x="79" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Problem</text>
# MAGIC <path d="M153 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="174" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="245" y="84" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Define</text>
# MAGIC <text x="245" y="104" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Success</text>
# MAGIC <text x="245" y="124" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Criteria</text>
# MAGIC <path d="M319 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="340" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="411" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Data</text>
# MAGIC <text x="411" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Collection</text>
# MAGIC <path d="M485 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="506" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="577" y="74" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Data</text>
# MAGIC <text x="577" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Preprocessing/</text>
# MAGIC <text x="577" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Feature</text>
# MAGIC <text x="577" y="134" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Engineering</text>
# MAGIC <path d="M651 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="672" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="743" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model Training</text>
# MAGIC <text x="743" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">& Tuning</text>
# MAGIC <path d="M817 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="838" y="44" width="142" height="120" rx="10" fill="#1B3139" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="909" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">Model</text>
# MAGIC <text x="909" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#ffffff">Evaluation</text>
# MAGIC <path d="M983 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="1004" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="1075" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="1075" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Deployment</text>
# MAGIC <path d="M1149 104 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="1170" y="44" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="1241" y="94" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="1241" y="114" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Monitoring</text>
# MAGIC <path d="M909 164 C 869 219, 617 219, 577 164" stroke="#1B5162" stroke-width="1.8" fill="none" stroke-dasharray="7 5" marker-end="url(#lc-fb)"/>
# MAGIC <path d="M909 164 C 829 254, 451 254, 411 164" stroke="#1B5162" stroke-width="1.8" fill="none" stroke-dasharray="7 5" marker-end="url(#lc-fb)"/>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC - **Objective:** assess the performance and effectiveness of the trained model to predict the length of stay for pets, ensuring it meets the defined success criteria.
# MAGIC - **Action:** use appropriate evaluation metrics (e.g., accuracy, precision, recall, AUC-ROC) and validation techniques (e.g., cross-validation) to thoroughly evaluate the model's predictive power and generalizability.
# MAGIC
# MAGIC <div style="background:#1B5162;color:#fff;border-radius:4px;padding:10px 16px;margin-top:10px;text-align:center;font-weight:700;">What should be used to evaluate the model?</div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Evaluating the Model Using the Spark ML API
# MAGIC ### Selecting the right metrics and evaluators for model assessment
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> evaluation turns predictions into a single, comparable number. Pick the right <b>metric</b> and the matching <b>evaluator</b>, then score the model on a held-out test set for an unbiased estimate.</p>
# MAGIC <div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;margin:0 0 2px;">Action to evaluate</div><div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:6px;"><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="6" width="24" height="20" rx="2"/><path d="M4 11 h24"/><path d="M9 18 l3 3 l6 -6"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Step 1</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Make predictions</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Apply the trained model to a test dataset to generate a predictions DataFrame.</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M6 26 V6 M6 26 H26"/><path d="M6 26 C 11 12, 19 8, 26 7"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Step 2</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Choose a metric</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Accuracy, AUC, precision, recall, F1, RMSE &mdash; whatever matches the task.</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="13" cy="13" r="9"/><path d="M20 20 l7 7"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Step 3</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Pick an evaluator</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;"><code>BinaryClassificationEvaluator</code>, <code>MulticlassClassificationEvaluator</code> or <code>RegressionEvaluator</code>.</p></div></div><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;margin:20px 0 2px;">Key points</div><div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:6px;"><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M6 26 V6 M6 26 H26"/><path d="M6 26 C 11 12, 19 8, 26 7"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Metrics</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">What each one tells you</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;"><b>AUC</b> (0&#8594;1) measures separability; <b>Accuracy</b> the proportion correct; <b>Precision / Recall / F1</b> trade off error types.</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="13" cy="13" r="9"/><path d="M20 20 l7 7"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Evaluator</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Match it to the task</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Binary, multi-class and regression each have their own evaluator class.</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="10" width="16" height="12" rx="2"/><rect x="22" y="10" width="7" height="12" rx="2" fill="#FF5F46" stroke="none"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Test set</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Evaluate held-out data</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Always score on a separate test set never seen during training to avoid optimistic bias.</p></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Evaluating the Model Using the Spark ML API
# MAGIC ### Assessing performance with BinaryClassificationEvaluator
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> <code>BinaryClassificationEvaluator</code> scores a binary classifier on a ranking metric. Point it at the label and raw-prediction columns, choose the metric, and it returns a single number.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:14px;margin-bottom:18px;"><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:14px 16px 16px;"><div style="display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;"><code style="font-size:15px;font-weight:700;color:#0b2026;">labelCol</code><span style="font-size:11px;font-weight:700;letter-spacing:0.6px;text-transform:uppercase;color:#FF5F46;">column</span></div><p style="margin:9px 0 0;font-size:14px;line-height:1.55;color:#33414B;">Name of the column holding the true labels (e.g. <code>Adopted</code>).</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:14px 16px 16px;"><div style="display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;"><code style="font-size:15px;font-weight:700;color:#0b2026;">rawPredictionCol</code><span style="font-size:11px;font-weight:700;letter-spacing:0.6px;text-transform:uppercase;color:#FF5F46;">default</span></div><p style="margin:9px 0 0;font-size:14px;line-height:1.55;color:#33414B;">The model's raw prediction column (default <code>rawPrediction</code>).</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:14px 16px 16px;"><div style="display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;"><code style="font-size:15px;font-weight:700;color:#0b2026;">metricName</code><span style="font-size:11px;font-weight:700;letter-spacing:0.6px;text-transform:uppercase;color:#FF5F46;">choice</span></div><p style="margin:9px 0 0;font-size:14px;line-height:1.55;color:#33414B;"><code>areaUnderROC</code> or <code>areaUnderPR</code> (precision-recall).</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:14px 16px 16px;"><div style="display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;"><code style="font-size:15px;font-weight:700;color:#0b2026;">weightCol</code><span style="font-size:11px;font-weight:700;letter-spacing:0.6px;text-transform:uppercase;color:#FF5F46;">optional</span></div><p style="margin:9px 0 0;font-size:14px;line-height:1.55;color:#33414B;">Per-instance weights; unset means every instance weighs 1.0.</p></div></div><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 540px;min-width:460px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">from</span> pyspark.ml.evaluation <span style="color:#0B5FA5;">import</span> BinaryClassificationEvaluator
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Assuming predictions have been made on the test dataset</span>
# MAGIC predictions = lr_model.transform(test_df)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Evaluate on area under the ROC curve</span>
# MAGIC evaluator = BinaryClassificationEvaluator(labelCol=<span style="color:#2E8B57;">&quot;Adopted&quot;</span>,
# MAGIC     rawPredictionCol=<span style="color:#2E8B57;">&quot;rawPrediction&quot;</span>, metricName=<span style="color:#2E8B57;">&quot;areaUnderROC&quot;</span>)
# MAGIC roc_auc = evaluator.evaluate(predictions)
# MAGIC &#8203;
# MAGIC <span style="color:#0B5FA5;">print</span>(f<span style="color:#2E8B57;">&quot;Area Under ROC: {roc_auc:.2f}&quot;</span>)</pre></div><div style="flex:1 1 360px;min-width:320px;background:#FFFFFF;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Output</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12.5px;line-height:1.55;color:#1C2733;white-space:pre;">Area Under ROC: 0.85</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Computing Basic Predictions Using a Spark ML Model
# MAGIC ### Interpreting the prediction and probability columns
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> once a model is applied with <code>transform()</code>, each row gains a <b>prediction</b> and a <b>probability</b>. Reading them correctly is what turns model output into a decision.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:0;"><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="6" width="22" height="20" rx="2"/><path d="M10 16 h12 M10 21 h8"/><circle cx="10" cy="11" r="1.6" fill="#FF5F46" stroke="none"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">prediction</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">The predicted class</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">The class label (e.g. 0 or 1) chosen using the model's decision threshold (default 0.5).</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M6 26 V6 M6 26 H26"/><rect x="10" y="14" width="4" height="10"/><rect x="17" y="9" width="4" height="15"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">probability</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Confidence per class</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">A vector <code>[P(class 0), P(class 1)]</code> giving the model's probability for each class.</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="16" cy="16" r="12"/><path d="M16 16 L16 5 A11 11 0 0 1 25 20 Z" fill="#FF5F46" stroke="none"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">interpretation</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Reading the vector</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;"><code>[0.7, 0.3]</code> means 70% confident in class 0 and 30% in class 1 &mdash; higher means more confident.</p></div></div><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 540px;min-width:460px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#6E8B6E;font-style:italic;"># A trained model (e.g. LogisticRegressionModel) applied to test data</span>
# MAGIC predictions = lr_model.transform(test_df)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Inspect the prediction and probability columns</span>
# MAGIC predictions.select(<span style="color:#2E8B57;">&quot;AnimalID&quot;</span>, <span style="color:#2E8B57;">&quot;prediction&quot;</span>, <span style="color:#2E8B57;">&quot;probability&quot;</span>).show()</pre></div><div style="flex:1 1 300px;min-width:320px;background:#FFFFFF;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Output</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12.5px;line-height:1.55;color:#1C2733;white-space:pre;">+--------+----------+-----------+
# MAGIC |AnimalID|prediction|probability|
# MAGIC +--------+----------+-----------+
# MAGIC |       1|       0.0|[0.7, 0.3] |
# MAGIC |       2|       1.0|[0.2, 0.8] |
# MAGIC |       3|       0.0|[0.65,0.35]|
# MAGIC |       4|       1.0|[0.1, 0.9] |
# MAGIC +--------+----------+-----------+</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Reviewing a Spark ML Model's Metadata
# MAGIC ### Reproducibility and comparison through model metadata
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> a fitted model carries <b>metadata</b> &mdash; its configuration, parameters, training statistics and data schema. Reviewing it makes experiments reproducible and gives a record of exactly how the model was trained.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:0;"><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="4" width="22" height="24" rx="2"/><path d="M9 10 h14 M9 15 h14 M9 20 h9"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">What it is</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Model metadata</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Configuration, parameters, training statistics and the schema used during training.</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="16" cy="16" r="12"/><path d="M16 21 v-1 c0 -3 3 -3 3 -6 a3 3 0 0 0 -6 0"/><circle cx="16" cy="24.5" r="1.2" fill="#FF5F46" stroke="none"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Why review it</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Reproducibility</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">A detailed record of how the model was trained &mdash; parameters, data and results &mdash; so runs can be reproduced and compared.</p></div></div><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">from</span> pyspark.ml <span style="color:#0B5FA5;">import</span> PipelineModel
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Load the saved PipelineModel</span>
# MAGIC pipeline_model = PipelineModel.load(<span style="color:#2E8B57;">&quot;/path/to/pipeline_model&quot;</span>)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Inspect the stages of the pipeline</span>
# MAGIC <span style="color:#0B5FA5;">for</span> stage <span style="color:#0B5FA5;">in</span> pipeline_model.stages:
# MAGIC     <span style="color:#0B5FA5;">print</span>(stage)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># The model is the last stage in the pipeline</span>
# MAGIC lr_model = pipeline_model.stages[-1]
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Review the model parameters</span>
# MAGIC <span style="color:#0B5FA5;">print</span>(<span style="color:#2E8B57;">&quot;Model Parameters:&quot;</span>)
# MAGIC <span style="color:#0B5FA5;">print</span>(lr_model.explainParams())
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Access the training summary</span>
# MAGIC training_summary = lr_model.summary
# MAGIC <span style="color:#0B5FA5;">print</span>(<span style="color:#2E8B57;">&quot;Coefficients:&quot;</span>, training_summary.coefficientMatrix)
# MAGIC <span style="color:#0B5FA5;">print</span>(<span style="color:#2E8B57;">&quot;Intercept:&quot;</span>, training_summary.interceptVector)
# MAGIC <span style="color:#0B5FA5;">print</span>(<span style="color:#2E8B57;">&quot;Accuracy:&quot;</span>, training_summary.accuracy)</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>