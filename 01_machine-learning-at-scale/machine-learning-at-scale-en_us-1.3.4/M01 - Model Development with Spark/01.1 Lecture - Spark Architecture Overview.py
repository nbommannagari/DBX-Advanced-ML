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
# MAGIC # Lecture: Spark Architecture Overview
# MAGIC #### Machine Learning Development with Spark
# MAGIC
# MAGIC A tour of how Apache Spark is built and when it is the right engine for machine learning — its structured data APIs, the Catalyst optimizer, the cluster execution model, and the situations where Spark helps (or hurts) an ML workload.
# MAGIC
# MAGIC <div style="max-width:1180px;margin:20px 0 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><div style="background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-left:4px solid #FF5F46;border-radius:10px;padding:18px 22px;"><div style="display:flex;align-items:center;gap:12px;"><svg viewBox="0 0 32 32" width="30" height="30" aria-hidden="true" style="flex:0 0 30px;"><g fill="none" stroke="#FF5F46" stroke-width="1.7"><circle cx="16" cy="16" r="12"/><circle cx="16" cy="16" r="7"/><circle cx="16" cy="16" r="2.4" fill="#FF5F46"/></g></svg><div style="font-size:12px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#FF5F46;">Learning Objectives</div></div><p style="margin:12px 0 8px;font-size:15px;color:#33414B;">By the end of this lesson, you will be able to:</p><ul style="line-height:1.7;font-size:14.5px;color:#33414B;margin:0;padding-left:22px;"><li style="margin:4px 0;">Describe Apache Spark's architecture — its structured data APIs and the driver/executor cluster execution model.</li><li style="margin:4px 0;">Explain how the Catalyst optimizer plans and optimizes work under the DataFrame API.</li><li style="margin:4px 0;">Identify when Spark is (and isn't) the right engine for a machine learning workload.</li></ul></div></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Apache Spark
# MAGIC ### Unified open-source engine for fast, scalable data and machine learning analytics
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup><col style="width:50%"><col style="width:50%"></colgroup>
# MAGIC   <tr>
# MAGIC     <td style="font-size:1em; vertical-align:top;">
# MAGIC       <ul>
# MAGIC         <li><b>Distributed SQL engine:</b> fast, large-scale data processing with in-memory capabilities.</li>
# MAGIC         <li><b>Multiple components:</b> Spark ML, streaming, SQL DataFrames, and GraphX.</li>
# MAGIC         <li><b>Multi-language support:</b> Java, Scala, Python, and R.</li>
# MAGIC         <li><b>Versatile workflows:</b> batch, real-time, ML, and graph processing.</li>
# MAGIC         <li><b>Ecosystem integration:</b> connects with popular data science and BI tools.</li>
# MAGIC         <li><b>Open-source community:</b> driven by global collaboration and innovation.</li>
# MAGIC       </ul>
# MAGIC     </td>
# MAGIC     <td style="vertical-align:middle; text-align:center;">
# MAGIC       <img src="https://files.training.databricks.com/binder/prod_main/machine-learning-at-scale-en_us-1.3.4/images/20261023T044612Z/Includes/images/1.1_company_logos.png" alt="Company Logos" style="max-width:600px; width:100%; display:block; margin:0 auto 10px auto;" /><br>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Spark's Structured Data APIs
# MAGIC
# MAGIC <div style="overflow-x: auto; margin: 16px 0;">
# MAGIC <div style="min-width: 1040px; max-width: 1300px; margin: 0 auto; box-sizing: border-box; padding: 26px 22px 30px; background: #FFFFFF; border: 1px solid #E3E6EF; border-radius: 6px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
# MAGIC <div style="position: relative;">
# MAGIC <div style="display:flex;align-items:flex-start;"><div style="flex:0 0 23.6%;max-width:23.6%;margin-top:35px;box-sizing:border-box;border:2px solid #16405A;border-radius:18px;background:#FFFFFF;padding:14px 15px 20px;"><div style="text-align:center;font-size:21px;font-weight:800;color:#16405A;line-height:1.25;">RDD</div><div style="text-align:center;font-size:16.5px;color:#16405A;margin-top:3px;">(2011 Release 1.0)</div><div style="margin-top:14px;font-size:15.5px;font-weight:700;color:#1C2B36;">Resilient Distributed Dataset</div><ul style="list-style-type:disc;margin:4px 0 0;padding-left:20px;font-size:15.5px;line-height:1.45;"><li style="color:#1C2B36;margin:2px 0;"><span style="color:#1C2B36;">Fundamental Data Structure in Spark</span></li><li style="color:#E79A5F;margin:2px 0;"><span style="color:#1C2B36;background:#FBE3CE;padding:1px 3px;">Low Level API</span><ul style="list-style-type:circle;margin:2px 0 2px;padding-left:20px;font-size:14px;"><li style="color:#8A97A3;"><span style="color:#1C2B36;">Offers Control &amp; Flexibility</span></li><li style="color:#8A97A3;"><span style="color:#1C2B36;">Code the &ldquo;how-to&rdquo;</span></li></ul></li><li style="color:#6FB56A;margin:2px 0;"><span style="color:#1C2B36;background:#D9EFD5;padding:1px 3px;">Compile-time Syntax &amp; Analysis error detection</span></li><li style="color:#E3BE3C;margin:2px 0;"><span style="color:#1C2B36;background:#FCF2C6;padding:1px 3px;">No Schema</span></li><li style="color:#6E97D6;margin:2px 0;"><span style="color:#1C2B36;background:#D9E4F8;padding:1px 3px;">Java, Scala, Python, R</span></li></ul></div><div style="flex:0 0 2.7%;"></div><div style="flex:0 0 23.9%;max-width:23.9%;margin-top:0px;box-sizing:border-box;border:2px solid #16405A;border-radius:18px;background:#FFFFFF;padding:14px 15px 20px;"><div style="text-align:center;font-size:21px;font-weight:800;color:#16405A;line-height:1.25;">DataFrame</div><div style="text-align:center;font-size:16.5px;color:#16405A;margin-top:3px;">(2013 Release 1.3)</div><div style="margin-top:14px;"></div><ul style="list-style-type:disc;margin:4px 0 0;padding-left:20px;font-size:15.5px;line-height:1.45;"><li style="color:#E79A5F;margin:2px 0;"><span style="color:#1C2B36;background:#FBE3CE;padding:1px 3px;">High Level API</span><ul style="list-style-type:circle;margin:2px 0 2px;padding-left:20px;font-size:14px;"><li style="color:#8A97A3;"><span style="color:#1C2B36;">Code the &ldquo;what-to-do&rdquo;</span></li><li style="color:#8A97A3;"><span style="color:#1C2B36;">Ease-of use &amp; Readability</span></li></ul></li><li style="color:#E3BE3C;margin:2px 0;"><span style="color:#1C2B36;background:#FCF2C6;padding:1px 3px;">Schema: Structured Data</span></li><li style="color:#8479C4;margin:2px 0;"><span style="color:#1C2B36;background:#DEDAF1;padding:1px 3px;">Logical Plans and Optimizer</span></li><li style="color:#1C2B36;margin:2px 0;"><span style="color:#1C2B36;">SQL Support</span></li><li style="color:#6FB56A;margin:2px 0;"><span style="color:#1C2B36;background:#D9EFD5;padding:1px 3px;">Compile-time Syntax error detection</span></li><li style="color:#6FB56A;margin:2px 0;"><span style="color:#1C2B36;background:#D9EFD5;padding:1px 3px;">Run-time Analysis Error Detected</span></li><li style="color:#6E97D6;margin:2px 0;"><span style="color:#1C2B36;background:#D9E4F8;padding:1px 3px;">Java, Scala, Python, R</span></li></ul><ul style="list-style-type:disc;margin:26px 0 0;padding-left:20px;font-size:15.5px;line-height:1.45;"><li style="color:#8A97A3;"><span style="color:#8A97A3;font-style:italic;">Python Conversion e.g.</span><ul style="list-style-type:circle;margin:2px 0 0;padding-left:20px;color:#8A97A3;"><li><span style="color:#5C7A96;font-family:ui-monospace,Menlo,Consolas,monospace;font-style:italic;">&rarr; &nbsp;df = rdd.toDF()</span></li><li><span style="color:#5C7A96;font-family:ui-monospace,Menlo,Consolas,monospace;font-style:italic;">&larr; rdd = df.rdd</span></li></ul></li></ul></div><div style="flex:0 0 2.4%;"></div><div style="flex:0 0 23.0%;max-width:23.0%;margin-top:150px;box-sizing:border-box;border:2px solid #16405A;border-radius:18px;background:#FFFFFF;padding:14px 15px 20px;"><div style="text-align:center;font-size:21px;font-weight:800;color:#16405A;line-height:1.25;">Dataset</div><div style="text-align:center;font-size:16.5px;color:#16405A;margin-top:3px;">(2015 Release 1.6)</div><div style="margin-top:14px;font-size:15.5px;font-weight:700;color:#1C2B36;">Best of RDD + Best of DataFrame</div><ul style="list-style-type:disc;margin:4px 0 0;padding-left:20px;font-size:15.5px;line-height:1.45;"><li style="color:#E79A5F;margin:2px 0;"><span style="color:#1C2B36;background:#FBE3CE;padding:1px 3px;">High Level API</span></li><li style="color:#6FB56A;margin:2px 0;"><span style="color:#1C2B36;background:#D9EFD5;padding:1px 3px;">Compile-time Syntax &amp; Analysis error detection</span></li><li style="color:#E3BE3C;margin:2px 0;"><span style="color:#1C2B36;background:#FCF2C6;padding:1px 3px;">Schema: Structured Data</span></li><li style="color:#8479C4;margin:2px 0;"><span style="color:#1C2B36;background:#DEDAF1;padding:1px 3px;">Logical Plans and Optimizer</span></li><li style="color:#1C2B36;margin:2px 0;"><span style="color:#1C2B36;">More memory-efficient</span></li><li style="color:#6E97D6;margin:2px 0;"><span style="color:#1C2B36;background:#D9E4F8;padding:1px 3px;">Java, Scala</span></li><li style="color:#1C2B36;margin:2px 0;"><span style="color:#1C2B36;font-style:italic;">Slower than DataFrames</span></li></ul></div><div style="flex:0 0 2.2%;"></div><div style="flex:0 0 22.2%;max-width:22.2%;margin-top:13px;box-sizing:border-box;border:2px solid #16405A;border-radius:18px;background:#FFFFFF;padding:14px 15px 20px;"><div style="text-align:center;font-size:21px;font-weight:800;color:#16405A;line-height:1.25;">Unifying DataFrames<br>and Datasets</div><div style="text-align:center;font-size:16.5px;color:#16405A;margin-top:3px;">(2016 Release 2.0)</div><div style="text-align:center;margin-top:18px;font-size:16.5px;font-weight:400;color:#1C2B36;line-height:1.45;">Dataset and DataFrame<br><strong>merge into one unit</strong><br>to reduce the complexity</div><div style="text-align:center;margin-top:22px;font-size:17px;font-weight:700;color:#1C2B36;line-height:1.45;">Dataset API takes on two forms</div><div style="text-align:center;margin-top:10px;font-size:16.5px;font-weight:400;color:#1C2B36;line-height:1.45;"><span style="background:#D9EFD5;padding:1px 4px;">Strongly-Typed API</span></div><div style="text-align:center;margin-top:2px;font-size:16.5px;font-weight:400;color:#1C2B36;line-height:1.45;"><span style="background:#D9E4F8;padding:1px 4px;font-style:italic;">(Java and Scala)</span></div><div style="text-align:center;margin-top:8px;font-size:16.5px;font-weight:400;color:#1C2B36;line-height:1.45;">Untyped API</div><div style="text-align:center;margin-top:2px;font-size:16.5px;font-weight:400;color:#1C2B36;line-height:1.45;"><span style="background:#D9E4F8;padding:1px 4px;font-style:italic;">(Python and R)</span></div></div></div>
# MAGIC <div style="position:absolute;left:23.6%;width:2.7%;top:274px;height:12px;" aria-hidden="true"><div style="position:absolute;left:0;right:7px;top:5px;border-top:2px solid #8A97A3;"></div><div style="position:absolute;right:0;top:1px;width:0;height:0;border-left:9px solid #8A97A3;border-top:5px solid transparent;border-bottom:5px solid transparent;"></div></div><div style="position:absolute;left:50.2%;width:27.6%;top:128px;height:12px;" aria-hidden="true"><div style="position:absolute;left:0;right:7px;top:5px;border-top:2px solid #8A97A3;"></div><div style="position:absolute;right:0;top:1px;width:0;height:0;border-left:9px solid #8A97A3;border-top:5px solid transparent;border-bottom:5px solid transparent;"></div></div><div style="position:absolute;left:75.6%;width:2.2%;top:360px;height:12px;" aria-hidden="true"><div style="position:absolute;left:0;right:7px;top:5px;border-top:2px solid #8A97A3;"></div><div style="position:absolute;right:0;top:1px;width:0;height:0;border-left:9px solid #8A97A3;border-top:5px solid transparent;border-bottom:5px solid transparent;"></div></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Under the Catalyst Optimizer's Hood
# MAGIC
# MAGIC <div style="max-width: 1300px; margin: 0 auto; overflow-x: auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1300 330" role="img" style="min-width: 1000px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
# MAGIC   <title>Spark Catalyst optimizer pipeline</title>
# MAGIC   <desc>A SQL query or DataFrame operation is turned into an unresolved logical plan, analyzed into a logical plan, optimized, planned into several candidate physical plans, costed to select one physical plan, and code-generated into RDDs.</desc>
# MAGIC   <defs>
# MAGIC     <marker id="cat-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#9AA3A9" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC   </defs>
# MAGIC   <g font-size="19" fill="#1C2733" text-anchor="middle">
# MAGIC     <text x="399" y="32">Analysis</text>
# MAGIC     <text x="563" y="19">Logical</text>
# MAGIC     <text x="563" y="45">Optimization</text>
# MAGIC     <text x="733" y="19">Physical</text>
# MAGIC     <text x="733" y="45">Planning</text>
# MAGIC     <text x="1152" y="19">Code</text>
# MAGIC     <text x="1152" y="45">Generation</text>
# MAGIC   </g>
# MAGIC   <rect x="38" y="18" width="172" height="126" rx="10" fill="#4CAE7A"/>
# MAGIC   <text x="124" y="88" text-anchor="middle" font-size="18" font-weight="700" fill="#FFFFFF">SQL Query</text>
# MAGIC   <rect x="38" y="172" width="172" height="126" rx="10" fill="#4CAE7A"/>
# MAGIC   <text x="124" y="228" text-anchor="middle" font-size="18" font-weight="700" fill="#FFFFFF">DataFrame</text>
# MAGIC   <text x="124" y="252" text-anchor="middle" font-size="18" font-weight="700" fill="#FFFFFF">Operation</text>
# MAGIC   <rect x="240" y="88" width="144" height="124" rx="10" fill="#ED6A4B"/>
# MAGIC   <text x="312" y="143" text-anchor="middle" font-size="17" font-weight="700" fill="#FFFFFF">Unresolved</text>
# MAGIC   <text x="312" y="166" text-anchor="middle" font-size="17" font-weight="700" fill="#FFFFFF">Logical Plan</text>
# MAGIC   <rect x="409" y="88" width="144" height="124" rx="10" fill="#ED6A4B"/>
# MAGIC   <text x="481" y="143" text-anchor="middle" font-size="17" font-weight="700" fill="#FFFFFF">Logical</text>
# MAGIC   <text x="481" y="166" text-anchor="middle" font-size="17" font-weight="700" fill="#FFFFFF">Plan</text>
# MAGIC   <rect x="578" y="88" width="144" height="124" rx="10" fill="#ED6A4B"/>
# MAGIC   <text x="650" y="132" text-anchor="middle" font-size="17" font-weight="700" fill="#FFFFFF">Optimized</text>
# MAGIC   <text x="650" y="155" text-anchor="middle" font-size="17" font-weight="700" fill="#FFFFFF">Logical</text>
# MAGIC   <text x="650" y="178" text-anchor="middle" font-size="17" font-weight="700" fill="#FFFFFF">Plan</text>
# MAGIC   <rect x="754" y="74" width="143" height="124" rx="10" fill="#4E7387"/>
# MAGIC   <rect x="748" y="81" width="143" height="124" rx="10" fill="#4E7387"/>
# MAGIC   <rect x="742" y="88" width="143" height="124" rx="10" fill="#ED6A4B"/>
# MAGIC   <text x="813" y="143" text-anchor="middle" font-size="17" font-weight="700" fill="#FFFFFF">Physical</text>
# MAGIC   <text x="813" y="166" text-anchor="middle" font-size="17" font-weight="700" fill="#FFFFFF">Plans</text>
# MAGIC   <rect x="916" y="64" width="58" height="172" rx="8" fill="#B7BBBD"/>
# MAGIC   <text x="945" y="150" text-anchor="middle" font-size="16" font-weight="700" fill="#FFFFFF" transform="rotate(-90 945 150)">Cost Model</text>
# MAGIC   <rect x="1000" y="88" width="144" height="124" rx="10" fill="#ED6A4B"/>
# MAGIC   <text x="1072" y="132" text-anchor="middle" font-size="17" font-weight="700" fill="#FFFFFF">Selected</text>
# MAGIC   <text x="1072" y="155" text-anchor="middle" font-size="17" font-weight="700" fill="#FFFFFF">Physical</text>
# MAGIC   <text x="1072" y="178" text-anchor="middle" font-size="17" font-weight="700" fill="#FFFFFF">Plan</text>
# MAGIC   <rect x="1160" y="88" width="100" height="124" rx="10" fill="#4CAE7A"/>
# MAGIC   <text x="1210" y="156" text-anchor="middle" font-size="18" font-weight="700" fill="#FFFFFF">RDDs</text>
# MAGIC   <g stroke="#9AA3A9" stroke-width="2.2" fill="none">
# MAGIC     <path d="M212 92 C 226 104, 226 118, 236 128" marker-end="url(#cat-ar)"/>
# MAGIC     <path d="M212 224 C 226 210, 226 184, 236 172" marker-end="url(#cat-ar)"/>
# MAGIC     <path d="M386 150 H 404" marker-end="url(#cat-ar)"/>
# MAGIC     <path d="M555 150 H 573" marker-end="url(#cat-ar)"/>
# MAGIC     <path d="M724 150 H 738" marker-end="url(#cat-ar)"/>
# MAGIC     <path d="M899 104 C 907 112, 908 120, 912 127" marker-end="url(#cat-ar)"/>
# MAGIC     <path d="M887 168 C 898 168, 903 158, 912 152" marker-end="url(#cat-ar)"/>
# MAGIC     <path d="M976 150 H 995" marker-end="url(#cat-ar)"/>
# MAGIC     <path d="M1146 150 H 1155" marker-end="url(#cat-ar)"/>
# MAGIC   </g>
# MAGIC </svg>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Spark Cluster
# MAGIC
# MAGIC <div style="font-size:1.23em; max-width: 1300px; margin: 0 auto; overflow-x: auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1300 640" role="img" style="min-width: 1000px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
# MAGIC   <title>Spark cluster architecture</title>
# MAGIC   <desc>The Driver Program with its SparkContext submits Spark jobs; the Cluster Manager with its Scheduler allocates resources and launches executors on worker nodes, each executor holding a cache and task threads.</desc>
# MAGIC   <defs>
# MAGIC     <marker id="sc-solid" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC     <marker id="sc-dash" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#5A8194" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></marker>
# MAGIC     <symbol id="sc-worker" viewBox="0 0 330 150">
# MAGIC       <rect x="1" y="1" width="328" height="148" rx="10" fill="#0B2027"/>
# MAGIC       <text x="165" y="31" text-anchor="middle" font-size="16" font-weight="700" fill="#FFFFFF">Worker Node &#8212; Spark Executor</text>
# MAGIC       <rect x="25" y="42" width="280" height="30" rx="5" fill="#1CA971"/>
# MAGIC       <text x="165" y="63" text-anchor="middle" font-size="14" font-weight="700" fill="#FFFFFF">Cache</text>
# MAGIC       <rect x="25" y="82" width="132" height="52" rx="6" fill="#F2A93B"/>
# MAGIC       <text x="91" y="114" text-anchor="middle" font-size="14" font-weight="700" fill="#1C2733">Task Thread</text>
# MAGIC       <rect x="173" y="82" width="132" height="52" rx="6" fill="#F2A93B"/>
# MAGIC       <text x="239" y="114" text-anchor="middle" font-size="14" font-weight="700" fill="#1C2733">Task Thread</text>
# MAGIC     </symbol>
# MAGIC   </defs>
# MAGIC   <text x="20" y="120" font-size="17" font-weight="700" fill="#1B3139">Driver Program</text>
# MAGIC   <g font-size="14.5" fill="#1C2733">
# MAGIC     <text x="20" y="150">&#8226; Entry point for your Spark application</text>
# MAGIC     <text x="20" y="176">&#8226; Submits Spark jobs to the cluster</text>
# MAGIC   </g>
# MAGIC   <text x="20" y="222" font-size="17" font-weight="700" fill="#1B3139">Cluster Manager</text>
# MAGIC   <g font-size="14.5" fill="#1C2733">
# MAGIC     <text x="20" y="252">&#8226; Allocates resources across the Workers</text>
# MAGIC     <text x="20" y="278">&#8226; Launches Executors on the nodes</text>
# MAGIC     <text x="20" y="304">&#8226; Handles the distribution of tasks</text>
# MAGIC   </g>
# MAGIC   <text x="20" y="356" font-size="17" font-weight="700" fill="#1B3139">Executor components</text>
# MAGIC   <g font-size="14" fill="#1C2733">
# MAGIC     <text x="20" y="386">&#8226; <tspan font-weight="700">Worker node:</tspan> executes tasks and</text>
# MAGIC     <text x="34" y="406">returns results to the Driver</text>
# MAGIC     <text x="20" y="434">&#8226; <tspan font-weight="700">Spark Executor:</tspan> JVM process running</text>
# MAGIC     <text x="34" y="454">tasks (e.g. map, reduce, filter)</text>
# MAGIC     <text x="20" y="482">&#8226; <tspan font-weight="700">Cache:</tspan> stores data locally to speed up reuse</text>
# MAGIC     <text x="20" y="510">&#8226; <tspan font-weight="700">Task Thread:</tspan> one thread per CPU core</text>
# MAGIC   </g>
# MAGIC   <rect x="380" y="110" width="250" height="110" rx="10" fill="#FFFFFF" stroke="#1B3139" stroke-width="2.5"/>
# MAGIC   <text x="505" y="150" text-anchor="middle" font-size="18" font-weight="700" fill="#1B3139">Driver Program</text>
# MAGIC   <rect x="410" y="162" width="190" height="42" rx="6" fill="#5A8194"/>
# MAGIC   <text x="505" y="189" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">SparkContext</text>
# MAGIC   <rect x="380" y="330" width="250" height="110" rx="10" fill="#FFFFFF" stroke="#1B3139" stroke-width="2.5"/>
# MAGIC   <text x="505" y="370" text-anchor="middle" font-size="18" font-weight="700" fill="#1B3139">Cluster Manager</text>
# MAGIC   <rect x="410" y="382" width="190" height="42" rx="6" fill="#5A8194"/>
# MAGIC   <text x="505" y="409" text-anchor="middle" font-size="15" font-weight="700" fill="#FFFFFF">Scheduler</text>
# MAGIC   <use href="#sc-worker" x="940" y="70" width="330" height="150"/>
# MAGIC   <use href="#sc-worker" x="940" y="250" width="330" height="150"/>
# MAGIC   <use href="#sc-worker" x="940" y="430" width="330" height="150"/>
# MAGIC   <g stroke="#1B3139" stroke-width="2.5" fill="none">
# MAGIC     <path d="M632 150 C 760 146, 820 116, 934 111" marker-end="url(#sc-solid)"/>
# MAGIC     <path d="M632 168 C 780 200, 800 272, 934 300" marker-end="url(#sc-solid)"/>
# MAGIC     <path d="M632 186 C 780 262, 800 424, 934 480" marker-end="url(#sc-solid)"/>
# MAGIC   </g>
# MAGIC   <g stroke="#5A8194" stroke-width="2.5" fill="none" stroke-dasharray="8 6">
# MAGIC     <path d="M632 368 C 780 340, 800 220, 934 181" marker-end="url(#sc-dash)"/>
# MAGIC     <path d="M632 385 C 780 380, 800 362, 934 356" marker-end="url(#sc-dash)"/>
# MAGIC     <path d="M632 402 C 780 432, 800 520, 934 545" marker-end="url(#sc-dash)"/>
# MAGIC   </g>
# MAGIC   <g font-size="13.5" font-style="italic" text-anchor="middle">
# MAGIC     <text x="505" y="246" fill="#5A6B73">submits jobs</text>
# MAGIC     <text x="505" y="466" fill="#5A8194">allocates resources</text>
# MAGIC   </g>
# MAGIC </svg>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## When is Spark Recommended for ML?
# MAGIC ### Scaling Out or Speeding Up
# MAGIC
# MAGIC <br>
# MAGIC <table style="width:100%; border-collapse:separate; border-spacing:0;">
# MAGIC   <tr>
# MAGIC <td style="width:50%; vertical-align:top; padding:8px;"><div style="background:#1B5162; color:#fff; padding:16px 20px; font-size:1.15em; font-weight:700; text-align:center; border-radius:6px 6px 0 0;">Large-Scale Data Processing</div><div style="background:#C6D0D6; color:#1c2733; padding:20px 24px; border-radius:0 0 6px 6px; min-height:150px; font-size:1.13em;"><ul style="margin:0; padding-left:24px; line-height:1.85;"><li>Excels at processing massive datasets needing distributed computation.</li><li><b>Benefit:</b> handles data that can't fit on a single machine — reduces or removes the need to sample down your dataset.</li></ul></div></td><td style="width:50%; vertical-align:top; padding:8px;"><div style="background:#1B5162; color:#fff; padding:16px 20px; font-size:1.15em; font-weight:700; text-align:center; border-radius:6px 6px 0 0;">Distributed Model Training</div><div style="background:#C6D0D6; color:#1c2733; padding:20px 24px; border-radius:0 0 6px 6px; min-height:150px; font-size:1.13em;"><ul style="margin:0; padding-left:24px; line-height:1.85;"><li>Distributes model training and hyperparameter tuning across multiple nodes.</li><li><b>Benefit:</b> significantly reduces training time using cluster computing.</li></ul></div></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC <td style="width:50%; vertical-align:top; padding:8px;"><div style="background:#1B5162; color:#fff; padding:16px 20px; font-size:1.15em; font-weight:700; text-align:center; border-radius:6px 6px 0 0;">Real-Time Data Processing &amp; Streaming Analytics</div><div style="background:#C6D0D6; color:#1c2733; padding:20px 24px; border-radius:0 0 6px 6px; min-height:150px; font-size:1.13em;"><ul style="margin:0; padding-left:24px; line-height:1.85;"><li>Integrates with Spark Streaming to apply ML models to real-time data streams.</li><li><b>Benefit:</b> enables real-time predictions and analytics, e.g. fraud detection and monitoring.</li></ul></div></td><td style="width:50%; vertical-align:top; padding:8px;"><div style="background:#1B5162; color:#fff; padding:16px 20px; font-size:1.15em; font-weight:700; text-align:center; border-radius:6px 6px 0 0;">End-to-End Machine Learning Pipelines</div><div style="background:#C6D0D6; color:#1c2733; padding:20px 24px; border-radius:0 0 6px 6px; min-height:150px; font-size:1.13em;"><ul style="margin:0; padding-left:24px; line-height:1.85;"><li>Supports the full machine learning lifecycle.</li><li><b>Benefit:</b> simplifies ML workflows with its Pipeline API, automating complex processes.</li></ul></div></td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## When is Spark <u>Not</u> Recommended for ML?
# MAGIC
# MAGIC <br>
# MAGIC <table style="width:100%; border-collapse:separate; border-spacing:0;">
# MAGIC   <tr>
# MAGIC <td style="width:50%; vertical-align:top; padding:8px;"><div style="background:#8B3A2E; color:#fff; padding:16px 20px; font-size:1.22em; font-weight:700; text-align:center; border-radius:6px 6px 0 0;">Small to Moderate-Sized Datasets</div><div style="background:#C6D0D6; color:#1c2733; padding:20px 24px; border-radius:0 0 6px 6px; min-height:150px; font-size:1.13em;"><ul style="margin:0; padding-left:22px; line-height:1.8;"><li>Spark adds unnecessary complexity for small datasets that fit on a single machine.</li><li><b>Alternative:</b> use scikit-learn or TensorFlow on one machine.</li></ul></div></td><td style="width:50%; vertical-align:top; padding:8px;"><div style="background:#8B3A2E; color:#fff; padding:16px 20px; font-size:1.22em; font-weight:700; text-align:center; border-radius:6px 6px 0 0;">Low-Latency, Real-Time Predictions</div><div style="background:#C6D0D6; color:#1c2733; padding:20px 24px; border-radius:0 0 6px 6px; min-height:150px; font-size:1.13em;"><ul style="margin:0; padding-left:22px; line-height:1.8;"><li>Spark isn't optimized for microsecond or millisecond response times.</li><li><b>Alternative:</b> use Databricks Model Serving or a similar option.</li></ul></div></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC <td style="width:50%; vertical-align:top; padding:8px;"><div style="background:#8B3A2E; color:#fff; padding:16px 20px; font-size:1.22em; font-weight:700; text-align:center; border-radius:6px 6px 0 0;">Short-Lived Model Training or Batch Scoring</div><div style="background:#C6D0D6; color:#1c2733; padding:20px 24px; border-radius:0 0 6px 6px; min-height:150px; font-size:1.13em;"><ul style="margin:0; padding-left:22px; line-height:1.8;"><li>Time to initialize a Spark session and distribute tasks can outweigh the benefits.</li><li><b>Alternative:</b> use scikit-learn, XGBoost, or Docker for lightweight execution.</li></ul></div></td><td style="width:50%; vertical-align:top; padding:8px;"><div style="background:#8B3A2E; color:#fff; padding:16px 20px; font-size:1.22em; font-weight:700; text-align:center; border-radius:6px 6px 0 0;">Strict Data Privacy / Compliance</div><div style="background:#C6D0D6; color:#1c2733; padding:20px 24px; border-radius:0 0 6px 6px; min-height:150px; font-size:1.13em;"><ul style="margin:0; padding-left:22px; line-height:1.8;"><li>Distributed processing complicates compliance with data privacy regulations.</li><li><b>Alternative:</b> use on-premises tools or controlled environments.</li></ul></div></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC <td style="width:50%; vertical-align:top; padding:8px;"><div style="background:#8B3A2E; color:#fff; padding:16px 20px; font-size:1.22em; font-weight:700; text-align:center; border-radius:6px 6px 0 0;">Highly Custom Algorithms</div><div style="background:#C6D0D6; color:#1c2733; padding:20px 24px; border-radius:0 0 6px 6px; min-height:150px; font-size:1.13em;"><ul style="margin:0; padding-left:22px; line-height:1.8;"><li>Custom algorithm development in Spark can be complex and inefficient.</li><li><b>Alternative:</b> use Python, scikit-learn, or R for more flexibility.</li></ul></div></td><td style="width:50%; padding:8px;"></td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>