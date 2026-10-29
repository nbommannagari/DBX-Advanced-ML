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
# MAGIC # Lecture - Model Rollout Strategies with Databricks
# MAGIC
# MAGIC Releasing a new model into production is a decision, not just a deployment. This lecture frames model rollout as a champion-versus-challenger choice and compares the major rollout strategies — shadow, rolling, blue-green, canary, and A/B testing — including the A/B testing framework and its metrics. It then shows how Databricks Model Serving supports online evaluation by serving multiple models from a single endpoint.
# MAGIC
# MAGIC <br>
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this lecture, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Describe various rollout strategies: shadow, rolling, blue-green, canary, A/B testing.</li>
# MAGIC         <li>Describe A/B testing framework and metrics.</li>
# MAGIC         <li>Understand how Model Serving solves key challenges for model rollout strategies.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Model Rollout
# MAGIC
# MAGIC <div style="max-width: 1280px; margin: 0 auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1280 764" role="img" style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
# MAGIC   <title>Model Rollout: the ML lifecycle and champion/challenger decision</title>
# MAGIC   <desc>The ML lifecycle runs Code, Build, Train, Test, Release, Deploy, Inference, Monitor, with the Release and Deploy stages highlighted as the rollout phase. An application calls a Model Serving endpoint, which routes to the Champion model in production while a new Challenger model is evaluated to decide whether to replace it.</desc>
# MAGIC   <defs>
# MAGIC     <marker id="s67-g" viewBox="0 0 10 10" refX="7" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="#B4BCC2"/></marker>
# MAGIC     <marker id="s67-r" viewBox="0 0 10 10" refX="7" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="#FF5F46"/></marker>
# MAGIC     <marker id="s67-navy" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-linecap="round"/></marker>
# MAGIC     <marker id="s67-rd" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#FF5F46" stroke-width="1.8" stroke-linecap="round"/></marker>
# MAGIC     <marker id="s67-fb" viewBox="0 0 10 10" refX="7" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="#1B5162"/></marker>
# MAGIC     <symbol id="ic-code" viewBox="0 0 100 100"><g stroke="#1B3139" stroke-width="4.6" fill="none" stroke-linecap="round" stroke-linejoin="round"><rect x="8" y="14" width="84" height="58" rx="6"/><path d="M40 82 h20 M50 72 v10"/><path d="M34 32 l-12 11 l12 11 M66 32 l12 11 l-12 11 M58 30 l-16 26"/></g></symbol>
# MAGIC     <symbol id="ic-build" viewBox="0 0 100 100"><g stroke="#1B3139" stroke-width="4.6" fill="none" stroke-linecap="round" stroke-linejoin="round"><path d="M20 22 a10 10 0 0 0 14 14 l30 30 a6 6 0 0 0 8 -8 l-30 -30 a10 10 0 0 0 -14 -14 z"/><path d="M74 20 l-14 14 6 6 14 -14 M78 24 l-32 44 a6 6 0 0 1 -8 -8 l44 -32"/></g></symbol>
# MAGIC     <symbol id="ic-train" viewBox="0 0 100 100"><g stroke="#1B3139" stroke-width="4.6" fill="none" stroke-linecap="round" stroke-linejoin="round"><path d="M30 34 l20 -11 l20 11 v22 l-20 11 l-20 -11 z M30 34 l20 11 l20 -11 M50 45 v22"/><path d="M20 70 a34 34 0 0 0 60 6 M80 80 l-2 -12 -12 4"/></g></symbol>
# MAGIC     <symbol id="ic-test" viewBox="0 0 100 100"><g stroke="#1B3139" stroke-width="4.6" fill="none" stroke-linecap="round" stroke-linejoin="round"><path d="M18 78 l6 -22 l40 -40 a8 8 0 0 1 12 12 l-40 40 z M60 20 l12 12"/></g></symbol>
# MAGIC     <symbol id="ic-release" viewBox="0 0 100 100"><g stroke="#1B3139" stroke-width="4.6" fill="none" stroke-linecap="round" stroke-linejoin="round"><rect x="20" y="20" width="60" height="66" rx="6"/><rect x="36" y="12" width="28" height="16" rx="4"/><path d="M34 52 l10 10 l18 -22"/></g></symbol>
# MAGIC     <symbol id="ic-deploy" viewBox="0 0 100 100"><g stroke="#1B3139" stroke-width="4.6" fill="none" stroke-linecap="round" stroke-linejoin="round"><path d="M22 34 h16 a12 12 0 0 1 12 12 a12 12 0 0 0 12 12 h16"/><circle cx="18" cy="34" r="7"/><circle cx="82" cy="58" r="7"/></g></symbol>
# MAGIC     <symbol id="ic-infer" viewBox="0 0 100 100"><g stroke="#1B3139" stroke-width="4.6" fill="none" stroke-linecap="round" stroke-linejoin="round"><rect x="14" y="16" width="72" height="64" rx="6"/><path d="M28 34 l14 14 M42 34 l-14 14 M60 32 h16 M60 44 h16 M28 62 h44"/></g></symbol>
# MAGIC     <symbol id="ic-monitor" viewBox="0 0 100 100"><g stroke="#1B3139" stroke-width="4.6" fill="none" stroke-linecap="round" stroke-linejoin="round"><path d="M22 20 h40 l16 16 v44 a4 4 0 0 1 -4 4 h-52 a4 4 0 0 1 -4 -4 v-56 a4 4 0 0 1 4 -4 z"/><path d="M62 20 v16 h16 M34 52 h30 M34 66 h20"/></g></symbol>
# MAGIC     <symbol id="ic-brain" viewBox="0 0 100 100"><g stroke-width="4" fill="none" stroke-linecap="round" stroke-linejoin="round"><path d="M40 22 a14 14 0 0 0 -14 14 a12 12 0 0 0 -4 22 a12 12 0 0 0 14 16 a14 14 0 0 0 28 0 a12 12 0 0 0 14 -16 a12 12 0 0 0 -4 -22 a14 14 0 0 0 -14 -14 a14 14 0 0 0 -26 0z"/><circle cx="30" cy="46" r="3.5"/><circle cx="70" cy="46" r="3.5"/><circle cx="50" cy="70" r="3.5"/><path d="M50 36 v10 M33 49 l14 18 M67 49 l-14 18 M18 52 h9 M73 52 h9"/></g></symbol>
# MAGIC   </defs>
# MAGIC   <text x="24" y="52" font-size="46" font-weight="800" fill="#1B3139">Model Rollout</text>
# MAGIC   <path d="M632 112 Q632 94 650 94 H896 Q914 94 914 112" fill="none" stroke="#FF5F46" stroke-width="4" stroke-linecap="round"/>
# MAGIC   <path d="M773 94 V78" fill="none" stroke="#FF5F46" stroke-width="4" stroke-linecap="round"/>
# MAGIC   <text x="773" y="66" text-anchor="middle" font-size="23" font-weight="800" fill="#FF5F46">Rollout</text>
# MAGIC   <rect x="24" y="120" width="130" height="54" rx="8" fill="#1B3A4B"/><text x="89" y="154" text-anchor="middle" font-size="20" fill="#FFFFFF">Code</text>
# MAGIC   <rect x="57" y="182" width="64" height="64" rx="10" fill="#FFFFFF" stroke="#C7CDD2" stroke-width="1.5"/><use href="#ic-code" x="62" y="187" width="54" height="54"/>
# MAGIC   <rect x="176" y="120" width="130" height="54" rx="8" fill="#1B3A4B"/><text x="241" y="154" text-anchor="middle" font-size="20" fill="#FFFFFF">Build</text>
# MAGIC   <rect x="209" y="182" width="64" height="64" rx="10" fill="#FFFFFF" stroke="#C7CDD2" stroke-width="1.5"/><use href="#ic-build" x="214" y="187" width="54" height="54"/>
# MAGIC   <rect x="328" y="120" width="130" height="54" rx="8" fill="#6E6E6E"/><text x="393" y="154" text-anchor="middle" font-size="20" fill="#FFFFFF">Train</text>
# MAGIC   <rect x="361" y="182" width="64" height="64" rx="10" fill="#FFFFFF" stroke="#C7CDD2" stroke-width="1.5"/><use href="#ic-train" x="366" y="187" width="54" height="54"/>
# MAGIC   <rect x="480" y="120" width="130" height="54" rx="8" fill="#5A8194"/><text x="545" y="154" text-anchor="middle" font-size="20" fill="#FFFFFF">Test</text>
# MAGIC   <rect x="513" y="182" width="64" height="64" rx="10" fill="#FFFFFF" stroke="#C7CDD2" stroke-width="1.5"/><use href="#ic-test" x="518" y="187" width="54" height="54"/>
# MAGIC   <rect x="632" y="120" width="130" height="54" rx="8" fill="#5A8194"/><text x="697" y="154" text-anchor="middle" font-size="20" fill="#FFFFFF">Release</text>
# MAGIC   <rect x="665" y="182" width="64" height="64" rx="10" fill="#FFFFFF" stroke="#C7CDD2" stroke-width="1.5"/><use href="#ic-release" x="670" y="187" width="54" height="54"/>
# MAGIC   <rect x="784" y="120" width="130" height="54" rx="8" fill="#5A8194"/><text x="849" y="154" text-anchor="middle" font-size="20" fill="#FFFFFF">Deploy</text>
# MAGIC   <rect x="817" y="182" width="64" height="64" rx="10" fill="#FFFFFF" stroke="#C7CDD2" stroke-width="1.5"/><use href="#ic-deploy" x="822" y="187" width="54" height="54"/>
# MAGIC   <rect x="936" y="120" width="130" height="54" rx="8" fill="#5A8194"/><text x="1001" y="154" text-anchor="middle" font-size="20" fill="#FFFFFF">Inference</text>
# MAGIC   <rect x="969" y="182" width="64" height="64" rx="10" fill="#FFFFFF" stroke="#C7CDD2" stroke-width="1.5"/><use href="#ic-infer" x="974" y="187" width="54" height="54"/>
# MAGIC   <rect x="1088" y="120" width="130" height="54" rx="8" fill="#A6AEB3"/><text x="1153" y="154" text-anchor="middle" font-size="20" fill="#FFFFFF">Monitor</text>
# MAGIC   <rect x="1121" y="182" width="64" height="64" rx="10" fill="#FFFFFF" stroke="#C7CDD2" stroke-width="1.5"/><use href="#ic-monitor" x="1126" y="187" width="54" height="54"/>
# MAGIC   <g stroke-width="6" fill="none">
# MAGIC     <path d="M158 147 h10" stroke="#B4BCC2" marker-end="url(#s67-g)"/>
# MAGIC     <path d="M310 147 h10" stroke="#B4BCC2" marker-end="url(#s67-g)"/>
# MAGIC     <path d="M462 147 h10" stroke="#B4BCC2" marker-end="url(#s67-g)"/>
# MAGIC     <path d="M614 147 h10" stroke="#FF5F46" marker-end="url(#s67-r)"/>
# MAGIC     <path d="M766 147 h10" stroke="#FF5F46" marker-end="url(#s67-r)"/>
# MAGIC     <path d="M918 147 h10" stroke="#B4BCC2" marker-end="url(#s67-g)"/>
# MAGIC     <path d="M1070 147 h10" stroke="#B4BCC2" marker-end="url(#s67-g)"/>
# MAGIC   </g>
# MAGIC   <g stroke="#1B5162" stroke-width="2.4" fill="none" stroke-dasharray="7 5">
# MAGIC     <path d="M1153 250 Q1153 302 1101 302 H128 Q76 302 76 260" marker-end="url(#s67-fb)"/>
# MAGIC     <path d="M545 250 Q545 280 493 280 H154 Q102 280 102 260" marker-end="url(#s67-fb)"/>
# MAGIC   </g>
# MAGIC   <rect x="40" y="330" width="470" height="250" rx="4" fill="#5A8194"/>
# MAGIC   <text x="275" y="386" text-anchor="middle" font-size="30" font-weight="800" fill="#FFFFFF">Objective</text>
# MAGIC   <text x="70" y="432" font-size="24" fill="#FFFFFF"><tspan x="70" dy="0">Determine whether to continue</tspan><tspan x="70" dy="34">with the current production model</tspan><tspan x="70" dy="34">(Champion Model) or replace it</tspan><tspan x="70" dy="34">with the new version (Challenger</tspan><tspan x="70" dy="34">Model).</tspan></text>
# MAGIC   <g stroke="#1B3139" stroke-width="3.5" fill="none" stroke-linecap="round" stroke-linejoin="round">
# MAGIC     <rect x="596" y="356" width="86" height="60" rx="5"/>
# MAGIC     <path d="M639 416 v10 M619 426 h40"/>
# MAGIC     <circle cx="620" cy="378" r="8"/>
# MAGIC     <path d="M638 372 h30 M638 386 h30 M610 400 h58"/>
# MAGIC     <rect x="694" y="372" width="34" height="56" rx="6"/>
# MAGIC     <path d="M703 382 h16"/>
# MAGIC   </g>
# MAGIC   <circle cx="711" cy="419" r="2.8" fill="#1B3139"/>
# MAGIC   <text x="662" y="462" text-anchor="middle" font-size="22" fill="#1B3139">Application</text>
# MAGIC   <path d="M742 400 H786" stroke="#1B3139" stroke-width="4" fill="none" marker-end="url(#s67-navy)"/>
# MAGIC   <g stroke="#1B3139" stroke-width="3.5" fill="none" stroke-linecap="round" stroke-linejoin="round">
# MAGIC     <path d="M812 366 h72 v52 h-72 z M800 430 h96 l-6 -12 h-84 z"/>
# MAGIC     <rect x="836" y="388" width="24" height="20" rx="3"/><path d="M840 388 v-6 a8 8 0 0 1 16 0 v6"/>
# MAGIC   </g>
# MAGIC   <text x="848" y="466" text-anchor="middle" font-size="22" fill="#1B3139"><tspan x="848" dy="0">Model Serving</tspan><tspan x="848" dy="26">Endpoint</tspan></text>
# MAGIC   <path d="M902 398 H1050" stroke="#1B3139" stroke-width="4" fill="none" marker-end="url(#s67-navy)"/>
# MAGIC   <path d="M965 400 V626 H1050" stroke="#FF5F46" stroke-width="4" fill="none" stroke-dasharray="8 6" marker-end="url(#s67-rd)"/>
# MAGIC   <rect x="1064" y="350" width="92" height="92" rx="16" fill="#8A959C"/>
# MAGIC   <use href="#ic-brain" x="1082" y="368" width="56" height="56" stroke="#FFFFFF"/>
# MAGIC   <text x="1110" y="474" text-anchor="middle" font-size="22" font-weight="800" fill="#2272B4"><tspan x="1110" dy="0">Champion</tspan><tspan x="1110" dy="26">Model</tspan></text>
# MAGIC   <text x="1110" y="524" text-anchor="middle" font-size="19" font-style="italic" fill="#2272B4">(in production)</text>
# MAGIC   <rect x="1064" y="580" width="92" height="92" rx="16" fill="#FF5F46"/>
# MAGIC   <use href="#ic-brain" x="1082" y="598" width="56" height="56" stroke="#FFFFFF"/>
# MAGIC   <text x="1110" y="704" text-anchor="middle" font-size="22" font-weight="800" fill="#FF5F46"><tspan x="1110" dy="0">Challenger</tspan><tspan x="1110" dy="26">Model</tspan></text>
# MAGIC   <text x="1110" y="754" text-anchor="middle" font-size="19" font-style="italic" fill="#FF5F46">(new)</text>
# MAGIC </svg>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## B. Model Rollout Strategies

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### B1. Shadow and Rolling
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC        <h3 style="text-align:center;">Shadow</h3>
# MAGIC         New deployment shows existing system but not used for decision making.
# MAGIC                  <div style="height:16px;"></div>
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M3L2_68_1.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC    </td>
# MAGIC     <td style="padding-left: 10px; font-size: 1em;">  
# MAGIC         <h3 style="text-align:center;">Rolling</h3>
# MAGIC         Updates nodes in a target environment incrementally in batches with the new version.
# MAGIC         <div style="height:10px;"></div>
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M3L2_68_2.png" alt="Image description" style="max-width: 90%; height: auto;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### B2. Blue-Green and Canary
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC        <h3 style="text-align:center;">Blue-Green</h3>
# MAGIC         Utilizes two identical environments, a “blue” and a “green” environment with different versions of an application or service.
# MAGIC         <div style="height:16px;"></div>
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M3L2_69_1.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC    </td>
# MAGIC     <td style="padding-left: 10px; font-size: 1em;">  
# MAGIC         <h3 style="text-align:center;">Canary</h3>
# MAGIC         Releases an application or service incrementally to a subset of users.
# MAGIC         <div style="height:40px;"></div>
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M3L2_69_2.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### B3. A/B Testing
# MAGIC
# MAGIC #### Different versions of the same service run simultaneously in the same environment for a period of time.
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC        <h4 style="text-align:left;">Framework:</h4>
# MAGIC        <div style="height:8px;"></div>
# MAGIC         A/B testing is essentially an experiment where two or more variants of a deployment are shown to users at random, and statistical analysis is used to determine which variation performs better for a given conversion goal.
# MAGIC         <ul><li>Collect data</li>
# MAGIC         <li>Identify goals</li>
# MAGIC         <li>Generate hypothesis</li>
# MAGIC         <li>Create variations</li>
# MAGIC         <li>Run experiment</li>
# MAGIC         <li>Analyze results</li></ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 10px; font-size: 1em;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M3L2_70.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## C. Databricks Model Serving
# MAGIC
# MAGIC ### Online evaluation
# MAGIC
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1.15em;">
# MAGIC         <p>Supports online evaluation strategies such as A/B testing or canary deployments through the ability to <a href="https://docs.databricks.com/aws/en/machine-learning/model-serving/serve-multiple-models-to-serving-endpoint" target="_blank" style="color: red;">serve multiple models to a serving endpoint</a>.</p>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 10px; font-size: 1em;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M3L2_71.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>