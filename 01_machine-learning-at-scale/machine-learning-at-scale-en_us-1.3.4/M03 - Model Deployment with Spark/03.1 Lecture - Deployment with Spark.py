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
# MAGIC # Lecture: Deployment with Spark
# MAGIC #### Deploying Machine Learning Models with Spark
# MAGIC
# MAGIC What model deployment means and the trade-offs between batch, streaming, real-time, and edge deployment modes — why Spark excels at batch/streaming but is slow for online scoring, and how to deploy both single-node and Spark ML models over the Spark DataFrame API.
# MAGIC
# MAGIC <div style="max-width:1180px;margin:20px 0 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><div style="background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-left:4px solid #FF5F46;border-radius:10px;padding:18px 22px;"><div style="display:flex;align-items:center;gap:12px;"><svg viewBox="0 0 32 32" width="30" height="30" aria-hidden="true" style="flex:0 0 30px;"><g fill="none" stroke="#FF5F46" stroke-width="1.7"><circle cx="16" cy="16" r="12"/><circle cx="16" cy="16" r="7"/><circle cx="16" cy="16" r="2.4" fill="#FF5F46"/></g></svg><div style="font-size:12px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#FF5F46;">Learning Objectives</div></div><p style="margin:12px 0 8px;font-size:15px;color:#33414B;">By the end of this lesson, you will be able to:</p><ul style="line-height:1.7;font-size:14.5px;color:#33414B;margin:0;padding-left:22px;"><li style="margin:4px 0;">Define model deployment and compare the batch, streaming, real-time, and edge deployment modes.</li><li style="margin:4px 0;">Explain why Spark excels at batch and streaming but is slow for online, single-request scoring.</li><li style="margin:4px 0;">Deploy both single-node and Spark ML models over the Spark DataFrame API.</li></ul></div></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Model Deployment
# MAGIC ### Your model is ready — then what?
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> deployment puts a trained model to work &mdash; integrating it into a <b>production environment</b> so end-users or systems can generate predictions. Which mode you pick depends on the latency and throughput the use case demands.</p>
# MAGIC <div style="overflow-x:auto;max-width:1320px;margin:0 auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1320 230" role="img" style="min-width:980px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC <title>Machine learning lifecycle</title><desc>The machine learning lifecycle with the current stage highlighted.</desc>
# MAGIC <defs><marker id="lc-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-linecap="round"/></marker><marker id="lc-fb" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B5162" stroke-width="1.6" stroke-linecap="round"/></marker></defs>
# MAGIC <rect x="8" y="30" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="79" y="80" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Business</text>
# MAGIC <text x="79" y="100" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Problem</text>
# MAGIC <path d="M153 90 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="174" y="30" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="245" y="70" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Define</text>
# MAGIC <text x="245" y="90" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Success</text>
# MAGIC <text x="245" y="110" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Criteria</text>
# MAGIC <path d="M319 90 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="340" y="30" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="411" y="80" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Data</text>
# MAGIC <text x="411" y="100" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Collection</text>
# MAGIC <path d="M485 90 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="506" y="30" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="577" y="60" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Data</text>
# MAGIC <text x="577" y="80" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Preprocessing/</text>
# MAGIC <text x="577" y="100" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Feature</text>
# MAGIC <text x="577" y="120" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Engineering</text>
# MAGIC <path d="M651 90 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="672" y="30" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="743" y="80" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model Training</text>
# MAGIC <text x="743" y="100" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">& Tuning</text>
# MAGIC <path d="M817 90 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="838" y="30" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="909" y="80" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="909" y="100" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Evaluation</text>
# MAGIC <path d="M983 90 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="1004" y="30" width="142" height="120" rx="10" fill="#1B3139" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="1075" y="80" text-anchor="middle" font-size="14.5" font-weight="700" fill="#fff">Model</text>
# MAGIC <text x="1075" y="100" text-anchor="middle" font-size="14.5" font-weight="700" fill="#fff">Deployment</text>
# MAGIC <path d="M1149 90 h16" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#lc-ar)"/>
# MAGIC <rect x="1170" y="30" width="142" height="120" rx="10" fill="#EEEDE9" stroke="#C7CDD2" stroke-width="1.5"/>
# MAGIC <text x="1241" y="80" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Model</text>
# MAGIC <text x="1241" y="100" text-anchor="middle" font-size="14.5" font-weight="700" fill="#1B3139">Monitoring</text>
# MAGIC <path d="M909 150 C 869 195, 617 195, 577 150" stroke="#1B5162" stroke-width="1.8" fill="none" stroke-dasharray="7 5" marker-end="url(#lc-fb)"/>
# MAGIC <path d="M909 150 C 829 222, 451 222, 411 150" stroke="#1B5162" stroke-width="1.8" fill="none" stroke-dasharray="7 5" marker-end="url(#lc-fb)"/>
# MAGIC </svg></div>
# MAGIC </div><p style="font-size:1.0em;color:#33414B;margin:14px 0 6px;">The process of integrating a model into a <b>production environment</b> to <b>generate predictions</b> or insights. Common deployment strategies:</p><div><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">Batch</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">Streaming</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">Real-time</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">Embedded / Edge</span></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Model Deployment Modes
# MAGIC ### Comparing the four deployment methods
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> the right mode is a trade-off between <b>throughput</b> and <b>latency</b>. Batch dominates in practice; real-time and edge give up throughput for responsiveness close to the user or device.</p>
# MAGIC <table style="border-collapse:collapse;width:100%;margin:6px 0;"><thead><tr><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">Method</th><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">Throughput</th><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">Latency</th><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">Best for</th><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">Example</th></tr></thead><tbody><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><b>Batch</b> <span style='color:#618794'>(80–90%)</span></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">High</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">High <span style='color:#618794'>(hours–days)</span></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Large-scale processing when immediate results aren't critical; predictions stored.</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Periodic churn prediction</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><b>Streaming</b> <span style='color:#618794'>(10–15%)</span></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Moderate</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Moderate <span style='color:#618794'>(sec–min)</span></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Continuous data needing timely updates; processed in micro-batches.</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Dynamic pricing</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><b>Real-time</b> <span style='color:#618794'>(5–10%)</span></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Low</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Low <span style='color:#618794'>(ms)</span></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Quick decisions &amp; user responsiveness; usually via REST.</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Recommendations; chatbot</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><b>Edge / Embedded</b></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Low</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Low <span style='color:#618794'>(device)</span></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Processing near the data source; varied hardware.</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">IoT; farm humidity sensor</td></tr></tbody></table><div style="border-left:4px solid #FF5F46;background:#FFF6F4;padding:12px 16px;border-radius:0 6px 6px 0;margin-top:16px;font-size:14.5px;line-height:1.55;color:#33414B;"><b>Note:</b> the percentages are estimates, in flux with the growing focus on Generative AI and direct user interactions.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Why is Spark Slow in Online Scoring?
# MAGIC ### Spark is built for batch and streaming, not single-request latency
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> Spark optimizes for <b>throughput, not latency</b>. The Catalyst compilation and cluster coordination that make large batch jobs fast add per-request overhead that hurts low-latency online scoring.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:0;"><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="6" y="4" width="20" height="24" rx="2"/><path d="M10 11 h12 M10 16 h12 M10 21 h8"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Compilation</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Complex execution plans</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Every query is compiled through the Catalyst pipeline &mdash; optimized for throughput rather than per-call latency.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="6"/><circle cx="22" cy="21" r="5"/><path d="M12 5 v-2 M12 21 v-2 M5 12 h-2 M21 12 h-1"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Coordination</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Per-job overhead</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Each job carries task scheduling, serialization/deserialization and cross-node coordination costs.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="7" y="10" width="18" height="12" rx="2"/><rect x="12" y="14" width="8" height="4" rx="1"/><path d="M11 10 v-3 M16 10 v-3 M21 10 v-3 M11 22 v3 M16 22 v3 M21 22 v3"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Runtime</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">In-memory GC</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">In-memory computation can trigger frequent garbage collection and disk spillage, delaying scoring.</p></div></div><div style="overflow-x:auto;max-width:1300px;margin:0 auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1300 250" role="img" style="min-width:1000px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC   <title>Spark Catalyst pipeline and cluster execution</title>
# MAGIC   <desc>A query is compiled through a multi-stage Catalyst pipeline into a selected physical plan, then executed across a driver, cluster manager and worker executors.</desc>
# MAGIC   <defs><marker id="s4-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#5A6B73" stroke-width="1.8" stroke-linecap="round"/></marker></defs>
# MAGIC   <g font-size="13" font-weight="700" fill="#fff" text-anchor="middle">
# MAGIC     <rect x="20" y="70" width="120" height="90" rx="8" fill="#FF5F46"/><text x="80" y="110">Unresolved</text><text x="80" y="128">Logical Plan</text>
# MAGIC     <rect x="160" y="70" width="120" height="90" rx="8" fill="#FF5F46"/><text x="220" y="119">Logical Plan</text>
# MAGIC     <rect x="300" y="70" width="120" height="90" rx="8" fill="#FF5F46"/><text x="360" y="110">Optimized</text><text x="360" y="128">Logical Plan</text>
# MAGIC     <rect x="447" y="62" width="120" height="90" rx="8" fill="#5A8194"/><rect x="440" y="70" width="120" height="90" rx="8" fill="#FF5F46"/><text x="500" y="119">Physical Plans</text>
# MAGIC     <rect x="580" y="62" width="52" height="98" rx="8" fill="#9AA5AC"/><text x="606" y="115" transform="rotate(-90 606 115)">Cost Model</text>
# MAGIC     <rect x="652" y="70" width="120" height="90" rx="8" fill="#FF5F46"/><text x="712" y="110">Selected</text><text x="712" y="128">Physical Plan</text>
# MAGIC   </g>
# MAGIC   <g stroke="#5A6B73" stroke-width="2" fill="none">
# MAGIC     <path d="M140 115 h16" marker-end="url(#s4-ar)"/><path d="M280 115 h16" marker-end="url(#s4-ar)"/><path d="M420 115 h16" marker-end="url(#s4-ar)"/><path d="M567 115 h9" marker-end="url(#s4-ar)"/><path d="M632 115 h16" marker-end="url(#s4-ar)"/><path d="M772 115 h20" marker-end="url(#s4-ar)"/>
# MAGIC   </g>
# MAGIC   <!-- mini cluster -->
# MAGIC   <rect x="800" y="30" width="200" height="76" rx="8" fill="#fff" stroke="#1B3139"/><text x="900" y="56" text-anchor="middle" font-size="13" font-weight="700" fill="#1B3139">Driver Program</text><rect x="820" y="66" width="160" height="28" rx="4" fill="#5A8194"/><text x="900" y="85" text-anchor="middle" font-size="12" fill="#fff">SparkContext</text>
# MAGIC   <rect x="800" y="140" width="200" height="76" rx="8" fill="#fff" stroke="#1B3139"/><text x="900" y="166" text-anchor="middle" font-size="13" font-weight="700" fill="#1B3139">Cluster Manager</text><rect x="820" y="176" width="160" height="28" rx="4" fill="#5A8194"/><text x="900" y="195" text-anchor="middle" font-size="12" fill="#fff">Scheduler</text>
# MAGIC   <g><rect x="1050" y="30" width="230" height="86" rx="8" fill="#0B2027"/><text x="1165" y="54" text-anchor="middle" font-size="13" font-weight="700" fill="#fff">Worker Node — Executor</text><rect x="1070" y="64" width="190" height="20" rx="4" fill="#1CA971"/><text x="1165" y="79" text-anchor="middle" font-size="11" fill="#fff">Cache</text><rect x="1070" y="90" width="90" height="18" rx="3" fill="#F2A93B"/><rect x="1170" y="90" width="90" height="18" rx="3" fill="#F2A93B"/></g>
# MAGIC   <g><rect x="1050" y="130" width="230" height="86" rx="8" fill="#0B2027"/><text x="1165" y="154" text-anchor="middle" font-size="13" font-weight="700" fill="#fff">Worker Node — Executor</text><rect x="1070" y="164" width="190" height="20" rx="4" fill="#1CA971"/><text x="1165" y="179" text-anchor="middle" font-size="11" fill="#fff">Cache</text><rect x="1070" y="190" width="90" height="18" rx="3" fill="#F2A93B"/><rect x="1170" y="190" width="90" height="18" rx="3" fill="#F2A93B"/></g>
# MAGIC   <g stroke="#1B3139" stroke-width="2" fill="none"><path d="M1000 66 L1046 66" marker-end="url(#s4-ar)"/></g>
# MAGIC   <g stroke="#5A8194" stroke-width="2" fill="none" stroke-dasharray="6 4"><path d="M1000 178 L1046 150" marker-end="url(#s4-ar)"/></g>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Deploying a Single-Node Model on a Spark DataFrame
# MAGIC ### Why run a single-node model over the DataFrame API?
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> you can wrap a single-machine model (e.g. scikit-learn) and apply it across a Spark DataFrame &mdash; getting Spark's distributed <b>data processing</b> while keeping a model designed to run on one machine.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:0;"><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M10 16 v-6 a2 2 0 0 1 4 0 v5 M14 12 a2 2 0 0 1 4 0 v3 M18 13 a2 2 0 0 1 4 0 v3 c0 6 -3 10 -8 10 c-3 0 -5 -1 -7 -4 l-3 -5 a2 2 0 0 1 3 -2 l1 1"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Ease of use</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Simple, expressive API</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">A domain-specific language simplifies distributed data manipulation.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="16" cy="16" r="3.5"/><circle cx="5" cy="6" r="2.4"/><circle cx="27" cy="6" r="2.4"/><circle cx="5" cy="26" r="2.4"/><circle cx="27" cy="26" r="2.4"/><path d="M13.3 14 L7 8 M18.7 14 L25 8 M13.3 18 L7 24 M18.7 18 L25 24"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Scalability</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Bigger datasets</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Distributes the data across nodes so you can process more than one machine holds.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M12 6 C8 6 8 12 5 12 C8 12 8 26 12 26"/><path d="M20 6 C24 6 24 12 27 12 C24 12 24 26 20 26"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">ML integration</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Works with MLlib</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Integrates directly with MLlib for advanced models and pipelines.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="16" cy="16" r="12"/><path d="M16 9 v14 M13 12 h5 a2.5 2.5 0 0 1 0 5 h-4 a2.5 2.5 0 0 0 0 5 h5"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Cost efficiency</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Flexible resources</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">More cost-effective than a full distributed system, with flexible resource management.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M6 11 h16 l-4 -4 M22 11 l0 0 M26 21 h-16 l4 4 M10 21 l0 0"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Flexibility</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Pandas ↔ Spark</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Seamlessly switch between pandas and Spark DataFrames.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M5 26 V5 M5 26 H27"/><path d="M9 22 L15 14 L19 18 L26 8"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Advanced analytics</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Custom functions</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Enables complex user functions for sophisticated analysis.</p></div></div><div style="background:#1B5162;color:#fff;border-radius:6px;padding:14px 18px;margin-top:16px;text-align:center;font-size:1.05em;"><b>Answer:</b> leverage Spark's distributed data processing while still using a model built to run on a single machine.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Why Use a Spark ML Model with the Spark DataFrame API?
# MAGIC ### Native distributed training and inference
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> a native Spark ML model trains and infers in a <b>distributed</b> way across the cluster &mdash; no wrapping needed. It scales to very large data and drops straight into end-to-end Spark ML pipelines.</p>
# MAGIC <table><colgroup><col style="width:50%"><col style="width:50%"></colgroup><tr><td style="font-size:1.02em;vertical-align:middle;"><ul style="line-height:1.9;"><li>Distributed training and <b>native distributed inference</b>.</li><li>Native integration with Spark's distributed environment and the DataFrame API.</li><li>Built to handle <b>very large datasets</b>.</li><li>Facilitates end-to-end ML pipelines within Spark's libraries.</li></ul></td><td style="vertical-align:middle;"><div style="overflow-x:auto;max-width:640px;margin:0 auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 640 320" role="img" style="min-width:520px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC   <title>Distributed inference on Spark</title>
# MAGIC   <desc>New data is split into subsets across nodes where a replicated model produces predictions in parallel.</desc>
# MAGIC   <defs><marker id="di-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#FF5F46" stroke-width="1.8" stroke-linecap="round"/></marker></defs>
# MAGIC   <text x="110" y="40" text-anchor="middle" font-size="15" fill="#1c2733">New Data</text>
# MAGIC   <ellipse cx="110" cy="60" rx="60" ry="16" fill="#5A7A88"/><rect x="50" y="60" width="120" height="90" fill="#5A7A88"/><ellipse cx="110" cy="150" rx="60" ry="16" fill="#4a6673"/>
# MAGIC   <text x="360" y="70" text-anchor="middle" font-size="15" fill="#1c2733">Nodes</text>
# MAGIC   <text x="540" y="70" text-anchor="middle" font-size="15" fill="#1c2733">Predictions</text>
# MAGIC   <text x="110" y="215" text-anchor="middle" font-size="15" fill="#1c2733">Model</text>
# MAGIC   <circle cx="110" cy="245" r="18" fill="#F2A93B"/>
# MAGIC   <g><rect x="330" y="95" width="60" height="44" rx="6" fill="#fff" stroke="#F2A93B"/><rect x="330" y="150" width="60" height="44" rx="6" fill="#fff" stroke="#F2A93B"/><rect x="330" y="205" width="60" height="44" rx="6" fill="#fff" stroke="#F2A93B"/></g>
# MAGIC   <circle cx="540" cy="117" r="18" fill="#1CA971"/><circle cx="540" cy="172" r="18" fill="#1CA971"/><circle cx="540" cy="227" r="18" fill="#1CA971"/>
# MAGIC   <g stroke="#FF5F46" stroke-width="1.8" fill="none">
# MAGIC     <path d="M170 110 L328 117" marker-end="url(#di-ar)"/><path d="M170 120 L328 172" marker-end="url(#di-ar)"/><path d="M170 130 L328 227" marker-end="url(#di-ar)"/>
# MAGIC     <path d="M128 240 L328 130" marker-end="url(#di-ar)"/><path d="M128 245 L328 180" marker-end="url(#di-ar)"/><path d="M128 250 L328 230" marker-end="url(#di-ar)"/>
# MAGIC     <path d="M390 117 L518 117" marker-end="url(#di-ar)"/><path d="M390 172 L518 172" marker-end="url(#di-ar)"/><path d="M390 227 L518 227" marker-end="url(#di-ar)"/>
# MAGIC   </g>
# MAGIC   <text x="250" y="100" font-size="11" font-style="italic" fill="#4a5a62">Data Subset</text>
# MAGIC   <text x="250" y="270" font-size="11" font-style="italic" fill="#4a5a62">Replicated</text>
# MAGIC </svg></div>
# MAGIC </div></td></tr></table><div style="background:#8B1A2E;color:#fff;border-radius:6px;padding:14px 18px;margin-top:16px;text-align:center;font-size:1.05em;"><b>Answer:</b> harness the full power of distributed computing for data processing, model training and inferencing.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Deploying a Spark ML Model on a Spark DataFrame
# MAGIC ### Load, infer, and write back — all in Unity Catalog
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> the end-to-end pattern &mdash; read a governed table from Unity Catalog, load a registered Spark ML model, run distributed inference with <code>transform()</code>, and write the predictions back to a Delta table.</p>
# MAGIC <table><colgroup><col style="width:58%"><col style="width:42%"></colgroup><tr><td style="vertical-align:top;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">import</span> mlflow
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># 1 · Load the DataFrame from Unity Catalog</span>
# MAGIC df = spark.table(<span style="color:#2E8B57;">&quot;catalog_name.schema_name.table_name&quot;</span>)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># 2 · Load a registered Spark ML model from Unity Catalog</span>
# MAGIC mlflow.set_registry_uri(<span style="color:#2E8B57;">&quot;databricks-uc&quot;</span>)
# MAGIC model = mlflow.spark.load_model(
# MAGIC     <span style="color:#2E8B57;">&quot;models:/catalog_name.schema_name.model_name@champion&quot;</span>)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># 3 · Run distributed inference</span>
# MAGIC predictions = model.transform(df)
# MAGIC predictions.select(<span style="color:#2E8B57;">&quot;id&quot;</span>, <span style="color:#2E8B57;">&quot;prediction&quot;</span>).show()
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># 4 · Write predictions back to a Delta table</span>
# MAGIC (predictions.write
# MAGIC     .format(<span style="color:#2E8B57;">&quot;delta&quot;</span>).mode(<span style="color:#2E8B57;">&quot;overwrite&quot;</span>)
# MAGIC     .saveAsTable(<span style="color:#2E8B57;">&quot;catalog_name.schema_name.predictions_table&quot;</span>))</pre></div></td><td style="vertical-align:middle;"><div style="overflow-x:auto;max-width:640px;margin:0 auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 640 320" role="img" style="min-width:520px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC   <title>Distributed inference on Spark</title>
# MAGIC   <desc>New data is split into subsets across nodes where a replicated model produces predictions in parallel.</desc>
# MAGIC   <defs><marker id="di-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#FF5F46" stroke-width="1.8" stroke-linecap="round"/></marker></defs>
# MAGIC   <text x="110" y="40" text-anchor="middle" font-size="15" fill="#1c2733">New Data</text>
# MAGIC   <ellipse cx="110" cy="60" rx="60" ry="16" fill="#5A7A88"/><rect x="50" y="60" width="120" height="90" fill="#5A7A88"/><ellipse cx="110" cy="150" rx="60" ry="16" fill="#4a6673"/>
# MAGIC   <text x="360" y="70" text-anchor="middle" font-size="15" fill="#1c2733">Nodes</text>
# MAGIC   <text x="540" y="70" text-anchor="middle" font-size="15" fill="#1c2733">Predictions</text>
# MAGIC   <text x="110" y="215" text-anchor="middle" font-size="15" fill="#1c2733">Model</text>
# MAGIC   <circle cx="110" cy="245" r="18" fill="#F2A93B"/>
# MAGIC   <g><rect x="330" y="95" width="60" height="44" rx="6" fill="#fff" stroke="#F2A93B"/><rect x="330" y="150" width="60" height="44" rx="6" fill="#fff" stroke="#F2A93B"/><rect x="330" y="205" width="60" height="44" rx="6" fill="#fff" stroke="#F2A93B"/></g>
# MAGIC   <circle cx="540" cy="117" r="18" fill="#1CA971"/><circle cx="540" cy="172" r="18" fill="#1CA971"/><circle cx="540" cy="227" r="18" fill="#1CA971"/>
# MAGIC   <g stroke="#FF5F46" stroke-width="1.8" fill="none">
# MAGIC     <path d="M170 110 L328 117" marker-end="url(#di-ar)"/><path d="M170 120 L328 172" marker-end="url(#di-ar)"/><path d="M170 130 L328 227" marker-end="url(#di-ar)"/>
# MAGIC     <path d="M128 240 L328 130" marker-end="url(#di-ar)"/><path d="M128 245 L328 180" marker-end="url(#di-ar)"/><path d="M128 250 L328 230" marker-end="url(#di-ar)"/>
# MAGIC     <path d="M390 117 L518 117" marker-end="url(#di-ar)"/><path d="M390 172 L518 172" marker-end="url(#di-ar)"/><path d="M390 227 L518 227" marker-end="url(#di-ar)"/>
# MAGIC   </g>
# MAGIC   <text x="250" y="100" font-size="11" font-style="italic" fill="#4a5a62">Data Subset</text>
# MAGIC   <text x="250" y="270" font-size="11" font-style="italic" fill="#4a5a62">Replicated</text>
# MAGIC </svg></div>
# MAGIC </div></td></tr></table><div style="background:#8B1A2E;color:#fff;border-radius:6px;padding:14px 18px;margin-top:16px;text-align:center;font-size:1.05em;"><b>Answer:</b> harness the full power of distributed computing for data processing, model training and inferencing.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>