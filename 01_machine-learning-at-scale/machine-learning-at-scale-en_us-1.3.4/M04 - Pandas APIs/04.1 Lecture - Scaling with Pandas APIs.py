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
# MAGIC # Lecture: Scaling with Pandas APIs
# MAGIC #### Pandas on Spark
# MAGIC
# MAGIC How the Pandas API on Spark (formerly the Koalas project) lets you run familiar pandas code at Spark scale — how it differs from pandas and PySpark DataFrames, the role of the InternalFrame that bridges them, and the scalability, conversion, SQL-integration, and performance benefits it unlocks.
# MAGIC
# MAGIC <div style="max-width:1180px;margin:20px 0 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><div style="background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-left:4px solid #FF5F46;border-radius:10px;padding:18px 22px;"><div style="display:flex;align-items:center;gap:12px;"><svg viewBox="0 0 32 32" width="30" height="30" aria-hidden="true" style="flex:0 0 30px;"><g fill="none" stroke="#FF5F46" stroke-width="1.7"><circle cx="16" cy="16" r="12"/><circle cx="16" cy="16" r="7"/><circle cx="16" cy="16" r="2.4" fill="#FF5F46"/></g></svg><div style="font-size:12px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#FF5F46;">Learning Objectives</div></div><p style="margin:12px 0 8px;font-size:15px;color:#33414B;">By the end of this lesson, you will be able to:</p><ul style="line-height:1.7;font-size:14.5px;color:#33414B;margin:0;padding-left:22px;"><li style="margin:4px 0;">Explain how the Pandas API on Spark runs familiar pandas code at Spark scale.</li><li style="margin:4px 0;">Contrast pandas, PySpark, and Pandas-API-on-Spark DataFrames, and describe the role of the InternalFrame.</li><li style="margin:4px 0;">Apply the Pandas API on Spark for scalability, DataFrame conversion, and SQL integration, and describe its performance benefits.</li></ul></div></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Scaling Data with the Pandas API on Spark
# MAGIC ### Formerly Koalas — the familiar pandas API, at Spark scale
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> pandas is beloved but single-machine; Spark is distributed but a different API. The Pandas API on Spark bridges them &mdash; run your existing pandas code across a cluster by changing a single import.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:0;"><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="7" width="24" height="18" rx="2"/><path d="M4 13 h24 M12 7 v18 M20 7 v18"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Single machine</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Pandas</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Popular, familiar and very performant on small data, with deep NumPy / Matplotlib / scikit-learn integration.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M17 3 l2.4 8.4 L27.5 12 l-6.8 4.8 l2.4 8.4 L16 19.6 l-7.5 5.4 L10.9 16.6 L4.1 12 l8.4 -0.4 Z"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Distributed</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Apache Spark</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">A scalable, fast distributed engine supporting many data sources, workloads and languages.</p></div></div><div style="margin-top:16px;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px;"><b style="font-size:1.05em;color:#0b2026;">Pandas API on Spark fills the gaps</b> &mdash; when your data gets big, run existing pandas code on Spark:<ul style="line-height:1.7;margin:8px 0 0;color:#33414B;"><li>Avoid learning a new framework and confusing pandas vs. Spark APIs</li><li>Be more productive; maintain a single codebase</li><li>Avoid time-consuming rewrites &amp; testing</li></ul></div><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">One-line change</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#6E8B6E;font-style:italic;"># Change one import — the rest of your pandas code stays the same</span>
# MAGIC <span style="color:#0B5FA5;">from</span> pyspark.pandas <span style="color:#0B5FA5;">import</span> read_csv     <span style="color:#6E8B6E;font-style:italic;"># was: from pandas import read_csv</span>
# MAGIC pdf = read_csv(<span style="color:#2E8B57;">&quot;data.csv&quot;</span>)</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Pandas DataFrame vs. PySpark DataFrame
# MAGIC ### Same idea, different semantics
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> the two DataFrames behave differently &mdash; pandas is mutable and eager, PySpark is immutable and lazy. The Pandas API on Spark gives pandas syntax with Spark's execution model underneath.</p>
# MAGIC <table style="border-collapse:collapse;width:100%;margin:6px 0;"><thead><tr><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;"></th><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">pandas DataFrame</th><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">PySpark DataFrame</th></tr></thead><tbody><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><b>Column</b></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>df['col']</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>df['col']</code></td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><b>Mutability</b></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Mutable</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Immutable</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><b>Execution</b></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Eager</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Lazy</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><b>Add a column</b></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>df['c'] = df['a'] + df['b']</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>df = df.withColumn('c', df['a'] + df['b'])</code></td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><b>Rename columns</b></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>df.columns = ['a','b']</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>df.select(df['c1'].alias('a'), …)</code> or <code>df.toDF('a','b')</code></td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><b>Value count</b></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>df['col'].value_counts()</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>df.groupBy('col').count().orderBy('count', ascending=False)</code></td></tr></tbody></table><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">The same operation, three ways</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#6E8B6E;font-style:italic;"># Pandas</span>
# MAGIC <span style="color:#0B5FA5;">import</span> pandas <span style="color:#0B5FA5;">as</span> pd
# MAGIC pandas_df[<span style="color:#2E8B57;">&#39;C&#39;</span>] = pandas_df[<span style="color:#2E8B57;">&#39;A&#39;</span>] + pandas_df[<span style="color:#2E8B57;">&#39;B&#39;</span>]
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Apache Spark</span>
# MAGIC <span style="color:#0B5FA5;">from</span> pyspark.sql.functions <span style="color:#0B5FA5;">import</span> col
# MAGIC spark_df = spark_df.withColumn(<span style="color:#2E8B57;">&#39;C&#39;</span>, spark_df[<span style="color:#2E8B57;">&#39;A&#39;</span>] + spark_df[<span style="color:#2E8B57;">&#39;B&#39;</span>])
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Pandas API on Spark</span>
# MAGIC <span style="color:#0B5FA5;">import</span> pyspark.pandas <span style="color:#0B5FA5;">as</span> ps
# MAGIC ps_df[<span style="color:#2E8B57;">&#39;C&#39;</span>] = ps_df[<span style="color:#2E8B57;">&#39;A&#39;</span>] + ps_df[<span style="color:#2E8B57;">&#39;B&#39;</span>]</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Role of the InternalFrame in PySpark Pandas
# MAGIC ### Connecting the pandas API with Spark DataFrames
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> the <b>InternalFrame</b> is the bridge. It holds a Spark DataFrame plus immutable metadata (columns, index, types) and maps pandas operations onto Spark &mdash; so pandas-style code executes as Spark jobs.</p>
# MAGIC <div style="background:#EDEDED;border-radius:6px;padding:12px 16px;text-align:center;font-size:1.02em;font-weight:700;color:#1c2733;margin-bottom:12px;">The InternalFrame converts between the Pandas API on Spark and the PySpark DataFrame.</div><div style="overflow-x:auto;max-width:1300px;margin:0 auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1300 300" role="img" style="min-width:1000px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC   <title>InternalFrame bridges the pandas API on Spark and PySpark DataFrames</title>
# MAGIC   <desc>A Pandas-on-Spark DataFrame is backed by an InternalFrame that holds a Spark DataFrame and immutable metadata; together they form the pandas API on Spark, which sits over the PySpark DataFrame and the Spark execution engine.</desc>
# MAGIC   <defs><marker id="if-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="9" markerHeight="9" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="#1B3139"/></marker></defs>
# MAGIC   <rect x="20" y="30" width="260" height="150" rx="8" fill="#98102a"/>
# MAGIC   <text x="150" y="66" text-anchor="middle" font-size="17" font-weight="700" fill="#fff">Pandas on Spark</text><text x="150" y="88" text-anchor="middle" font-size="17" font-weight="700" fill="#fff">DataFrame</text>
# MAGIC   <text x="40" y="128" font-size="13" fill="#fff">– User operations &amp;</text><text x="40" y="146" font-size="13" fill="#fff">&nbsp;&nbsp;transformation</text>
# MAGIC   <rect x="330" y="30" width="300" height="150" rx="8" fill="#98102a"/>
# MAGIC   <text x="480" y="62" text-anchor="middle" font-size="17" font-weight="700" fill="#fff">InternalFrame</text>
# MAGIC   <text x="350" y="92" font-size="13" fill="#fff">– Holds Spark DataFrame + metadata</text>
# MAGIC   <text x="350" y="116" font-size="13" fill="#fff">– Manages metadata (columns, index, types)</text>
# MAGIC   <text x="350" y="140" font-size="13" fill="#fff">– Manages mappings</text>
# MAGIC   <rect x="740" y="30" width="260" height="150" rx="8" fill="#5A7A88"/>
# MAGIC   <text x="870" y="90" text-anchor="middle" font-size="17" font-weight="700" fill="#fff">PySpark</text><text x="870" y="112" text-anchor="middle" font-size="17" font-weight="700" fill="#fff">DataFrame</text>
# MAGIC   <text x="870" y="140" text-anchor="middle" font-size="13" fill="#fff">– Distributed data</text>
# MAGIC   <rect x="1050" y="30" width="230" height="150" rx="8" fill="#5A7A88"/>
# MAGIC   <text x="1165" y="90" text-anchor="middle" font-size="17" font-weight="700" fill="#fff">Spark</text><text x="1165" y="112" text-anchor="middle" font-size="17" font-weight="700" fill="#fff">Execution</text><text x="1165" y="134" text-anchor="middle" font-size="17" font-weight="700" fill="#fff">Engine</text>
# MAGIC   <g stroke="#1B3139" stroke-width="3" fill="none">
# MAGIC     <path d="M280 105 h44" marker-end="url(#if-ar)"/><path d="M630 105 h104" marker-end="url(#if-ar)"/><path d="M1000 105 h44" marker-end="url(#if-ar)"/>
# MAGIC   </g>
# MAGIC   <path d="M20 200 q0 16 16 16 h278 q16 0 16 16 q0 -16 16 -16 h278 q16 0 16 -16" fill="none" stroke="#98102a" stroke-width="3"/>
# MAGIC   <text x="330" y="270" text-anchor="middle" font-size="18" font-weight="800" fill="#98102a">Pandas API on Spark</text>
# MAGIC   <path d="M740 200 q0 16 16 16 h242 q16 0 16 16 q0 -16 16 -16 h242 q16 0 16 -16" fill="none" stroke="#5A7A88" stroke-width="3"/>
# MAGIC   <text x="1020" y="270" text-anchor="middle" font-size="18" font-weight="800" fill="#5A7A88">PySpark DataFrame</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## InternalFrame in the Pandas API on Spark
# MAGIC ### What happens when a user operates on a DataFrame
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> every operation either produces a new InternalFrame or just updates metadata &mdash; understanding which keeps you aware of when a new Spark DataFrame is created versus reused.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:0;"><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="7" width="22" height="18" rx="2"/><path d="M10 16 l3 3 l-3 3 M16 22 h6" transform="translate(0 -6)"/><path d="M9 13 h14"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">New state</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Calls an API</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">The Spark DataFrame is updated and the InternalFrame is created or copied with the new state (e.g. new columns/indices) &mdash; a new DataFrame and InternalFrame result.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M8 6 a11 11 0 1 0 6 -2"/><path d="M14 2 l0 8 l-8 0" transform="translate(0 0)"/><path d="M22 16 l-6 6 l-4 -4"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">In-place</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Updates internal state</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">e.g. <code>psdf.dropna(..., inplace=True)</code> &mdash; copies the InternalFrame with new state pointing at a new Spark DataFrame.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="6" y="5" width="20" height="22" rx="2"/><path d="M10 11 h12 M10 16 h12 M10 21 h8"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Metadata only</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Updates metadata only</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">e.g. <code>psdf.set_index(...)</code> &mdash; doesn't copy the DataFrame; points a new InternalFrame at the old Spark DataFrame.</p></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Unlocking Data Capabilities
# MAGIC ### Scalability, easy conversion, and SQL integration
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> running pandas on Spark unlocks three things at once &mdash; scale beyond one machine, seamless conversion to/from pandas for libraries like Matplotlib, and direct SQL over the same data.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:0;"><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="16" cy="16" r="3.5"/><circle cx="5" cy="6" r="2.4"/><circle cx="27" cy="6" r="2.4"/><circle cx="5" cy="26" r="2.4"/><circle cx="27" cy="26" r="2.4"/><path d="M13.3 14 L7 8 M18.7 14 L25 8 M13.3 18 L7 24 M18.7 18 L25 24"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Scale</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Beyond a single machine</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Run existing pandas code (<code>from pyspark.pandas import read_csv</code>) distributed across the cluster.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M5 11 h18 l-4 -4 M23 11 M27 21 h-18 l4 4"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Convert</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Pandas ↔ Spark</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Move to pandas for libraries like Matplotlib, then back to Spark for distributed work.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="16" cy="8" rx="11" ry="4"/><path d="M5 8 v16 a11 4 0 0 0 22 0 V8 M5 16 a11 4 0 0 0 22 0"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Query</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">SQL integration</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Query the same data with <code>ps.sql()</code> or a registered temp view.</p></div></div><div style="border-left:4px solid #FF5F46;background:#FFF6F4;padding:12px 16px;border-radius:0 6px 6px 0;margin-top:16px;font-size:14.5px;line-height:1.55;color:#33414B;"><b><code>ConnectException</code> error:</b> often caused by an OOM error that closes the connection to the Python REPL &mdash; check your query's memory usage.</div><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 460px;min-width:380px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Convert &amp; plot</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">import</span> pandas <span style="color:#0B5FA5;">as</span> pd, matplotlib.pyplot <span style="color:#0B5FA5;">as</span> plt
# MAGIC <span style="color:#0B5FA5;">import</span> pyspark.sql.functions <span style="color:#0B5FA5;">as</span> F
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Load a Spark DataFrame from Unity Catalog</span>
# MAGIC sdf = spark.table(<span style="color:#2E8B57;">&quot;catalog.schema.table_name&quot;</span>)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Convert to pandas for Matplotlib (Spark can't plot directly)</span>
# MAGIC pdf = sdf.toPandas()
# MAGIC pdf.plot(kind=<span style="color:#2E8B57;">&quot;bar&quot;</span>, x=<span style="color:#2E8B57;">&quot;Name&quot;</span>, y=<span style="color:#2E8B57;">&quot;Salary&quot;</span>); plt.show()
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Distributed group-by stays on Spark</span>
# MAGIC result = sdf.groupBy(<span style="color:#2E8B57;">&quot;Name&quot;</span>).agg(F.avg(<span style="color:#2E8B57;">&quot;Salary&quot;</span>).alias(<span style="color:#2E8B57;">&quot;AvgSal&quot;</span>))
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Convert a pandas DataFrame back to Spark</span>
# MAGIC sdf_converted = spark.createDataFrame(pdf)</pre></div><div style="flex:1 1 400px;min-width:360px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Integrate with SQL</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">import</span> pyspark.pandas <span style="color:#0B5FA5;">as</span> ps
# MAGIC psdf = ps.read_table(<span style="color:#2E8B57;">&quot;catalog.schema.table_name&quot;</span>)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Method 1 — direct SQL with ps.sql()</span>
# MAGIC ps.sql(<span style="color:#2E8B57;">&quot;SELECT Name, Salary FROM {psdf} WHERE Salary &gt; 6000&quot;</span>).show()
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Method 2 — register a temp view, then Spark SQL</span>
# MAGIC psdf.spark.cache().createOrReplaceTempView(<span style="color:#2E8B57;">&quot;employees&quot;</span>)
# MAGIC spark.sql(<span style="color:#2E8B57;">&quot;SELECT Name, Salary FROM employees WHERE Salary &gt; 6000&quot;</span>).show()</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Performance of Pandas, Spark, and PySpark Pandas
# MAGIC ### Often faster than pandas — even on one machine
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> thanks to Spark's multi-threading and the Catalyst optimizer, the Pandas API on Spark frequently <b>beats plain pandas even on a single machine</b> &mdash; and scales far beyond it.</p>
# MAGIC <table><colgroup><col style="width:56%"><col style="width:44%"></colgroup><tr><td style="vertical-align:top;"><div style="overflow-x:auto;max-width:820px;margin:0 auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 820 380" role="img" style="min-width:700px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC <title>Benchmark: pandas vs pandas API on Spark (local)</title><desc>Elapsed time in seconds for common operations; pandas API on Spark is faster on most operations.</desc>
# MAGIC <text x="425" y="18" text-anchor="middle" font-size="14" font-weight="700" fill="#1c2733">Results — Local (elapsed time, seconds; lower is better)</text>
# MAGIC <line x1="50" y1="30" x2="50" y2="290" stroke="#999"/>
# MAGIC <line x1="50" y1="290" x2="800" y2="290" stroke="#999"/>
# MAGIC <line x1="50" y1="290.0" x2="800" y2="290.0" stroke="#eee"/>
# MAGIC <text x="44" y="294.0" text-anchor="end" font-size="11" fill="#777">0</text>
# MAGIC <line x1="50" y1="217.8" x2="800" y2="217.8" stroke="#eee"/>
# MAGIC <text x="44" y="221.8" text-anchor="end" font-size="11" fill="#777">50</text>
# MAGIC <line x1="50" y1="145.6" x2="800" y2="145.6" stroke="#eee"/>
# MAGIC <text x="44" y="149.6" text-anchor="end" font-size="11" fill="#777">100</text>
# MAGIC <line x1="50" y1="73.3" x2="800" y2="73.3" stroke="#eee"/>
# MAGIC <text x="44" y="77.3" text-anchor="end" font-size="11" fill="#777">150</text>
# MAGIC <rect x="66.1" y="266.5" width="36.4" height="23.5" fill="#2272B4"/>
# MAGIC <text x="84.3" y="263.5" text-anchor="middle" font-size="9" fill="#2272B4">16.3</text>
# MAGIC <rect x="105.5" y="288.6" width="36.4" height="1.4" fill="#F57C00"/>
# MAGIC <text x="123.7" y="285.6" text-anchor="middle" font-size="9" fill="#F57C00">1.0</text>
# MAGIC <text x="102.5" y="306" text-anchor="end" font-size="10.5" fill="#1c2733" transform="rotate(-35 102.5 306)">Std dev</text>
# MAGIC <rect x="173.2" y="275.4" width="36.4" height="14.6" fill="#2272B4"/>
# MAGIC <text x="191.4" y="272.4" text-anchor="middle" font-size="9" fill="#2272B4">10.1</text>
# MAGIC <rect x="212.6" y="288.3" width="36.4" height="1.7" fill="#F57C00"/>
# MAGIC <text x="230.9" y="285.3" text-anchor="middle" font-size="9" fill="#F57C00">1.2</text>
# MAGIC <text x="209.6" y="306" text-anchor="end" font-size="10.5" fill="#1c2733" transform="rotate(-35 209.6 306)">Series add</text>
# MAGIC <rect x="280.4" y="277.3" width="36.4" height="12.7" fill="#2272B4"/>
# MAGIC <text x="298.6" y="274.3" text-anchor="middle" font-size="9" fill="#2272B4">8.8</text>
# MAGIC <rect x="319.8" y="277.4" width="36.4" height="12.6" fill="#F57C00"/>
# MAGIC <text x="338.0" y="274.4" text-anchor="middle" font-size="9" fill="#F57C00">8.7</text>
# MAGIC <text x="316.8" y="306" text-anchor="end" font-size="10.5" fill="#1c2733" transform="rotate(-35 316.8 306)">Value counts</text>
# MAGIC <rect x="387.5" y="88.5" width="36.4" height="201.5" fill="#2272B4"/>
# MAGIC <text x="405.7" y="85.5" text-anchor="middle" font-size="9" fill="#2272B4">139.5</text>
# MAGIC <rect x="426.9" y="160.0" width="36.4" height="130.0" fill="#F57C00"/>
# MAGIC <text x="445.1" y="157.0" text-anchor="middle" font-size="9" fill="#F57C00">90.0</text>
# MAGIC <text x="423.9" y="306" text-anchor="end" font-size="10.5" fill="#1c2733" transform="rotate(-35 423.9 306)">Complex arith</text>
# MAGIC <rect x="494.6" y="95.4" width="36.4" height="194.6" fill="#2272B4"/>
# MAGIC <text x="512.9" y="92.4" text-anchor="middle" font-size="9" fill="#2272B4">134.7</text>
# MAGIC <rect x="534.1" y="249.4" width="36.4" height="40.6" fill="#F57C00"/>
# MAGIC <text x="552.3" y="246.4" text-anchor="middle" font-size="9" fill="#F57C00">28.1</text>
# MAGIC <text x="531.1" y="306" text-anchor="end" font-size="10.5" fill="#1c2733" transform="rotate(-35 531.1 306)">Mean cplx arith</text>
# MAGIC <rect x="601.8" y="231.5" width="36.4" height="58.5" fill="#2272B4"/>
# MAGIC <text x="620.0" y="228.5" text-anchor="middle" font-size="9" fill="#2272B4">40.5</text>
# MAGIC <rect x="641.2" y="283.5" width="36.4" height="6.5" fill="#F57C00"/>
# MAGIC <text x="659.4" y="280.5" text-anchor="middle" font-size="9" fill="#F57C00">4.5</text>
# MAGIC <text x="638.2" y="306" text-anchor="end" font-size="10.5" fill="#1c2733" transform="rotate(-35 638.2 306)">Groupby stats</text>
# MAGIC <rect x="708.9" y="30.4" width="36.4" height="259.6" fill="#2272B4"/>
# MAGIC <text x="727.1" y="27.4" text-anchor="middle" font-size="9" fill="#2272B4">179.7</text>
# MAGIC <rect x="748.4" y="287.7" width="36.4" height="2.3" fill="#F57C00"/>
# MAGIC <text x="766.6" y="284.7" text-anchor="middle" font-size="9" fill="#F57C00">1.6</text>
# MAGIC <text x="745.4" y="306" text-anchor="end" font-size="10.5" fill="#1c2733" transform="rotate(-35 745.4 306)">Join count</text>
# MAGIC <rect x="610" y="32" width="12" height="12" fill="#2272B4"/><text x="626" y="42" font-size="12" fill="#1c2733">pandas</text>
# MAGIC <rect x="690" y="32" width="12" height="12" fill="#F57C00"/><text x="706" y="42" font-size="12" fill="#1c2733">pandas API on Spark</text>
# MAGIC </svg>
# MAGIC </div></td><td style="vertical-align:top;font-size:1.0em;color:#33414B;padding-left:8px;"><ul style="line-height:1.7;"><li>Both <b>multi-threading</b> and the Spark SQL <b>Catalyst optimizer</b> drive the speedups.</li><li>Group-by statistics is about <b>9× faster</b>.</li></ul><div style="margin-top:12px;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:14px 16px;"><div style="font-size:22px;font-weight:800;color:#FF5F46;">10×+ faster</div><p style="margin:4px 0 0;font-size:14px;color:#33414B;">Virgin Hyperloop One cut processing from <b>hours to minutes</b> with Koalas &mdash; more than 10× faster with under 1% code changes.</p></div><p style="margin-top:12px;font-size:13px;color:#618794;"><i>Available beginning in Apache Spark 3.2.</i></p></td></tr></table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>