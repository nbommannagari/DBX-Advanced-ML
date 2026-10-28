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
# MAGIC # Lecture: Pandas UDFs and Function APIs
# MAGIC #### Pandas on Spark
# MAGIC
# MAGIC How to run custom Python logic at Spark scale — user-defined functions and why they differ in performance, how Apache Arrow makes Pandas UDFs fast, the four types of Pandas UDFs and the three Pandas Function APIs, and how to parallelize group-specific model training and inference.
# MAGIC
# MAGIC <div style="max-width:1180px;margin:20px 0 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><div style="background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-left:4px solid #FF5F46;border-radius:10px;padding:18px 22px;"><div style="display:flex;align-items:center;gap:12px;"><svg viewBox="0 0 32 32" width="30" height="30" aria-hidden="true" style="flex:0 0 30px;"><g fill="none" stroke="#FF5F46" stroke-width="1.7"><circle cx="16" cy="16" r="12"/><circle cx="16" cy="16" r="7"/><circle cx="16" cy="16" r="2.4" fill="#FF5F46"/></g></svg><div style="font-size:12px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#FF5F46;">Learning Objectives</div></div><p style="margin:12px 0 8px;font-size:15px;color:#33414B;">By the end of this lesson, you will be able to:</p><ul style="line-height:1.7;font-size:14.5px;color:#33414B;margin:0;padding-left:22px;"><li style="margin:4px 0;">Explain how Apache Arrow makes Pandas UDFs efficient.</li><li style="margin:4px 0;">Distinguish the four types of Pandas UDFs and the three Pandas Function APIs.</li><li style="margin:4px 0;">Apply Pandas UDFs and Function APIs to parallelize group-specific model training and inference.</li></ul></div></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## User-defined Functions (UDFs)
# MAGIC ### Custom logic, and what it costs
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"></p>
# MAGIC <b>Purpose:</b> a UDF runs your own logic over a Spark DataFrame &mdash; flexible, but performance varies a lot by type. Built-in and JVM functions are fastest; Python UDFs are slowest; Pandas UDFs use Arrow to close much of the gap.<ul style="line-height:1.8;font-size:1.0em;color:#33414B;margin:14px 0 4px;"><li><b>Built-in functions</b> are fastest (Databricks-optimized); <b>JVM</b> (Scala/Java/Hive) beats Python.</li><li><b>Pandas UDFs</b> use Arrow for efficient exchange; <b>Python UDFs</b> are handy but slow for large-scale ETL.</li><li><b>Benefit:</b> custom logic in familiar languages, reducing the human cost of refactoring.</li></ul><table style="border-collapse:collapse;width:100%;margin:6px 0;"><thead><tr><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">Type</th><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">Optimized</th><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">Execution environment</th><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">Notes</th></tr></thead><tbody><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Hive UDF</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">No</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">JVM</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"></td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Python UDF</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">No</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Python</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"></td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Pandas UDF</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">No</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Python (Arrow)</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><i>Arrow reduces serialization cost</i></td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Scala UDF</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">No</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">JVM</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"></td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Spark SQL</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Yes</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">JVM (Photon)</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><i>No serialization penalty</i></td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Spark DataFrame</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Yes</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">JVM (Photon)</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"></td></tr></tbody></table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Apache Arrow
# MAGIC ### Why Pandas UDFs are fast
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> moving data between Python (Pandas) and the JVM normally means costly serialization. <b>Apache Arrow</b> is a shared columnar memory format both sides read directly &mdash; removing that overhead.</p>
# MAGIC <div style="overflow-x:auto;max-width:1300px;margin:0 auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1300 300" role="img" style="min-width:1000px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC   <title>Apache Arrow removes serialization between Pandas and the JVM</title>
# MAGIC   <desc>Without Arrow, data is serialized and deserialized between the Pandas DataFrame and the JVM inside a Spark executor; with Arrow, the two exchange data directly in a shared columnar format.</desc>
# MAGIC   <defs>
# MAGIC     <marker id="aa-o" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="#F2A93B"/></marker>
# MAGIC   </defs>
# MAGIC   <!-- left: without arrow -->
# MAGIC   <rect x="20" y="30" width="600" height="240" rx="16" fill="#0B2027"/>
# MAGIC   <text x="320" y="66" text-anchor="middle" font-size="18" font-weight="700" fill="#fff">Worker Node</text>
# MAGIC   <rect x="45" y="80" width="550" height="170" rx="12" fill="#5A7A88"/>
# MAGIC   <text x="320" y="108" text-anchor="middle" font-size="16" font-weight="700" fill="#fff">Spark Executor</text>
# MAGIC   <rect x="70" y="130" width="180" height="100" rx="10" fill="#98102a"/><text x="160" y="175" text-anchor="middle" font-size="15" font-weight="700" fill="#fff">Pandas</text><text x="160" y="196" text-anchor="middle" font-size="15" font-weight="700" fill="#fff">DataFrame</text>
# MAGIC   <rect x="390" y="130" width="180" height="100" rx="10" fill="#98102a"/><text x="480" y="170" text-anchor="middle" font-size="15" font-weight="700" fill="#fff">JVM</text><text x="480" y="192" text-anchor="middle" font-size="13" font-style="italic" fill="#fff">Work Done Here</text>
# MAGIC   <text x="320" y="132" text-anchor="middle" font-size="13" font-style="italic" fill="#F2A93B">Serialize/Deserialize</text>
# MAGIC   <path d="M270 165 q50 -26 100 0" fill="none" stroke="#F2A93B" stroke-width="6" marker-end="url(#aa-o)"/>
# MAGIC   <path d="M370 200 q-50 26 -100 0" fill="none" stroke="#F2A93B" stroke-width="6" marker-end="url(#aa-o)"/>
# MAGIC   <text x="320" y="240" text-anchor="middle" font-size="13" font-style="italic" fill="#F2A93B">Serialize/Deserialize</text>
# MAGIC   <!-- right: with arrow -->
# MAGIC   <rect x="680" y="30" width="600" height="240" rx="16" fill="#0B2027"/>
# MAGIC   <text x="980" y="66" text-anchor="middle" font-size="18" font-weight="700" fill="#fff">Worker Node</text>
# MAGIC   <rect x="705" y="80" width="550" height="170" rx="12" fill="#5A7A88"/>
# MAGIC   <text x="980" y="108" text-anchor="middle" font-size="16" font-weight="700" fill="#fff">Spark Executor</text>
# MAGIC   <rect x="730" y="130" width="180" height="100" rx="10" fill="#98102a"/><text x="820" y="168" text-anchor="middle" font-size="15" font-weight="700" fill="#fff">Pandas DataFrame</text><text x="820" y="200" text-anchor="middle" font-size="12" font-weight="700" fill="#F2A93B">APACHE ARROW &#187;&#187;</text>
# MAGIC   <rect x="1050" y="130" width="180" height="100" rx="10" fill="#98102a"/><text x="1140" y="162" text-anchor="middle" font-size="15" font-weight="700" fill="#fff">JVM</text><text x="1140" y="182" text-anchor="middle" font-size="12" font-style="italic" fill="#fff">Work Done Here</text><text x="1140" y="204" text-anchor="middle" font-size="12" font-weight="700" fill="#F2A93B">APACHE ARROW &#187;&#187;</text>
# MAGIC   <path d="M910 180 h140" fill="none" stroke="#F2A93B" stroke-width="7" marker-end="url(#aa-o)" marker-start="url(#aa-o)"/>
# MAGIC </svg>
# MAGIC </div><div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:18px;"><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M8 6 h16 v20 h-16 z"/><path d="M8 12 h16 M8 18 h16"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Without Arrow</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Serialize / deserialize</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">The Pandas DataFrame is serialized/deserialized to and from the JVM &mdash; a real performance cost from format conversion.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M4 16 h24 M20 10 l8 6 l-8 6 M12 10 l-8 6 l8 6"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">With Arrow</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Shared columnar format</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">A language-independent columnar format lets Pandas and the JVM share data with no serialization &mdash; libraries exist for 11+ languages.</p></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Pandas UDFs
# MAGIC ### Scale out with vectorized operations
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> a Pandas UDF processes a whole <b>batch</b> as a pandas Series rather than one row at a time. Vectorized execution over Arrow can be up to <b>100× faster</b> than a row-at-a-time Python UDF.</p>
# MAGIC <table><colgroup><col style="width:48%"><col style="width:52%"></colgroup><tr><td style="font-size:1.0em;vertical-align:top;color:#33414B;"><ul style="line-height:1.7;"><li>Use native <b>Pandas operations</b> inside the function.</li><li>Exchange data over <b>Apache Arrow</b> with near-zero overhead.</li><li>Executed in a <b>vectorized</b> manner.</li></ul><b>Writing one:</b><ul style="line-height:1.7;"><li>Use <code>pandas_udf</code> as a decorator.</li><li>Python <b>type hints are required</b>.</li><li>There are 4 types.</li></ul></td><td style="vertical-align:top;"><div style="overflow-x:auto;max-width:700px;margin:0 auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 700 380" role="img" style="min-width:560px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC <title>Performance comparison (shorter is better)</title><desc>Row-at-a-time UDF versus Pandas UDF elapsed time for plus one, cdf and subtract mean.</desc>
# MAGIC <text x="367" y="18" text-anchor="middle" font-size="14" font-weight="700" fill="#1c2733">Performance Comparison (shorter is better)</text>
# MAGIC <line x1="54" y1="34" x2="54" y2="320" stroke="#999"/>
# MAGIC <line x1="54" y1="320" x2="680" y2="320" stroke="#999"/>
# MAGIC <line x1="54" y1="320.0" x2="680" y2="320.0" stroke="#eee"/>
# MAGIC <text x="48" y="324.0" text-anchor="end" font-size="11" fill="#777">0</text>
# MAGIC <line x1="54" y1="272.3" x2="680" y2="272.3" stroke="#eee"/>
# MAGIC <text x="48" y="276.3" text-anchor="end" font-size="11" fill="#777">50</text>
# MAGIC <line x1="54" y1="224.7" x2="680" y2="224.7" stroke="#eee"/>
# MAGIC <text x="48" y="228.7" text-anchor="end" font-size="11" fill="#777">100</text>
# MAGIC <line x1="54" y1="177.0" x2="680" y2="177.0" stroke="#eee"/>
# MAGIC <text x="48" y="181.0" text-anchor="end" font-size="11" fill="#777">150</text>
# MAGIC <line x1="54" y1="129.3" x2="680" y2="129.3" stroke="#eee"/>
# MAGIC <text x="48" y="133.3" text-anchor="end" font-size="11" fill="#777">200</text>
# MAGIC <line x1="54" y1="81.7" x2="680" y2="81.7" stroke="#eee"/>
# MAGIC <text x="48" y="85.7" text-anchor="end" font-size="11" fill="#777">250</text>
# MAGIC <line x1="54" y1="34.0" x2="680" y2="34.0" stroke="#eee"/>
# MAGIC <text x="48" y="38.0" text-anchor="end" font-size="11" fill="#777">300</text>
# MAGIC <text x="16" y="177" font-size="12" fill="#555" transform="rotate(-90 16 177)" text-anchor="middle">Time (seconds)</text>
# MAGIC <rect x="95.7" y="317.0" width="58.4" height="3.0" fill="#2272B4"/>
# MAGIC <text x="124.9" y="313.0" text-anchor="middle" font-size="11" fill="#2272B4">3.15</text>
# MAGIC <rect x="160.2" y="319.1" width="58.4" height="0.9" fill="#F57C00"/>
# MAGIC <text x="189.4" y="315.1" text-anchor="middle" font-size="11" fill="#F57C00">0.9</text>
# MAGIC <text x="154.2" y="338" text-anchor="middle" font-size="12" fill="#1c2733">plus one</text>
# MAGIC <rect x="304.4" y="89.3" width="58.4" height="230.7" fill="#2272B4"/>
# MAGIC <text x="333.6" y="85.3" text-anchor="middle" font-size="11" fill="#2272B4">242</text>
# MAGIC <rect x="368.8" y="319.0" width="58.4" height="1.0" fill="#F57C00"/>
# MAGIC <text x="398.0" y="315.0" text-anchor="middle" font-size="11" fill="#F57C00">1.1</text>
# MAGIC <text x="362.8" y="338" text-anchor="middle" font-size="12" fill="#1c2733">cdf</text>
# MAGIC <rect x="513.1" y="208.5" width="58.4" height="111.5" fill="#2272B4"/>
# MAGIC <text x="542.3" y="204.5" text-anchor="middle" font-size="11" fill="#2272B4">117</text>
# MAGIC <rect x="577.5" y="313.1" width="58.4" height="6.9" fill="#F57C00"/>
# MAGIC <text x="606.7" y="309.1" text-anchor="middle" font-size="11" fill="#F57C00">7.2</text>
# MAGIC <text x="571.5" y="338" text-anchor="middle" font-size="12" fill="#1c2733">subtract mean</text>
# MAGIC <rect x="64" y="36" width="12" height="12" fill="#2272B4"/><text x="80" y="46" font-size="11" fill="#1c2733">row-at-a-time UDF</text>
# MAGIC <rect x="204" y="36" width="12" height="12" fill="#F57C00"/><text x="220" y="46" font-size="11" fill="#1c2733">Pandas UDF</text>
# MAGIC </svg>
# MAGIC </div><p style="text-align:center;font-weight:700;color:#33414B;margin-top:4px;">Up to 100× faster than row-at-a-time Python UDFs.</p></td></tr></table><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">A Pandas UDF</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">import</span> pandas <span style="color:#0B5FA5;">as</span> pd
# MAGIC <span style="color:#0B5FA5;">from</span> pyspark.sql.functions <span style="color:#0B5FA5;">import</span> pandas_udf
# MAGIC &#8203;
# MAGIC @pandas_udf(<span style="color:#2E8B57;">&#39;long&#39;</span>)
# MAGIC <span style="color:#0B5FA5;">def</span> pandas_plus_one(s: pd.Series) -&gt; pd.Series:
# MAGIC     <span style="color:#0B5FA5;">return</span> s + 1
# MAGIC &#8203;
# MAGIC spark.range(10).select(pandas_plus_one(<span style="color:#2E8B57;">&quot;id&quot;</span>)).show()</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Pandas UDFs
# MAGIC ### The four types
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> the four Pandas UDF types differ by what goes in and out &mdash; Series, an iterator of Series (for setup like loading a model once), multiple Series, or a single scalar aggregate.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:0;"><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="6" width="7" height="20" rx="1.5"/><path d="M14 16 h5 M17 13 l3 3 l-3 3"/><rect x="21" y="6" width="7" height="20" rx="1.5"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">1 → 1</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Series to Series</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Takes one or more <code>pandas.Series</code>, returns one; input and output have the same number of rows.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M8 8 a10 10 0 1 0 4 -3"/><path d="M12 3 l1 6 l-6 1"/><rect x="13" y="13" width="6" height="6" rx="1"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">batched</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Iterator of Series → Iterator</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Takes an iterator of batches, returns one &mdash; useful for state init, e.g. loading a model once for inference.</p></div></div><div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:18px;"><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="6" width="5" height="20" rx="1.3"/><rect x="10" y="6" width="5" height="20" rx="1.3"/><path d="M17 16 h4 M19 13 l3 3 l-3 3"/><rect x="23" y="6" width="5" height="20" rx="1.3"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">multi-in</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Iterator of multiple Series → Iterator</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Takes an iterator of several Series, returns a single Series &mdash; ideal for stateful ops and prefetching.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="6" width="7" height="20" rx="1.5"/><path d="M14 16 h5 M17 13 l3 3 l-3 3"/></g><g fill="#FF5F46"><circle cx="25" cy="16" r="3.5"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">→ scalar</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Series to scalar</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Reduces one or more Series to a single value &mdash; use with <code>select</code>, <code>groupBy.agg</code>, <code>Window</code>.</p></div></div><div style="text-align:center;color:#F2A93B;font-weight:700;margin-top:14px;">Series = a DataFrame column &nbsp;·&nbsp; Scalar = a single discrete value</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 4 Types of Pandas UDFs
# MAGIC ### One example of each
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> the same <code>@pandas_udf</code> decorator expresses all four types &mdash; the type is inferred from the function's input and output <b>type hints</b>.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 420px;min-width:360px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Series to Series</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;">@pandas_udf(<span style="color:#2E8B57;">&#39;double&#39;</span>)
# MAGIC <span style="color:#0B5FA5;">def</span> standardize(s: pd.Series) -&gt; pd.Series:
# MAGIC     <span style="color:#0B5FA5;">return</span> (s - s.mean()) / s.std()
# MAGIC &#8203;
# MAGIC df.select(standardize(<span style="color:#2E8B57;">&quot;feature_column&quot;</span>)).show()</pre></div><div style="flex:1 1 420px;min-width:360px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Iterator of Series → Iterator</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;">@pandas_udf(<span style="color:#2E8B57;">&quot;double&quot;</span>)
# MAGIC <span style="color:#0B5FA5;">def</span> model_inference(it: Iterator[pd.Series]) -&gt; Iterator[pd.Series]:
# MAGIC     model = load_model()          <span style="color:#6E8B6E;font-style:italic;"># load once</span>
# MAGIC     <span style="color:#0B5FA5;">for</span> batch <span style="color:#0B5FA5;">in</span> it:
# MAGIC         <span style="color:#0B5FA5;">yield</span> pd.Series(model.predict(batch))
# MAGIC &#8203;
# MAGIC df.select(model_inference(<span style="color:#2E8B57;">&quot;features&quot;</span>)).show()</pre></div></div><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 420px;min-width:360px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Iterator of multiple Series → Iterator</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;">@pandas_udf(<span style="color:#2E8B57;">&quot;double&quot;</span>)
# MAGIC <span style="color:#0B5FA5;">def</span> model_inference(it: Iterator[Tuple[pd.Series, pd.Series]]) -&gt; Iterator[pd.Series]:
# MAGIC     model = load_model()
# MAGIC     <span style="color:#0B5FA5;">for</span> f1, f2 <span style="color:#0B5FA5;">in</span> it:
# MAGIC         X = np.column_stack((f1, f2))
# MAGIC         <span style="color:#0B5FA5;">yield</span> pd.Series(model.predict(X))
# MAGIC &#8203;
# MAGIC df.select(model_inference(<span style="color:#2E8B57;">&quot;f1&quot;</span>, <span style="color:#2E8B57;">&quot;f2&quot;</span>)).show()</pre></div><div style="flex:1 1 420px;min-width:360px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Series to scalar</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;">@pandas_udf(<span style="color:#2E8B57;">&quot;double&quot;</span>)
# MAGIC <span style="color:#0B5FA5;">def</span> accuracy(pred: pd.Series, label: pd.Series) -&gt; float:
# MAGIC     <span style="color:#0B5FA5;">return</span> (pred == label).mean()
# MAGIC &#8203;
# MAGIC df.select(accuracy(<span style="color:#2E8B57;">&quot;predicted&quot;</span>, <span style="color:#2E8B57;">&quot;actual&quot;</span>)).show()</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Pandas Function APIs in Spark
# MAGIC ### Optimizing table-level operations with Pandas
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> where a Pandas UDF operates on Series (columns), a <b>Pandas Function API</b> operates on whole <b>DataFrames</b> &mdash; letting you run native pandas DataFrame logic (join, sort, filter) per partition or group.</p>
# MAGIC <ul style="line-height:1.8;font-size:1.0em;color:#33414B;margin:0 0 4px;"><li>Native <b>pandas DataFrame</b> operations inside the function.</li><li>Like Pandas UDFs, they use <b>Arrow</b> and run <b>vectorized</b>; type hints are <b>optional</b>.</li><li>Input/output are <b>DataFrames</b>, with flexible output length. Three types: <b>Map · Grouped Map · Cogrouped Map</b>.</li></ul><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">mapInPandas</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">from</span> typing <span style="color:#0B5FA5;">import</span> Iterator
# MAGIC <span style="color:#0B5FA5;">import</span> pandas <span style="color:#0B5FA5;">as</span> pd
# MAGIC &#8203;
# MAGIC df = spark.createDataFrame([(1, 21), (2, 30)], (<span style="color:#2E8B57;">&quot;id&quot;</span>, <span style="color:#2E8B57;">&quot;age&quot;</span>))
# MAGIC &#8203;
# MAGIC <span style="color:#0B5FA5;">def</span> pandas_filter(it: Iterator[pd.DataFrame]) -&gt; Iterator[pd.DataFrame]:
# MAGIC     <span style="color:#0B5FA5;">for</span> pdf <span style="color:#0B5FA5;">in</span> it:
# MAGIC         <span style="color:#0B5FA5;">yield</span> pdf[pdf.id == 1]
# MAGIC &#8203;
# MAGIC df.mapInPandas(pandas_filter, schema=df.schema).show()</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Pandas Function APIs
# MAGIC ### Three types: Map, Grouped Map, Cogrouped Map
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> the three function-API types differ by <b>what they operate over</b> &mdash; each partition, each group of one DataFrame, or matched groups of two DataFrames.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:0;"><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="6" width="9" height="20" rx="1.5"/><path d="M15 16 h4 M17 13 l3 3 l-3 3"/><rect x="21" y="6" width="7" height="20" rx="1.5"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Per partition</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Map</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;"><b>Operation:</b> a function per partition (batch). <b>In:</b> the whole DataFrame split into partitions. <b>Out:</b> a transformed DataFrame, usually the same schema.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="10" r="3"/><circle cx="9" cy="20" r="3"/><rect x="17" y="7" width="11" height="18" rx="2"/><path d="M12 10 h5 M12 20 h5"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Per group</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Grouped Map</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;"><b>Operation:</b> a function per group of one DataFrame, defined by a key. <b>In:</b> a DataFrame grouped by columns. <b>Out:</b> a transformed DataFrame.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="7" cy="9" r="2.6"/><circle cx="7" cy="23" r="2.6"/><circle cx="25" cy="9" r="2.6"/><circle cx="25" cy="23" r="2.6"/><rect x="12" y="12" width="8" height="8" rx="1.5"/><path d="M9.5 9 h2.5 M9.5 23 h2.5 M20 16 h2.5 M22.5 9 h-2.5"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Two DataFrames</div><div style="font-size:15.5px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Cogrouped Map</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;"><b>Operation:</b> group two DataFrames by a common key and apply the function per group. <b>Out:</b> a combined or transformed DataFrame.</p></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Pandas Function APIs
# MAGIC ### One example of each type
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> each type maps to a common ML data task &mdash; normalize per partition, aggregate per group, or align two datasets by key.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 360px;min-width:320px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Map — normalize per partition</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">def</span> normalize_partition(pdf):
# MAGIC     <span style="color:#0B5FA5;">return</span> (pdf - pdf.mean()) / pdf.std()
# MAGIC &#8203;
# MAGIC df.mapInPandas(normalize_partition, schema=df.schema)</pre></div><div style="flex:1 1 360px;min-width:320px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Grouped Map — mean per group</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">def</span> calculate_group_mean(pdf):
# MAGIC     <span style="color:#0B5FA5;">return</span> pdf.assign(mean_value=pdf[<span style="color:#2E8B57;">&#39;feature&#39;</span>].mean())
# MAGIC &#8203;
# MAGIC df.groupby(<span style="color:#2E8B57;">&#39;group_key&#39;</span>).applyInPandas(
# MAGIC     calculate_group_mean, schema=df.schema)</pre></div><div style="flex:1 1 360px;min-width:320px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Cogrouped Map — align two sets</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">def</span> align_features(pdf1, pdf2):
# MAGIC     <span style="color:#0B5FA5;">return</span> pdf1.merge(pdf2, on=<span style="color:#2E8B57;">&#39;key&#39;</span>)
# MAGIC &#8203;
# MAGIC df1.cogroup(df2).applyInPandas(align_features, schema=<span style="color:#2E8B57;">&quot;...&quot;</span>)</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Parallelizing Group-Specific Model Training
# MAGIC ### Grouped map: one model per group of rows
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> <code>groupby(...).applyInPandas(...)</code> trains a <b>separate model per group</b> (e.g. per store, stock, or device) &mdash; and runs those trainings <b>in parallel</b> across executors instead of one after another.</p>
# MAGIC <table><colgroup><col style="width:42%"><col style="width:58%"></colgroup><tr><td style="font-size:1.0em;vertical-align:top;color:#33414B;"><ul style="line-height:1.7;"><li><b>Train one model per group of rows</b>, defined by a column's values.</li><li><b>Examples:</b> a model per store/product (retail), per stock symbol (finance), per metric (healthcare), or per IoT device.</li></ul><div style="max-width:520px;margin-top:10px;"><svg width="100%" viewBox="0 0 520 180" role="img" style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC   <title>Sequential versus parallel group training</title><desc>Sequential runs groups one after another; parallel runs each group on its own executor at the same time.</desc>
# MAGIC   <text x="130" y="20" text-anchor="middle" font-size="14" font-weight="800" fill="#FF5F46">Sequential</text>
# MAGIC   <g font-size="11" fill="#fff" text-anchor="middle">
# MAGIC     <rect x="20" y="40" width="70" height="34" rx="5" fill="#5A7A88"/><text x="55" y="62">Group #1</text>
# MAGIC     <rect x="95" y="40" width="70" height="34" rx="5" fill="#5A7A88"/><text x="130" y="62">Group #2</text>
# MAGIC     <rect x="170" y="40" width="70" height="34" rx="5" fill="#5A7A88"/><text x="205" y="62">Group #3</text>
# MAGIC   </g>
# MAGIC   <line x1="20" y1="90" x2="240" y2="90" stroke="#FF5F46" stroke-width="2"/><text x="130" y="106" text-anchor="middle" font-size="10" fill="#555">Time</text>
# MAGIC   <text x="400" y="20" text-anchor="middle" font-size="14" font-weight="800" fill="#FF5F46">Parallel</text>
# MAGIC   <g font-size="11" fill="#fff" text-anchor="middle">
# MAGIC     <rect x="330" y="34" width="90" height="26" rx="5" fill="#5A7A88"/><text x="375" y="51">Group #1</text>
# MAGIC     <rect x="330" y="66" width="90" height="26" rx="5" fill="#5A7A88"/><text x="375" y="83">Group #2</text>
# MAGIC     <rect x="330" y="98" width="90" height="26" rx="5" fill="#5A7A88"/><text x="375" y="115">Group #3</text>
# MAGIC   </g>
# MAGIC   <g font-size="9" fill="#555"><text x="312" y="51">E1</text><text x="312" y="83">E2</text><text x="312" y="115">E3</text></g>
# MAGIC   <line x1="330" y1="140" x2="500" y2="140" stroke="#FF5F46" stroke-width="2"/><text x="415" y="156" text-anchor="middle" font-size="10" fill="#555">Time</text>
# MAGIC </svg></div></td><td style="vertical-align:top;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Grouped-map training</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">import</span> pandas <span style="color:#0B5FA5;">as</span> pd
# MAGIC <span style="color:#0B5FA5;">from</span> sklearn.linear_model <span style="color:#0B5FA5;">import</span> LinearRegression
# MAGIC &#8203;
# MAGIC <span style="color:#0B5FA5;">def</span> train_model(pdf: pd.DataFrame) -&gt; pd.DataFrame:
# MAGIC     model = LinearRegression()
# MAGIC     X, y = pdf[[<span style="color:#2E8B57;">&#39;feature1&#39;</span>, <span style="color:#2E8B57;">&#39;feature2&#39;</span>]], pdf[<span style="color:#2E8B57;">&#39;target&#39;</span>]
# MAGIC     model.fit(X, y)
# MAGIC     pdf[<span style="color:#2E8B57;">&#39;predictions&#39;</span>] = model.predict(X)
# MAGIC     <span style="color:#0B5FA5;">return</span> pdf
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># One model per store_id, all groups trained in parallel</span>
# MAGIC df.groupby(<span style="color:#2E8B57;">&#39;store_id&#39;</span>).applyInPandas(train_model, schema=df.schema)</pre></div></td></tr></table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Parallelizing Group-Specific Model Inferencing
# MAGIC ### Route each group to its own model
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> the same grouped pattern applies at inference &mdash; split the data by group key, score each subset with its group's model, and record which model served each group.</p>
# MAGIC <div style="overflow-x:auto;max-width:1300px;margin:0 auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1300 560" role="img" style="min-width:1040px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC   <title>Group-specific model inference</title>
# MAGIC   <desc>A dataset is split by group key into A and B subsets, each scored by its own group model, and the model paths are recorded per group.</desc>
# MAGIC   <defs><marker id="gi-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="#FF5F46"/></marker></defs>
# MAGIC   <!-- source table -->
# MAGIC   <g font-size="22" font-weight="700" text-anchor="middle">
# MAGIC     <rect x="40" y="140" width="70" height="60" fill="#1CA971"/><text x="75" y="180" fill="#fff">Y</text>
# MAGIC     <rect x="110" y="140" width="90" height="60" fill="#5A7A88"/><text x="155" y="180" fill="#fff">X1</text>
# MAGIC     <rect x="200" y="140" width="90" height="60" fill="#5A7A88"/><text x="245" y="180" fill="#fff">X2</text>
# MAGIC   </g>
# MAGIC   <g font-size="22" text-anchor="middle" fill="#1c2733">
# MAGIC     <rect x="40" y="200" width="70" height="60" fill="#ededed" stroke="#bbb"/><text x="75" y="240">…</text><rect x="110" y="200" width="90" height="60" fill="#ededed" stroke="#bbb"/><text x="155" y="240">A</text><rect x="200" y="200" width="90" height="60" fill="#ededed" stroke="#bbb"/><text x="245" y="240">…</text>
# MAGIC     <rect x="40" y="260" width="70" height="60" fill="#ededed" stroke="#bbb"/><text x="75" y="300">…</text><rect x="110" y="260" width="90" height="60" fill="#ededed" stroke="#bbb"/><text x="155" y="300">A</text><rect x="200" y="260" width="90" height="60" fill="#ededed" stroke="#bbb"/><text x="245" y="300">…</text>
# MAGIC     <rect x="40" y="320" width="70" height="60" fill="#ededed" stroke="#bbb"/><text x="75" y="360">…</text><rect x="110" y="320" width="90" height="60" fill="#ededed" stroke="#bbb"/><text x="155" y="360">B</text><rect x="200" y="320" width="90" height="60" fill="#ededed" stroke="#bbb"/><text x="245" y="360">…</text>
# MAGIC     <rect x="40" y="380" width="70" height="60" fill="#ededed" stroke="#bbb"/><text x="75" y="420">…</text><rect x="110" y="380" width="90" height="60" fill="#ededed" stroke="#bbb"/><text x="155" y="420">B</text><rect x="200" y="380" width="90" height="60" fill="#ededed" stroke="#bbb"/><text x="245" y="420">…</text>
# MAGIC   </g>
# MAGIC   <!-- split subsets -->
# MAGIC   <g font-size="18" font-weight="700" text-anchor="middle">
# MAGIC     <rect x="420" y="90" width="56" height="46" fill="#1CA971"/><text x="448" y="122" fill="#fff">Y</text><rect x="476" y="90" width="70" height="46" fill="#5A7A88"/><text x="511" y="122" fill="#fff">X1</text><rect x="546" y="90" width="70" height="46" fill="#5A7A88"/><text x="581" y="122" fill="#fff">X2</text>
# MAGIC     <rect x="420" y="136" width="56" height="46" fill="#ededed" stroke="#bbb"/><rect x="476" y="136" width="70" height="46" fill="#ededed" stroke="#bbb"/><text x="511" y="168" fill="#1c2733">A</text><rect x="546" y="136" width="70" height="46" fill="#ededed" stroke="#bbb"/>
# MAGIC     <rect x="420" y="182" width="56" height="46" fill="#ededed" stroke="#bbb"/><rect x="476" y="182" width="70" height="46" fill="#ededed" stroke="#bbb"/><text x="511" y="214" fill="#1c2733">A</text><rect x="546" y="182" width="70" height="46" fill="#ededed" stroke="#bbb"/>
# MAGIC     <rect x="420" y="330" width="56" height="46" fill="#1CA971"/><text x="448" y="362" fill="#fff">Y</text><rect x="476" y="330" width="70" height="46" fill="#5A7A88"/><text x="511" y="362" fill="#fff">X1</text><rect x="546" y="330" width="70" height="46" fill="#5A7A88"/><text x="581" y="362" fill="#fff">X2</text>
# MAGIC     <rect x="420" y="376" width="56" height="46" fill="#ededed" stroke="#bbb"/><rect x="476" y="376" width="70" height="46" fill="#ededed" stroke="#bbb"/><text x="511" y="408" fill="#1c2733">B</text><rect x="546" y="376" width="70" height="46" fill="#ededed" stroke="#bbb"/>
# MAGIC     <rect x="420" y="422" width="56" height="46" fill="#ededed" stroke="#bbb"/><rect x="476" y="422" width="70" height="46" fill="#ededed" stroke="#bbb"/><text x="511" y="454" fill="#1c2733">B</text><rect x="546" y="422" width="70" height="46" fill="#ededed" stroke="#bbb"/>
# MAGIC   </g>
# MAGIC   <!-- model boxes -->
# MAGIC   <rect x="720" y="120" width="160" height="80" rx="12" fill="#5A7A88"/><text x="800" y="155" text-anchor="middle" font-size="18" font-weight="700" fill="#fff">Group A</text><text x="800" y="180" text-anchor="middle" font-size="18" font-weight="700" fill="#fff">Model</text>
# MAGIC   <rect x="720" y="360" width="160" height="80" rx="12" fill="#5A7A88"/><text x="800" y="395" text-anchor="middle" font-size="18" font-weight="700" fill="#fff">Group B</text><text x="800" y="420" text-anchor="middle" font-size="18" font-weight="700" fill="#fff">Model</text>
# MAGIC   <!-- model path table -->
# MAGIC   <g font-size="20" font-weight="700" text-anchor="middle">
# MAGIC     <rect x="1000" y="230" width="70" height="56" fill="#1CA971"/><text x="1035" y="266" fill="#fff">X1</text><rect x="1070" y="230" width="220" height="56" fill="#5A7A88"/><text x="1180" y="266" fill="#fff">Model Path</text>
# MAGIC     <rect x="1000" y="286" width="70" height="56" fill="#ededed" stroke="#bbb"/><text x="1035" y="322" fill="#1c2733">A</text><rect x="1070" y="286" width="220" height="56" fill="#ededed" stroke="#bbb"/><text x="1180" y="322" fill="#1c2733">…</text>
# MAGIC     <rect x="1000" y="342" width="70" height="56" fill="#ededed" stroke="#bbb"/><text x="1035" y="378" fill="#1c2733">B</text><rect x="1070" y="342" width="220" height="56" fill="#ededed" stroke="#bbb"/><text x="1180" y="378" fill="#1c2733">…</text>
# MAGIC   </g>
# MAGIC   <!-- arrows -->
# MAGIC   <g stroke="#FF5F46" stroke-width="2.4" fill="none">
# MAGIC     <path d="M290 250 L416 150" marker-end="url(#gi-ar)"/>
# MAGIC     <path d="M290 350 L416 400" marker-end="url(#gi-ar)"/>
# MAGIC     <path d="M616 159 L716 160" marker-end="url(#gi-ar)"/>
# MAGIC     <path d="M616 399 L716 400" marker-end="url(#gi-ar)"/>
# MAGIC     <path d="M880 160 L996 300" marker-end="url(#gi-ar)"/>
# MAGIC     <path d="M880 400 L996 340" marker-end="url(#gi-ar)"/>
# MAGIC   </g>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>