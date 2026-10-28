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
# MAGIC # Lecture: Model Tracking and Packaging with MLflow
# MAGIC #### Machine Learning Development with Spark
# MAGIC
# MAGIC How MLflow packages a trained model as a portable, self-describing artifact — the MLflow Model directory and its `MLmodel` file, model *flavors* for interoperability, and the Model Registry for centralized lifecycle management, including registering a model to Unity Catalog.
# MAGIC
# MAGIC <div style="max-width:1180px;margin:20px 0 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><div style="background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-left:4px solid #FF5F46;border-radius:10px;padding:18px 22px;"><div style="display:flex;align-items:center;gap:12px;"><svg viewBox="0 0 32 32" width="30" height="30" aria-hidden="true" style="flex:0 0 30px;"><g fill="none" stroke="#FF5F46" stroke-width="1.7"><circle cx="16" cy="16" r="12"/><circle cx="16" cy="16" r="7"/><circle cx="16" cy="16" r="2.4" fill="#FF5F46"/></g></svg><div style="font-size:12px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#FF5F46;">Learning Objectives</div></div><p style="margin:12px 0 8px;font-size:15px;color:#33414B;">By the end of this lesson, you will be able to:</p><ul style="line-height:1.7;font-size:14.5px;color:#33414B;margin:0;padding-left:22px;"><li style="margin:4px 0;">Explain how MLflow packages a model as a portable, self-describing artifact — the <code>MLmodel</code> file and model flavors.</li><li style="margin:4px 0;">Track experiments, parameters, metrics, and artifacts with MLflow.</li><li style="margin:4px 0;">Register a model to the Model Registry in Unity Catalog and manage its lifecycle.</li></ul></div></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## MLflow Models
# MAGIC ### A portable, self-describing model artifact
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> when you log a model, MLflow saves it as a self-contained directory &mdash; arbitrary files plus an <code>MLmodel</code> descriptor at the root. From the model page you get its schema and copy-paste prediction snippets for both Spark and pandas.</p>
# MAGIC <div style="background:#1B5162;color:#fff;border-radius:6px;padding:16px 20px;font-size:1.05em;line-height:1.55;">Each <b>MLflow Model</b> is a directory of arbitrary files, together with an <b>MLmodel</b> descriptor at its root.</div><div style="text-align:center;margin:16px 0 4px;"><img src="https://files.training.databricks.com/binder/prod_main/machine-learning-at-scale-en_us-1.3.4/images/20261023T044612Z/Includes/images/MLflow Model artifacts view.png" alt="MLflow Model artifacts view" style="max-width:730px;width:100%;border:1px solid #E3E6E8;border-radius:6px;"/></div><p style="font-size:1.0em;color:#33414B;margin:14px 0 0;">The model page shows its input/output <b>schema</b> and ready-to-run <b>prediction</b> snippets:</p><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 460px;min-width:380px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Load as a Spark UDF</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">import</span> mlflow
# MAGIC <span style="color:#0B5FA5;">from</span> pyspark.sql.functions <span style="color:#0B5FA5;">import</span> struct, col
# MAGIC logged_model = <span style="color:#2E8B57;">&#39;runs:/&lt;run-id&gt;/model&#39;</span>
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Load the model as a Spark UDF</span>
# MAGIC loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type=<span style="color:#2E8B57;">&#39;double&#39;</span>)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Predict on a Spark DataFrame</span>
# MAGIC df.withColumn(<span style="color:#2E8B57;">&#39;predictions&#39;</span>, loaded_model(struct(*map(col, df.columns))))</pre></div><div style="flex:1 1 460px;min-width:380px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Load as a PyFunc model</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">import</span> mlflow
# MAGIC <span style="color:#0B5FA5;">import</span> pandas <span style="color:#0B5FA5;">as</span> pd
# MAGIC logged_model = <span style="color:#2E8B57;">&#39;runs:/&lt;run-id&gt;/model&#39;</span>
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Load the model as a PyFuncModel</span>
# MAGIC loaded_model = mlflow.pyfunc.load_model(logged_model)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Predict on a pandas DataFrame</span>
# MAGIC loaded_model.predict(pd.DataFrame(data))</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Example MLflow Model
# MAGIC ### The directory and its MLmodel descriptor
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> a logged model is a self-describing directory. The <code>MLmodel</code> file at its root declares the model's <b>flavors</b>, environment and provenance &mdash; and the flavors are what let one saved model be consumed from many different tools.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:0;"><div style="flex:1 1 360px;min-width:320px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Model directory</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;">my_model/
# MAGIC ├── sparkml/
# MAGIC ├── <span style="color:#FF5F46;font-weight:700;">MLmodel</span>            ← declares the flavors
# MAGIC ├── conda.yaml
# MAGIC ├── input_example.json
# MAGIC ├── python_env.yaml
# MAGIC ├── requirements.txt
# MAGIC ├── search_results.csv
# MAGIC └── estimator_info.json</pre></div><div style="flex:1 1 460px;min-width:400px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">MLmodel</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#1B5162;font-weight:600;">artifact_path</span>: best_model
# MAGIC <span style="color:#1B5162;font-weight:600;">databricks_runtime</span>: 17.3.x-cpu-ml-scala2.12
# MAGIC <span style="color:#1B5162;font-weight:600;">flavors</span>:
# MAGIC   <span style="color:#1B5162;font-weight:600;">python_function</span>:     <span style="color:#6E8B6E;font-style:italic;"> # any tool that can run Python</span>
# MAGIC     <span style="color:#1B5162;font-weight:600;">loader_module</span>: mlflow.spark
# MAGIC     <span style="color:#1B5162;font-weight:600;">python_version</span>: 3.12.3
# MAGIC   <span style="color:#1B5162;font-weight:600;">spark</span>:               <span style="color:#6E8B6E;font-style:italic;"> # tools that understand Spark models</span>
# MAGIC     <span style="color:#1B5162;font-weight:600;">model_class</span>: pyspark.ml.pipeline.PipelineModel
# MAGIC     <span style="color:#1B5162;font-weight:600;">pyspark_version</span>: 4.0.0
# MAGIC <span style="color:#1B5162;font-weight:600;">mlflow_version</span>: 3.1.0
# MAGIC <span style="color:#1B5162;font-weight:600;">run_id</span>: 4575c73360594fc19a482318d573f43c</pre></div></div><div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:18px;"><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M12 6 C8 6 8 12 5 12 C8 12 8 26 12 26"/><path d="M20 6 C24 6 24 12 27 12 C24 12 24 26 20 26"/></g><g fill="#FF5F46"><circle cx="16" cy="16" r="2"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Flavor</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">python_function</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Makes the model loadable by <b>any tool that can run Python</b> &mdash; the universal pyfunc interface.</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M17 3 l2.5 8.5 L28 12 l-7 5 l2.5 8.5 L16 20 l-7.5 5.5 L11 17 L4 12 l8.5 -0.5 Z"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Flavor</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">spark</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Makes the same model usable by <b>tools that understand the Spark model format</b>.</p></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## MLflow Model Flavors
# MAGIC ### One model, many tools
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> a <b>flavor</b> is a convention that lets a single logged model be consumed in different ways. The <code>python_function</code> (pyfunc) flavor is the universal one; MLflow also ships flavors for the major ML frameworks.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:24px;align-items:flex-start;"><div style="flex:1 1 420px;min-width:360px;"><div style="flex:1 1 100%;min-width:100%;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="6" y="10" width="20" height="12" rx="2"/><path d="M2 14 h4 M2 18 h4 M26 14 h4 M26 18 h4"/><path d="M12 16 h8"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Universal interface</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Python function (mlflow.pyfunc)</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">The default interface for MLflow Python models &mdash; every model is expected to load as a plain python function. It carries everything needed to <b>load and use a model</b>, via <code>log_model</code>, <code>save_model</code>, <code>load_model</code> and <code>predict</code>.</p></div></div><div style="flex:1 1 380px;min-width:320px;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;margin-bottom:8px;">Built-in flavors</div><div><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">Python function</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">Spark MLlib</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">Scikit-learn</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">PyTorch</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">TensorFlow</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">OpenAI</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">LangChain</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">HuggingFace</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">Custom flavors</span></div></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## MLflow Model Registry
# MAGIC ### Centralized, collaborative model lifecycle management
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> the Model Registry is where trained models live after tracking. It <b>versions</b> each model and moves versions through lifecycle stages, giving data scientists and deployment engineers a shared, governed hand-off point.</p>
# MAGIC <div style="overflow-x:auto;max-width:1300px;margin:0 auto;">
# MAGIC <svg width="100%" viewBox="0 0 1300 520" role="img" style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC   <title>MLflow Tracking Server feeding the MLflow Model Registry</title>
# MAGIC   <desc>The MLflow Tracking Server captures parameters, metrics, artifacts, metadata and models, which flow into the MLflow Model Registry where data scientists and deployment engineers manage model versions across experimental, aliases, production and archived stages.</desc>
# MAGIC   <defs><marker id="mr-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#2E8BE6" stroke-width="2" stroke-linecap="round"/></marker></defs>
# MAGIC
# MAGIC   <!-- Tracking Server -->
# MAGIC   <rect x="20" y="30" width="360" height="460" rx="14" fill="#0B2027" stroke="#26414d"/>
# MAGIC   <text x="200" y="88" text-anchor="middle" font-size="26" font-weight="800" fill="#fff">ml<tspan fill="#2E8BE6" font-style="italic">flow</tspan></text>
# MAGIC   <text x="200" y="120" text-anchor="middle" font-size="20" fill="#2E8BE6">Tracking Server</text>
# MAGIC   <g fill="none" stroke="#fff" stroke-width="2">
# MAGIC     <rect x="56" y="170" width="80" height="70" rx="6"/><rect x="160" y="170" width="80" height="70" rx="6"/><rect x="264" y="170" width="80" height="70" rx="6"/>
# MAGIC     <rect x="108" y="300" width="80" height="70" rx="6"/><rect x="212" y="300" width="80" height="70" rx="6"/>
# MAGIC   </g>
# MAGIC   <g fill="#cfd8dc" font-size="14" text-anchor="middle">
# MAGIC     <text x="96" y="262">Parameters</text><text x="200" y="262">Metrics</text><text x="304" y="262">Artifacts</text>
# MAGIC     <text x="148" y="392">Metadata</text><text x="252" y="392">Models</text>
# MAGIC   </g>
# MAGIC
# MAGIC   <!-- arrow -->
# MAGIC   <path d="M382 300 h96" stroke="#2E8BE6" stroke-width="6" fill="none" marker-end="url(#mr-ar)"/>
# MAGIC
# MAGIC   <!-- Model Registry -->
# MAGIC   <rect x="500" y="30" width="780" height="460" rx="14" fill="#06131a" stroke="#26414d"/>
# MAGIC   <text x="890" y="88" text-anchor="middle" font-size="26" font-weight="800" fill="#fff">ml<tspan fill="#2E8BE6" font-style="italic">flow</tspan></text>
# MAGIC   <text x="890" y="120" text-anchor="middle" font-size="20" fill="#2E8BE6">Model Registry</text>
# MAGIC   <text x="640" y="160" text-anchor="middle" font-size="16" font-weight="700" fill="#fff">&#128100; Data Scientists</text>
# MAGIC   <text x="1150" y="160" text-anchor="middle" font-size="16" font-weight="700" fill="#fff">Deployment Engineers &#128104;</text>
# MAGIC   <!-- columns -->
# MAGIC   <g font-size="16" fill="#9fb2bd" text-anchor="middle">
# MAGIC     <text x="590" y="220">Experimental</text><text x="780" y="220">Aliases</text><text x="970" y="220">Production</text><text x="1170" y="220">Archived</text>
# MAGIC   </g>
# MAGIC   <g stroke="#26414d" stroke-width="1"><line x1="500" y1="235" x2="1280" y2="235"/><line x1="685" y1="200" x2="685" y2="490"/><line x1="875" y1="200" x2="875" y2="490"/><line x1="1065" y1="200" x2="1065" y2="490"/></g>
# MAGIC   <g fill="#7f97a3" font-size="20" text-anchor="middle">
# MAGIC     <text x="590" y="290">&#9881;</text><text x="780" y="290">&#9881;</text><text x="970" y="290">&#9881;</text>
# MAGIC     <text x="590" y="360">&#9881;</text><text x="780" y="360">&#9881;</text><text x="970" y="360">&#9881;</text><text x="1170" y="360">&#9881;</text>
# MAGIC     <text x="590" y="430">&#9881;</text>
# MAGIC   </g>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Registering a Model to Unity Catalog
# MAGIC ### Governed models with MLflow + Unity Catalog
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> pointing MLflow at Unity Catalog registers models as governed UC objects under a three-level name. A single <code>log_model</code> call logs the model and creates a new registered version, inferring the input schema from your example data.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:0;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">import</span> mlflow
# MAGIC <span style="color:#0B5FA5;">from</span> sklearn.ensemble <span style="color:#0B5FA5;">import</span> RandomForestClassifier
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Point the MLflow client at the Unity Catalog registry</span>
# MAGIC mlflow.set_registry_uri(<span style="color:#2E8B57;">&quot;databricks-uc&quot;</span>)
# MAGIC &#8203;
# MAGIC X, y = datasets.wine_quality(return_X_y=<span style="color:#0B5FA5;">True</span>, as_frame=<span style="color:#0B5FA5;">True</span>)
# MAGIC clf = RandomForestClassifier(max_depth=7)
# MAGIC clf.fit(X, y)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Log + register in one call; the schema is inferred from input_example</span>
# MAGIC input_example = X.iloc[[0]]
# MAGIC mlflow.sklearn.log_model(
# MAGIC     sk_model=clf,
# MAGIC     name=<span style="color:#2E8B57;">&quot;model&quot;</span>,
# MAGIC     input_example=input_example,
# MAGIC     registered_model_name=<span style="color:#2E8B57;">&quot;catalog.schema.model_name&quot;</span>,
# MAGIC )</pre></div></div><div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:18px;"><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="16" cy="16" r="5"/><path d="M16 3 v4 M16 25 v4 M3 16 h4 M25 16 h4 M6.5 6.5 l3 3 M22.5 22.5 l3 3 M25.5 6.5 l-3 3 M9.5 22.5 l-3 3"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Step 1</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Point at UC</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;"><code>set_registry_uri("databricks-uc")</code> tells MLflow to use the Unity Catalog registry.</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="6" y="4" width="20" height="24" rx="2"/><path d="M10 10 h12 M10 15 h12 M10 20 h7"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Step 2</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Infer the schema</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">MLflow reads the first row of <code>input_example</code> to infer the model's input signature.</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="14" width="22" height="13" rx="2"/><path d="M16 22 V5 M11 10 l5 -5 l5 5"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Step 3</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">log_model</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Logs the trained model to the tracking server with its environment and metadata.</p></div><div style="flex:1 1 300px;min-width:280px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="6" width="22" height="6" rx="1.5"/><rect x="5" y="14" width="22" height="6" rx="1.5"/><rect x="5" y="22" width="22" height="6" rx="1.5"/></g><g fill="#FF5F46"><circle cx="9" cy="9" r="1.4"/><circle cx="9" cy="17" r="1.4"/><circle cx="9" cy="25" r="1.4"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Step 4</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Register the version</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;"><code>registered_model_name</code> creates a new version (or the model) under <code>catalog.schema.name</code>.</p></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>