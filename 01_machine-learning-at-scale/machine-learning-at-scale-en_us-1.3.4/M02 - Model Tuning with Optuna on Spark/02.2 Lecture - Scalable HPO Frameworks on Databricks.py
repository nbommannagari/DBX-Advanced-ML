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
# MAGIC # Lecture: Scalable HPO Frameworks on Databricks
# MAGIC #### Distributed Model Tuning on Databricks
# MAGIC
# MAGIC The main frameworks for scalable hyperparameter optimization on Databricks — Spark ML's built-in tuning, Optuna, and Ray Tune — how they parallelize search and integrate with MLflow and Unity Catalog, and the core concepts of the Ray Tune workflow.
# MAGIC
# MAGIC <div style="max-width:1180px;margin:20px 0 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;"><div style="background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-left:4px solid #FF5F46;border-radius:10px;padding:18px 22px;"><div style="display:flex;align-items:center;gap:12px;"><svg viewBox="0 0 32 32" width="30" height="30" aria-hidden="true" style="flex:0 0 30px;"><g fill="none" stroke="#FF5F46" stroke-width="1.7"><circle cx="16" cy="16" r="12"/><circle cx="16" cy="16" r="7"/><circle cx="16" cy="16" r="2.4" fill="#FF5F46"/></g></svg><div style="font-size:12px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;color:#FF5F46;">Learning Objectives</div></div><p style="margin:12px 0 8px;font-size:15px;color:#33414B;">By the end of this lesson, you will be able to:</p><ul style="line-height:1.7;font-size:14.5px;color:#33414B;margin:0;padding-left:22px;"><li style="margin:4px 0;">Compare the scalable HPO frameworks on Databricks — Spark ML tuning, Optuna, and Ray Tune.</li><li style="margin:4px 0;">Describe how each framework parallelizes search and integrates with MLflow and Unity Catalog.</li><li style="margin:4px 0;">Explain the core concepts and workflow of Ray Tune.</li></ul></div></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Popular Tools for Hyperparameter Optimization
# MAGIC ### Four ways to tune at scale on Databricks
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> Databricks supports several tuning frameworks. Two are distributed-first (Spark ML, Ray Tune) and two are Python-native optimizers (Optuna, Hyperopt) &mdash; and the recommendation for new work is Optuna or Ray.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:0;"><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M17 3 l2.4 8.4 L27.5 12 l-6.8 4.8 l2.4 8.4 L16 19.6 l-7.5 5.4 L10.9 16.6 L4.1 12 l8.4 -0.4 Z"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Distributed</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Spark ML</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Built-in <code>CrossValidator</code> and <code>TrainValidationSplit</code> for distributed search over large datasets.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M5 26 V5 M5 26 H27"/><path d="M6 22 C 12 22, 13 11, 18 10 C 23 9, 24 16, 27 8"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Python-native</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Optuna</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Open-source framework that automates hyperparameter search with Pythonic search spaces.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="16" cy="16" r="4"/><circle cx="6" cy="8" r="2.4"/><circle cx="26" cy="8" r="2.4"/><circle cx="6" cy="24" r="2.4"/><circle cx="26" cy="24" r="2.4"/><path d="M13 13.5 L8 9.5 M19 13.5 L24 9.5 M13 18.5 L8 22 M19 18.5 L24 22"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Distributed</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Ray Tune</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">A scalable tuning library offering both grid and advanced search algorithms.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M5 26 V5 M5 26 H27"/><path d="M6 24 C 12 6, 20 6, 26 24"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Python-native</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Hyperopt</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Performs optimization using Bayesian methods.</p></div></div><div style="border-left:4px solid #FF5F46;background:#FFF6F4;padding:12px 16px;border-radius:0 6px 6px 0;margin-top:16px;font-size:14.5px;line-height:1.55;color:#33414B;"><b>Note:</b> the open-source version of Hyperopt is no longer maintained and is not pre-installed on DBRML 17.0+. Databricks primarily recommends <b>Optuna</b> or <b>Ray</b>.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Hyperparameter Optimization with Spark ML
# MAGIC ### Scalable tuning built into Spark ML
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> Spark ML tunes models with the cluster's parallelism &mdash; a <code>ParamGridBuilder</code> defines the search, a <code>CrossValidator</code> (or <code>TrainValidationSplit</code>) runs it, and the results log straight to MLflow.</p>
# MAGIC <ul style="line-height:1.85;font-size:1.0em;color:#33414B;margin:0 0 4px;"><li><b>Scalable &amp; distributed</b> &mdash; leverages Spark parallelism for large-scale tuning.</li><li><b>Built-in cross-validation</b> via <code>CrossValidator</code> and <code>TrainValidationSplit</code>.</li><li><b>Native to Spark ML pipelines</b>, and supports grid &amp; random search with <code>ParamGridBuilder</code>.</li><li><b>Integrates with MLflow</b> for tracking and reproducibility.</li></ul><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#6E8B6E;font-style:italic;"># Define model, hyperparameter grid, and evaluator</span>
# MAGIC dt = DecisionTreeRegressor(featuresCol=<span style="color:#2E8B57;">&quot;features&quot;</span>, labelCol=<span style="color:#2E8B57;">&quot;label&quot;</span>)
# MAGIC paramGrid = (
# MAGIC     ParamGridBuilder()
# MAGIC     .addGrid(dt.maxDepth, [3, 5, 10])
# MAGIC     .addGrid(dt.minInstancesPerNode, [1, 2, 4])
# MAGIC     .build()
# MAGIC )
# MAGIC evaluator = RegressionEvaluator(
# MAGIC     labelCol=<span style="color:#2E8B57;">&quot;label&quot;</span>, predictionCol=<span style="color:#2E8B57;">&quot;prediction&quot;</span>, metricName=<span style="color:#2E8B57;">&quot;rmse&quot;</span>)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Cross-validation and model training</span>
# MAGIC cv = CrossValidator(
# MAGIC     estimator=dt, estimatorParamMaps=paramGrid,
# MAGIC     evaluator=evaluator, numFolds=5)
# MAGIC cvModel = cv.fit(trainingData)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Log the best model with MLflow</span>
# MAGIC mlflow.spark.log_model(cvModel.bestModel, name=<span style="color:#2E8B57;">&quot;best_sparkml_model&quot;</span>)</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Optuna
# MAGIC ### A lightweight hyperparameter optimization framework
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"></p>
# MAGIC <b>Purpose:</b> Optuna is a define-by-run optimizer &mdash; you express the search space in plain Python inside an objective function, and it efficiently samples configurations, parallelizes trials, and visualizes results.<div style="margin-top:14px;"><div><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">Lightweight &amp; platform-agnostic</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">Pythonic search spaces</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">Efficient algorithms</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">Easy parallelization</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">Quick visualization</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#0B2027;color:#fff;font-size:13px;font-weight:600;">MLflow integration</span></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Recommended Frameworks
# MAGIC ### Optuna, end to end
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> a full Optuna run in a few lines &mdash; define an <code>objective</code> that suggests hyperparameters, create a study with a sampler (TPE), optimize in parallel with <code>n_jobs=-1</code>, then log and visualize the results.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#6E8B6E;font-style:italic;"># Define the objective function</span>
# MAGIC <span style="color:#0B5FA5;">def</span> objective(trial):
# MAGIC     <span style="color:#6E8B6E;font-style:italic;"># Write your machine learning logic here</span>
# MAGIC     max_depth = trial.suggest_int(<span style="color:#2E8B57;">&quot;max_depth&quot;</span>, 2, 20)   <span style="color:#6E8B6E;font-style:italic;"># Pythonic search space</span>
# MAGIC     ...
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Run optimization with parallelization</span>
# MAGIC study = optuna.create_study(sampler=optuna.samplers.TPESampler())
# MAGIC study.optimize(objective, n_trials=50, n_jobs=-1)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Log to MLflow</span>
# MAGIC mlflow.log_params(study.best_params)
# MAGIC mlflow.log_metric(<span style="color:#2E8B57;">&quot;best_value&quot;</span>, study.best_value)
# MAGIC &#8203;
# MAGIC optuna.visualization.plot_optimization_history(study).show()</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Multi-Algorithm or Multi-Metric Optimization
# MAGIC ### Advanced strategies for model selection and evaluation
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> Optuna can optimize <b>several objectives at once</b> (multi-metric) or <b>choose the algorithm inside the objective</b> (multi-model). Multi-metric studies return a Pareto front of trade-off solutions.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 460px;min-width:380px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Multi Metric — joint objectives</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">def</span> objective(trial):
# MAGIC     <span style="color:#6E8B6E;font-style:italic;"># Write your machine learning logic here</span>
# MAGIC     <span style="color:#0B5FA5;">return</span> accuracy, f1_score
# MAGIC &#8203;
# MAGIC study = optuna.create_study(directions=[<span style="color:#2E8B57;">&quot;maximize&quot;</span>, <span style="color:#2E8B57;">&quot;maximize&quot;</span>])
# MAGIC study.optimize(objective, n_trials=&lt;<span style="color:#6E8B6E;font-style:italic;"># of trials&gt;)</span></pre></div><div style="flex:1 1 460px;min-width:380px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Multi Model — algorithm as a choice</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">def</span> objective(trial):
# MAGIC     classifier = trial.suggest_categorical(<span style="color:#2E8B57;">&quot;classifier&quot;</span>, [<span style="color:#2E8B57;">&quot;SVC&quot;</span>, <span style="color:#2E8B57;">&quot;RandomForest&quot;</span>])
# MAGIC     <span style="color:#0B5FA5;">if</span> classifier == <span style="color:#2E8B57;">&quot;SVC&quot;</span>:
# MAGIC         ...  <span style="color:#6E8B6E;font-style:italic;"># Search space, model init, etc.</span>
# MAGIC     <span style="color:#0B5FA5;">else</span>:
# MAGIC         ...  <span style="color:#6E8B6E;font-style:italic;"># Search space, model init, etc.</span>
# MAGIC     <span style="color:#0B5FA5;">return</span> evaluation_score
# MAGIC &#8203;
# MAGIC <span style="color:#0B5FA5;">print</span>(f<span style="color:#2E8B57;">&quot;Best trial metrics: {trial.values}&quot;</span>)
# MAGIC optuna.visualization.plot_pareto_front(study).show()</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Advanced Options in Optuna
# MAGIC ### Streamline your hyperparameter tuning
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> beyond the basic loop, Optuna lets you <b>inspect a study's history</b> through attributes, <b>resume</b> a finished study to search further, and <b>pass extra arguments</b> to an objective by wrapping it.</p>
# MAGIC <div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;margin:2px 0 6px;">Access trials’ history &mdash; study attributes</div>
# MAGIC <table style="border-collapse:collapse;width:100%;margin:6px 0;"><thead><tr><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">Attribute</th><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">Returns</th></tr></thead><tbody><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>best_params</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Parameters of the best trial.</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>best_trial</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">The best trial in the study.</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>best_trials</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Trials on the Pareto front.</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>best_value</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">The best objective value.</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>direction</code> / <code>directions</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">The direction(s) of the study.</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>metric_names</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Metric names.</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>system_attrs</code> / <code>user_attrs</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">System / user attributes.</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>trials</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">All trials in the study.</td></tr></tbody></table>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 380px;min-width:340px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Continue to optimize</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#6E8B6E;font-style:italic;"># Not satisfied? Resume — load_if_exists=True is the default for create_study</span>
# MAGIC study.optimize(objective, n_trials=100)</pre></div><div style="flex:1 1 460px;min-width:380px;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Objective with multiple params</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">def</span> objective(trial, X, y, cv, scoring):
# MAGIC     ...  <span style="color:#6E8B6E;font-style:italic;"># Machine learning logic</span>
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Wrap the objective, generally with a lambda</span>
# MAGIC kf = KFold(n_splits=5, random_state=18)
# MAGIC func = <span style="color:#0B5FA5;">lambda</span> trial: objective(trial, X, y, cv=kf,
# MAGIC                                scoring=<span style="color:#2E8B57;">&quot;neg_mean_squared_log_error&quot;</span>)
# MAGIC study.optimize(func, n_trials=100)</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Ray Tune
# MAGIC ### Ray is a unified framework for scaling AI and Python applications
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> Ray Tune is the hyperparameter-tuning library of the Ray ecosystem. It scales the same tuning code from a laptop to a large cluster, works with any ML framework, and offers state-of-the-art schedulers.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:0;"><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="16" cy="16" r="3.5"/><circle cx="5" cy="6" r="2.4"/><circle cx="27" cy="6" r="2.4"/><circle cx="5" cy="26" r="2.4"/><circle cx="27" cy="26" r="2.4"/><path d="M13.3 14 L7 8 M18.7 14 L25 8 M13.3 18 L7 24 M18.7 18 L25 24"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Scale</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Laptop to cluster</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">A scalable tuning library on Ray &mdash; scale from a single machine to a large cluster <b>without changing code</b>.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="7" width="10" height="8" rx="1.5"/><rect x="18" y="7" width="10" height="8" rx="1.5"/><rect x="4" y="19" width="10" height="8" rx="1.5"/><rect x="18" y="19" width="10" height="8" rx="1.5"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Framework-agnostic</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Any ML library</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Tune PyTorch, XGBoost, TensorFlow/Keras and more with the same API.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M5 26 V5 M5 26 H27"/><path d="M9 24 V16 M15 24 V11 M21 24 V7 M27 24 V13"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">State of the art</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Modern schedulers</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Methods like <b>ASHA, BOHB and Population-Based Training</b> stop weak trials early.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="16" cy="16" r="12"/><path d="M16 9 v7 l5 3"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Tracking</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">TensorBoard &amp; MLflow</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Integrates with TensorBoard and MLflow to track and compare results.</p></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Ray Ecosystem
# MAGIC ### Libraries built on top of Ray Core
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> Ray Core is a general distributed-computing engine, and a family of libraries builds on it &mdash; RLlib, Ray Train, Ray Tune, Ray Serve and custom Python &mdash; each mapping to common workloads.</p>
# MAGIC <div style="overflow-x:auto;max-width:1300px;margin:0 auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1300 620" role="img" style="min-width:1040px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC   <title>Ray ecosystem</title>
# MAGIC   <desc>Ray Core sits at the center of an ecosystem of RLlib (reinforcement learning), Ray Train (deep learning and LLMs), Ray Tune (hyperparameter tuning), Ray Serve (function serving) and Custom Python, each mapping to common use cases.</desc>
# MAGIC   <defs><marker id="re-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#4a5a62" stroke-width="1.8" stroke-linecap="round"/></marker></defs>
# MAGIC   <rect x="330" y="20" width="640" height="470" rx="16" fill="#5A7A88"/>
# MAGIC   <rect x="330" y="20" width="640" height="70" rx="16" fill="#FF5F46"/>
# MAGIC   <text x="650" y="52" text-anchor="middle" font-size="24" font-weight="800" fill="#fff">Ray Ecosystem</text>
# MAGIC   <text x="650" y="78" text-anchor="middle" font-size="14" fill="#fff">Integrates with many third-party OSS tools and libraries built on top of Ray***</text>
# MAGIC   <!-- center + satellites -->
# MAGIC   <g text-anchor="middle">
# MAGIC     <rect x="540" y="180" width="220" height="150" rx="12" fill="#fff"/><text x="650" y="250" font-size="26" font-weight="800" fill="#1c2733">RAY</text><text x="650" y="285" font-size="16" font-family="'SFMono-Regular',monospace" fill="#1c2733">Ray Core</text>
# MAGIC     <rect x="360" y="120" width="150" height="110" rx="12" fill="#fff"/><text x="435" y="165" font-size="15" font-weight="700" fill="#1c2733">RLlib</text><text x="435" y="192" font-size="13" fill="#1c2733">Reinforcement</text><text x="435" y="210" font-size="13" fill="#1c2733">Learning</text>
# MAGIC     <rect x="790" y="120" width="150" height="110" rx="12" fill="#fff"/><text x="865" y="165" font-size="15" font-weight="700" fill="#1c2733">Ray Train</text><text x="865" y="192" font-size="13" fill="#1c2733">Deep Learning</text><text x="865" y="210" font-size="13" fill="#1c2733">LLMs</text>
# MAGIC     <rect x="360" y="300" width="150" height="110" rx="12" fill="#fff"/><text x="435" y="345" font-size="15" font-weight="700" fill="#1c2733">tune</text><text x="435" y="372" font-size="13" fill="#1c2733">Hyperparameter</text><text x="435" y="390" font-size="13" fill="#1c2733">Tuning</text>
# MAGIC     <rect x="565" y="360" width="170" height="90" rx="12" fill="#fff"/><text x="650" y="400" font-size="15" font-weight="700" fill="#1c2733">Ray Serve</text><text x="650" y="424" font-size="13" fill="#1c2733">Function Serving*</text>
# MAGIC     <rect x="790" y="300" width="150" height="110" rx="12" fill="#fff"/><text x="865" y="360" font-size="15" font-weight="700" fill="#1c2733">Custom Python</text>
# MAGIC   </g>
# MAGIC   <g stroke="#4a5a62" stroke-width="2" fill="none">
# MAGIC     <path d="M540 240 L512 210" marker-end="url(#re-ar)"/><path d="M760 240 L788 210" marker-end="url(#re-ar)"/>
# MAGIC     <path d="M560 320 L512 350" marker-end="url(#re-ar)"/><path d="M650 330 L650 356" marker-end="url(#re-ar)"/><path d="M745 320 L788 350" marker-end="url(#re-ar)"/>
# MAGIC   </g>
# MAGIC   <!-- side callouts -->
# MAGIC   <g font-size="13" fill="#1c2733">
# MAGIC     <rect x="20" y="130" width="290" height="90" rx="8" fill="#fff" stroke="#F3B7A9"/><text x="36" y="158">– Autonomous Driving  – Robotics</text><text x="36" y="182">– Gaming  – Manufacturing</text>
# MAGIC     <rect x="20" y="310" width="290" height="90" rx="8" fill="#fff" stroke="#F3B7A9"/><text x="36" y="340">– Traditional ML  – Developing DL</text><text x="36" y="364">– Custom Python optimizations</text>
# MAGIC     <rect x="990" y="130" width="290" height="90" rx="8" fill="#fff" stroke="#F3B7A9"/><text x="1006" y="155">– ML and DL distributed training</text><text x="1006" y="179">– Alternative to Horovod  – GenAI**</text>
# MAGIC     <rect x="990" y="310" width="290" height="90" rx="8" fill="#fff" stroke="#F3B7A9"/><text x="1006" y="340">– Most types of Python apps</text><text x="1006" y="364">– Finance  – HPC-style workloads</text>
# MAGIC   </g>
# MAGIC   <g font-size="13" fill="#4a5a62">
# MAGIC     <text x="650" y="530" text-anchor="middle">*Ray Serve applications are not recommended on Databricks — use Model Serving instead.</text>
# MAGIC     <text x="650" y="558" text-anchor="middle">**For LLM use cases, Mosaic is recommended.</text>
# MAGIC     <text x="650" y="586" text-anchor="middle">***Not all OSS tools built with Ray are supported on Databricks (e.g., tools built for Kubernetes).</text>
# MAGIC   </g>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Ray on Databricks Combines Spark and Ray
# MAGIC ### Spark for data, Ray for compute, MLflow for delivery
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> on Databricks the two engines are complementary &mdash; prepare data with Spark and Delta Lake, run the Ray application, then serve and monitor with MLflow, all governed by Unity Catalog.</p>
# MAGIC <div style="overflow-x:auto;max-width:1300px;margin:0 auto;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1300 560" role="img" style="min-width:1040px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
# MAGIC   <title>Ray on Databricks combines Spark and Ray</title>
# MAGIC   <desc>Delta Lake data preparation with Spark feeds Ray applications, which serve models via MLflow; all governed by Unity Catalog on the Delta Lake data platform.</desc>
# MAGIC   <defs><marker id="rd-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#4a5a62" stroke-width="2" stroke-linecap="round"/></marker></defs>
# MAGIC   <!-- 3 panels -->
# MAGIC   <g>
# MAGIC     <rect x="40" y="20" width="360" height="330" rx="14" fill="#fff" stroke="#C7CDD2"/>
# MAGIC     <text x="220" y="66" text-anchor="middle" font-size="20" font-weight="800" fill="#1c2733">DELTA LAKE</text>
# MAGIC     <rect x="60" y="90" width="320" height="240" rx="10" fill="#0B2027"/>
# MAGIC     <text x="220" y="128" text-anchor="middle" font-size="17" font-weight="700" fill="#fff">Data</text>
# MAGIC     <text x="220" y="168" text-anchor="middle" font-size="14" fill="#dfe6e9">Read data from Delta Lake</text><text x="220" y="188" text-anchor="middle" font-size="14" fill="#dfe6e9">integrations and Unity Catalog</text>
# MAGIC     <text x="220" y="236" text-anchor="middle" font-size="14" fill="#dfe6e9">Prepare data &amp; features</text><text x="220" y="256" text-anchor="middle" font-size="14" fill="#dfe6e9">using Spark.</text>
# MAGIC   </g>
# MAGIC   <g>
# MAGIC     <rect x="470" y="20" width="360" height="330" rx="14" fill="#fff" stroke="#C7CDD2"/>
# MAGIC     <text x="650" y="66" text-anchor="middle" font-size="20" font-weight="800" fill="#1c2733">RAY</text>
# MAGIC     <rect x="490" y="90" width="320" height="240" rx="10" fill="#0B2027"/>
# MAGIC     <text x="650" y="128" text-anchor="middle" font-size="17" font-weight="700" fill="#fff">Ray Apps</text>
# MAGIC     <text x="650" y="168" text-anchor="middle" font-size="14" fill="#dfe6e9">Read Delta Lake table with</text><text x="650" y="188" text-anchor="middle" font-size="14" fill="#dfe6e9">Spark, transfer to Ray.data.</text>
# MAGIC     <text x="650" y="236" text-anchor="middle" font-size="14" fill="#dfe6e9">Run Ray application and save</text><text x="650" y="256" text-anchor="middle" font-size="14" fill="#dfe6e9">outputs or send to Model Serving</text>
# MAGIC   </g>
# MAGIC   <g>
# MAGIC     <rect x="900" y="20" width="360" height="330" rx="14" fill="#fff" stroke="#C7CDD2"/>
# MAGIC     <text x="1080" y="66" text-anchor="middle" font-size="20" font-weight="800" fill="#1c2733">MLflow</text>
# MAGIC     <rect x="920" y="90" width="320" height="240" rx="10" fill="#0B2027"/>
# MAGIC     <text x="1080" y="128" text-anchor="middle" font-size="17" font-weight="700" fill="#fff">Data / Users</text>
# MAGIC     <text x="1080" y="168" text-anchor="middle" font-size="14" fill="#dfe6e9">Serve models into real-time</text><text x="1080" y="188" text-anchor="middle" font-size="14" fill="#dfe6e9">apps and monitor</text>
# MAGIC     <text x="1080" y="236" text-anchor="middle" font-size="14" fill="#dfe6e9">Connect output data with Delta</text><text x="1080" y="256" text-anchor="middle" font-size="14" fill="#dfe6e9">Lake to the entire ecosystem</text>
# MAGIC   </g>
# MAGIC   <path d="M400 175 h64" stroke="#4a5a62" stroke-width="2.4" fill="none" marker-end="url(#rd-ar)"/>
# MAGIC   <path d="M830 175 h64" stroke="#4a5a62" stroke-width="2.4" fill="none" marker-end="url(#rd-ar)"/>
# MAGIC   <!-- governance bar + platform base -->
# MAGIC   <rect x="120" y="380" width="1060" height="56" rx="28" fill="#8FD3A9"/>
# MAGIC   <text x="650" y="416" text-anchor="middle" font-size="22" font-weight="800" fill="#0B2027" font-family="'SFMono-Regular',monospace">Governance — Unity Catalog</text>
# MAGIC   <path d="M60 470 h1180 l-120 70 h-940 z" fill="#EE4D3A"/>
# MAGIC   <text x="650" y="516" text-anchor="middle" font-size="22" font-weight="800" fill="#fff" font-family="'SFMono-Regular',monospace">Data Platform — Delta Lake</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Key Concepts of Ray Tune
# MAGIC ### Six crucial components
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> a Ray Tune job is built from six pieces &mdash; the <b>trainable</b> you optimize, the <b>search space</b>, a <b>search algorithm</b>, a <b>scheduler</b>, the <b>trials</b> that run, and the <b>analysis</b> of results. The next slides walk through each.</p>
# MAGIC <div style="margin:6px 0 4px;"><div><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#F6D2D2;color:#0b2026;font-size:13px;font-weight:600;">Trainables</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#FCE3C8;color:#0b2026;font-size:13px;font-weight:600;">Search Space</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#FBEEBD;color:#0b2026;font-size:13px;font-weight:600;">Search Algorithms</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#D6EBD3;color:#0b2026;font-size:13px;font-weight:600;">Schedulers</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#CBDDF5;color:#0b2026;font-size:13px;font-weight:600;">Trials</span><span style="display:inline-block;margin:4px 6px 4px 0;padding:6px 13px;border-radius:16px;background:#E0D6EF;color:#0b2026;font-size:13px;font-weight:600;">Analyses</span></div></div><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">A complete Ray Tune run</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">import</span> ray
# MAGIC <span style="color:#0B5FA5;">from</span> ray <span style="color:#0B5FA5;">import</span> tune, train
# MAGIC <span style="color:#0B5FA5;">from</span> ray.tune.search.basic_variant <span style="color:#0B5FA5;">import</span> BasicVariantGenerator
# MAGIC <span style="color:#0B5FA5;">from</span> ray.tune.schedulers <span style="color:#0B5FA5;">import</span> ASHAScheduler
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Define the objective function</span>
# MAGIC <span style="color:#0B5FA5;">def</span> train_fn(config):
# MAGIC     score = config[<span style="color:#2E8B57;">&quot;param&quot;</span>] ** 2   <span style="color:#6E8B6E;font-style:italic;"># Example computation</span>
# MAGIC     train.report({<span style="color:#2E8B57;">&quot;score&quot;</span>: score})
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Define the search space</span>
# MAGIC search_space = {<span style="color:#2E8B57;">&quot;param&quot;</span>: tune.uniform(-10, 10)}
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Run the tuning process</span>
# MAGIC tuner = tune.Tuner(
# MAGIC     train_fn,
# MAGIC     param_space=search_space,
# MAGIC     tune_config=tune.TuneConfig(
# MAGIC         metric=<span style="color:#2E8B57;">&quot;score&quot;</span>, mode=<span style="color:#2E8B57;">&quot;min&quot;</span>,
# MAGIC         search_alg=BasicVariantGenerator(),
# MAGIC         scheduler=ASHAScheduler()),
# MAGIC )
# MAGIC results = tuner.fit()
# MAGIC <span style="color:#0B5FA5;">print</span>(<span style="color:#2E8B57;">&quot;Best Config:&quot;</span>, results.get_best_result().config)</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Defining Trainables in Ray Tune
# MAGIC ### The object you pass into a Tune run
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> a <b>trainable</b> is the function or class Ray Tune runs once per trial. It receives a <code>config</code> of hyperparameters, does the work, and reports a metric back &mdash; Ray handles the parallelism, checkpointing and early stopping.</p>
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:16px;margin-top:0;"><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M12 6 C8 6 8 12 5 12 C8 12 8 26 12 26"/><path d="M20 6 C24 6 24 12 27 12 C24 12 24 26 20 26"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Simple &amp; quick</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Function API</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Define a function that calls <code>train.report()</code> &mdash; the common choice for most cases.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="6" y="5" width="20" height="22" rx="2"/><path d="M6 12 h20 M11 5 v22"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">More control</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Class API</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Subclass <code>tune.Trainable</code> when you need more flexibility and lifecycle control.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="14" width="22" height="13" rx="2"/><path d="M16 22 V5 M11 10 l5 -5 l5 5"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Resilience</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Auto checkpointing</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Automatic checkpointing gives state recovery and fault tolerance across distributed actors.</p></div><div style="flex:1 1 250px;min-width:230px;box-sizing:border-box;background:#F9F7F4;border:1px solid rgba(27,81,98,0.28);border-radius:10px;padding:16px 18px 18px;"><div style="display:flex;align-items:flex-start;gap:12px;"><svg viewBox="0 0 32 32" width="34" height="34" aria-hidden="true" style="flex:0 0 34px;display:block;"><g fill="none" stroke="#FF5F46" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="16" cy="16" r="12"/><rect x="12" y="12" width="8" height="8" rx="1"/></g></svg><div style="min-width:0;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#FF5F46;line-height:1.3;">Efficiency</div><div style="font-size:16px;font-weight:700;color:#0b2026;line-height:1.3;margin-top:3px;">Early stopping</div></div></div><p style="margin:12px 0 0;font-size:14.5px;line-height:1.6;color:#33414B;">Use <code>tune.stop()</code> and custom conditions to end unpromising trials early.</p></div></div><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Function trainable (XGBoost)</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;"><span style="color:#0B5FA5;">import</span> xgboost <span style="color:#0B5FA5;">as</span> xgb
# MAGIC &#8203;
# MAGIC <span style="color:#0B5FA5;">def</span> train_function(config):
# MAGIC     context = get_context()   <span style="color:#6E8B6E;font-style:italic;"># Access trial metadata</span>
# MAGIC &#8203;
# MAGIC     <span style="color:#6E8B6E;font-style:italic;"># Define model parameters</span>
# MAGIC     params = {<span style="color:#2E8B57;">&quot;objective&quot;</span>: <span style="color:#2E8B57;">&quot;reg:squarederror&quot;</span>, <span style="color:#2E8B57;">&quot;eval_metric&quot;</span>: <span style="color:#2E8B57;">&quot;rmse&quot;</span>,
# MAGIC               <span style="color:#2E8B57;">&quot;max_depth&quot;</span>: config[<span style="color:#2E8B57;">&quot;max_depth&quot;</span>]}
# MAGIC &#8203;
# MAGIC     <span style="color:#6E8B6E;font-style:italic;"># Train the model</span>
# MAGIC     model = xgb.train(params, dtrain,
# MAGIC                       num_boost_round=config[<span style="color:#2E8B57;">&quot;num_boost_round&quot;</span>])
# MAGIC &#8203;
# MAGIC     <span style="color:#6E8B6E;font-style:italic;"># Report RMSE and save a checkpoint</span>
# MAGIC     tune.report(rmse=float(model.eval(dtrain).split(<span style="color:#2E8B57;">&quot;:&quot;</span>)[-1]))
# MAGIC &#8203;
# MAGIC     <span style="color:#6E8B6E;font-style:italic;"># Log experiment directory and trial ID</span>
# MAGIC     <span style="color:#0B5FA5;">print</span>(f<span style="color:#2E8B57;">&quot;Trial {context.trial_id} running in {context.experiment_dir}&quot;</span>)</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Search Space
# MAGIC ### Defines valid values and selection methods for hyperparameters
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> the <b>search space</b> declares which hyperparameters to explore and how to sample them. Keep it lightweight, and use <code>tune.sample_from</code> for values that depend on others.</p>
# MAGIC <div style="overflow-x:auto;max-width:1050px;margin:0 auto 14px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1040 330" role="img" style="min-width:820px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <title>Ray Tune pipeline</title><desc>Search space and trainables, with search algorithms and schedulers, drive trials, which feed analysis.</desc>
# MAGIC <defs><marker id="tp-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="2" stroke-linecap="round"/></marker></defs>
# MAGIC <path d="M230 78 h36" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 78 C 520 78, 520 165, 556 165" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 173 h76" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 268 C 520 268, 520 185, 556 185" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M770 173 h36" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <rect x="20" y="40" width="210" height="76" rx="10" fill="#FCE3C8" stroke="#1B3139" stroke-width="4"/>
# MAGIC <text x="125" y="74" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Search Space</text>
# MAGIC <text x="125" y="96" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Which Parameters)</text>
# MAGIC <rect x="270" y="40" width="210" height="76" rx="10" fill="#F6D2D2" stroke="#D98A8A" stroke-width="1.6"/>
# MAGIC <text x="375" y="74" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Trainables</text>
# MAGIC <text x="375" y="96" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Which objective to tune)</text>
# MAGIC <rect x="270" y="135" width="210" height="76" rx="10" fill="#FBEEBD" stroke="#E5C860" stroke-width="1.6"/>
# MAGIC <text x="375" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Search Algorithms</text>
# MAGIC <text x="375" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(How to tune)</text>
# MAGIC <rect x="270" y="230" width="210" height="76" rx="10" fill="#D6EBD3" stroke="#8FC08A" stroke-width="1.6"/>
# MAGIC <text x="375" y="264" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Schedulers</text>
# MAGIC <text x="375" y="286" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(When to stop)</text>
# MAGIC <rect x="560" y="135" width="210" height="76" rx="10" fill="#CBDDF5" stroke="#8AAEDD" stroke-width="1.6"/>
# MAGIC <text x="665" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Trials</text>
# MAGIC <text x="665" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Run your experiments)</text>
# MAGIC <rect x="810" y="135" width="210" height="76" rx="10" fill="#E0D6EF" stroke="#B49FD6" stroke-width="1.6"/>
# MAGIC <text x="915" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Analysis</text>
# MAGIC <text x="915" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Analyze results)</text>
# MAGIC </svg></div><ul style="line-height:1.8;font-size:1.0em;color:#33414B;margin:0 0 4px;"><li>Define it with <code>Tuner(param_space=…)</code>.</li><li><b>Avoid large objects</b> in <code>param_space</code> &mdash; use <code>tune.with_parameters()</code> instead.</li><li>Use <code>tune.sample_from(func)</code> for dependent hyperparameters (only HyperOpt/Optuna support conditional spaces).</li></ul><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;">tuner = tune.Tuner(
# MAGIC     trainable,
# MAGIC     param_space={
# MAGIC         <span style="color:#2E8B57;">&quot;param1&quot;</span>: tune.choice([<span style="color:#0B5FA5;">True</span>, <span style="color:#0B5FA5;">False</span>]),
# MAGIC         <span style="color:#2E8B57;">&quot;bar&quot;</span>: tune.uniform(0, 10),
# MAGIC         <span style="color:#2E8B57;">&quot;alpha&quot;</span>: tune.sample_from(<span style="color:#0B5FA5;">lambda</span> _: np.random.uniform(100) ** 2),
# MAGIC         <span style="color:#2E8B57;">&quot;const&quot;</span>: <span style="color:#2E8B57;">&quot;hello&quot;</span>})   <span style="color:#6E8B6E;font-style:italic;"># constant</span></pre></div></div><p style="font-size:14px;color:#618794;margin-top:14px;"><b>Distributions:</b> <code>uniform</code> · <code>quniform</code> · <code>loguniform</code> · <code>randn</code> · <code>randint</code> · <code>lograndint</code> · <code>choice</code> · <code>grid_search</code> · <code>sample_from</code>.</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Search Algorithms
# MAGIC ### Efficient strategies for hyperparameter optimization
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> the <b>search algorithm</b> decides which configurations to try next. Ray Tune defaults to random search, but plugs into Bayesian and gradient-free libraries for smarter, adaptive search.</p>
# MAGIC <div style="overflow-x:auto;max-width:1050px;margin:0 auto 14px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1040 330" role="img" style="min-width:820px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <title>Ray Tune pipeline</title><desc>Search space and trainables, with search algorithms and schedulers, drive trials, which feed analysis.</desc>
# MAGIC <defs><marker id="tp-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="2" stroke-linecap="round"/></marker></defs>
# MAGIC <path d="M230 78 h36" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 78 C 520 78, 520 165, 556 165" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 173 h76" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 268 C 520 268, 520 185, 556 185" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M770 173 h36" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <rect x="20" y="40" width="210" height="76" rx="10" fill="#FCE3C8" stroke="#E8A24D" stroke-width="1.6"/>
# MAGIC <text x="125" y="74" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Search Space</text>
# MAGIC <text x="125" y="96" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Which Parameters)</text>
# MAGIC <rect x="270" y="40" width="210" height="76" rx="10" fill="#F6D2D2" stroke="#D98A8A" stroke-width="1.6"/>
# MAGIC <text x="375" y="74" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Trainables</text>
# MAGIC <text x="375" y="96" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Which objective to tune)</text>
# MAGIC <rect x="270" y="135" width="210" height="76" rx="10" fill="#FBEEBD" stroke="#1B3139" stroke-width="4"/>
# MAGIC <text x="375" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Search Algorithms</text>
# MAGIC <text x="375" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(How to tune)</text>
# MAGIC <rect x="270" y="230" width="210" height="76" rx="10" fill="#D6EBD3" stroke="#8FC08A" stroke-width="1.6"/>
# MAGIC <text x="375" y="264" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Schedulers</text>
# MAGIC <text x="375" y="286" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(When to stop)</text>
# MAGIC <rect x="560" y="135" width="210" height="76" rx="10" fill="#CBDDF5" stroke="#8AAEDD" stroke-width="1.6"/>
# MAGIC <text x="665" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Trials</text>
# MAGIC <text x="665" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Run your experiments)</text>
# MAGIC <rect x="810" y="135" width="210" height="76" rx="10" fill="#E0D6EF" stroke="#B49FD6" stroke-width="1.6"/>
# MAGIC <text x="915" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Analysis</text>
# MAGIC <text x="915" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Analyze results)</text>
# MAGIC </svg></div><ul style="line-height:1.8;font-size:1.0em;color:#33414B;margin:0 0 4px;"><li>If none is specified, Ray Tune <b>uses random search</b>.</li><li>Supports Bayesian (Ax, BOHB, HyperOpt) and gradient-free (Nevergrad) methods.</li><li>Runs in <b>parallel across CPUs/GPUs</b> for faster tuning.</li></ul><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;">search_alg = OptunaSearch()</pre></div></div>
# MAGIC <table style="border-collapse:collapse;width:100%;margin:6px 0;"><thead><tr><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">Search Algorithm</th><th style="text-align:left;padding:8px 11px;background:#1B5162;color:#fff;border:1px solid #C9CDD1;font-size:13.5px;">Summary</th></tr></thead><tbody><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Random / Grid search</td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Random search / grid search (default)</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>AxSearch</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Bayesian / Bandit optimization</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>HyperOptSearch</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Tree-Parzen Estimators</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>BayesOptSearch</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Bayesian optimization</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>TuneBOHB</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Bayesian Opt / HyperBand</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>NevergradSearch</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Gradient-free optimization</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>OptunaSearch</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Optuna search algorithms</td></tr><tr><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;"><code>ConcurrencyLimiter</code></td><td style="padding:7px 11px;border:1px solid #C9CDD1;font-size:13.5px;color:#33414B;vertical-align:top;">Limit concurrent trials</td></tr></tbody></table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Tune Schedulers
# MAGIC ### Stop, pause, or tweak the hyperparameters of running trials
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> a <b>scheduler</b> manages trials while they run &mdash; stopping weak ones early, pausing and resuming to save resources, or mutating hyperparameters &mdash; using a metric to decide what is working.</p>
# MAGIC <div style="overflow-x:auto;max-width:1050px;margin:0 auto 14px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1040 330" role="img" style="min-width:820px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <title>Ray Tune pipeline</title><desc>Search space and trainables, with search algorithms and schedulers, drive trials, which feed analysis.</desc>
# MAGIC <defs><marker id="tp-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="2" stroke-linecap="round"/></marker></defs>
# MAGIC <path d="M230 78 h36" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 78 C 520 78, 520 165, 556 165" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 173 h76" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 268 C 520 268, 520 185, 556 185" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M770 173 h36" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <rect x="20" y="40" width="210" height="76" rx="10" fill="#FCE3C8" stroke="#E8A24D" stroke-width="1.6"/>
# MAGIC <text x="125" y="74" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Search Space</text>
# MAGIC <text x="125" y="96" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Which Parameters)</text>
# MAGIC <rect x="270" y="40" width="210" height="76" rx="10" fill="#F6D2D2" stroke="#D98A8A" stroke-width="1.6"/>
# MAGIC <text x="375" y="74" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Trainables</text>
# MAGIC <text x="375" y="96" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Which objective to tune)</text>
# MAGIC <rect x="270" y="135" width="210" height="76" rx="10" fill="#FBEEBD" stroke="#E5C860" stroke-width="1.6"/>
# MAGIC <text x="375" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Search Algorithms</text>
# MAGIC <text x="375" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(How to tune)</text>
# MAGIC <rect x="270" y="230" width="210" height="76" rx="10" fill="#D6EBD3" stroke="#1B3139" stroke-width="4"/>
# MAGIC <text x="375" y="264" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Schedulers</text>
# MAGIC <text x="375" y="286" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(When to stop)</text>
# MAGIC <rect x="560" y="135" width="210" height="76" rx="10" fill="#CBDDF5" stroke="#8AAEDD" stroke-width="1.6"/>
# MAGIC <text x="665" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Trials</text>
# MAGIC <text x="665" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Run your experiments)</text>
# MAGIC <rect x="810" y="135" width="210" height="76" rx="10" fill="#E0D6EF" stroke="#B49FD6" stroke-width="1.6"/>
# MAGIC <text x="915" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Analysis</text>
# MAGIC <text x="915" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Analyze results)</text>
# MAGIC </svg></div><ul style="line-height:1.8;font-size:1.0em;color:#33414B;margin:0 0 4px;"><li><b>Trial schedulers can</b> stop underperformers early, pause &amp; resume, and clone/tweak trials.</li><li>They use a <b>metric</b> (e.g. <code>"loss"</code>) with <code>mode="min"</code> or <code>"max"</code>.</li><li><b>Default:</b> a FIFO scheduler that runs trials in order and does <b>not</b> early-stop.</li></ul><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;">tuner = tune.Tuner(
# MAGIC     train_fn,
# MAGIC     tune_config=tune.TuneConfig(
# MAGIC         scheduler=ASHAScheduler(),
# MAGIC         metric=<span style="color:#2E8B57;">&quot;loss&quot;</span>, mode=<span style="color:#2E8B57;">&quot;min&quot;</span>,
# MAGIC         num_samples=10),
# MAGIC     param_space=config)</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Trials
# MAGIC ### Each trial is one experiment run
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> a <b>trial</b> is a single run for one hyperparameter combination. <code>Tuner.fit()</code> launches and manages all trials and returns a <code>ResultGrid</code> once they finish.</p>
# MAGIC <div style="overflow-x:auto;max-width:1050px;margin:0 auto 14px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1040 330" role="img" style="min-width:820px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <title>Ray Tune pipeline</title><desc>Search space and trainables, with search algorithms and schedulers, drive trials, which feed analysis.</desc>
# MAGIC <defs><marker id="tp-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="2" stroke-linecap="round"/></marker></defs>
# MAGIC <path d="M230 78 h36" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 78 C 520 78, 520 165, 556 165" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 173 h76" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 268 C 520 268, 520 185, 556 185" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M770 173 h36" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <rect x="20" y="40" width="210" height="76" rx="10" fill="#FCE3C8" stroke="#E8A24D" stroke-width="1.6"/>
# MAGIC <text x="125" y="74" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Search Space</text>
# MAGIC <text x="125" y="96" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Which Parameters)</text>
# MAGIC <rect x="270" y="40" width="210" height="76" rx="10" fill="#F6D2D2" stroke="#D98A8A" stroke-width="1.6"/>
# MAGIC <text x="375" y="74" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Trainables</text>
# MAGIC <text x="375" y="96" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Which objective to tune)</text>
# MAGIC <rect x="270" y="135" width="210" height="76" rx="10" fill="#FBEEBD" stroke="#E5C860" stroke-width="1.6"/>
# MAGIC <text x="375" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Search Algorithms</text>
# MAGIC <text x="375" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(How to tune)</text>
# MAGIC <rect x="270" y="230" width="210" height="76" rx="10" fill="#D6EBD3" stroke="#8FC08A" stroke-width="1.6"/>
# MAGIC <text x="375" y="264" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Schedulers</text>
# MAGIC <text x="375" y="286" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(When to stop)</text>
# MAGIC <rect x="560" y="135" width="210" height="76" rx="10" fill="#CBDDF5" stroke="#1B3139" stroke-width="4"/>
# MAGIC <text x="665" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Trials</text>
# MAGIC <text x="665" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Run your experiments)</text>
# MAGIC <rect x="810" y="135" width="210" height="76" rx="10" fill="#E0D6EF" stroke="#B49FD6" stroke-width="1.6"/>
# MAGIC <text x="915" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Analysis</text>
# MAGIC <text x="915" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Analyze results)</text>
# MAGIC </svg></div><div style="display:flex;flex-wrap:wrap;gap:22px;"><div style="flex:1 1 300px;min-width:280px;"><b>How trials are managed</b><ul style="line-height:1.7;margin:6px 0 0;"><li><code>Tuner.fit()</code> runs and manages the tuning.</li><li>Requires a <b>trainable</b> and a <b>param_space</b>.</li><li>Optionally takes tune, runtime, checkpoint and failure configs.</li></ul></div><div style="flex:1 1 300px;min-width:280px;"><b>Default behavior</b><ul style="line-height:1.7;margin:6px 0 0;"><li>Runs until all trials complete or an error occurs.</li><li>Returns a <b>ResultGrid</b> to inspect and analyze.</li></ul></div></div><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Example</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;">space = {<span style="color:#2E8B57;">&quot;a&quot;</span>: tune.uniform(0, 1), <span style="color:#2E8B57;">&quot;b&quot;</span>: tune.uniform(0, 1)}
# MAGIC tuner = tune.Tuner(
# MAGIC     trainable, param_space=space,
# MAGIC     tune_config=tune.TuneConfig(num_samples=10))
# MAGIC tuner.fit()</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Analysis
# MAGIC ### Tuner.fit() returns a ResultGrid
# MAGIC
# MAGIC <div style="max-width:1180px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <p style="font-size:1.02em;line-height:1.65;color:#33414B;margin:0 0 18px;"><b>Purpose:</b> after tuning, the <b>ResultGrid</b> holds everything &mdash; per-trial logs, metrics and checkpoints, plus experiment-level insight. Pull out the best result, export a DataFrame, or plot a metric over time.</p>
# MAGIC <div style="overflow-x:auto;max-width:1050px;margin:0 auto 14px;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 1040 330" role="img" style="min-width:820px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
# MAGIC <title>Ray Tune pipeline</title><desc>Search space and trainables, with search algorithms and schedulers, drive trials, which feed analysis.</desc>
# MAGIC <defs><marker id="tp-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="2" stroke-linecap="round"/></marker></defs>
# MAGIC <path d="M230 78 h36" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 78 C 520 78, 520 165, 556 165" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 173 h76" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M480 268 C 520 268, 520 185, 556 185" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <path d="M770 173 h36" stroke="#1B3139" stroke-width="2.2" fill="none" marker-end="url(#tp-ar)"/>
# MAGIC <rect x="20" y="40" width="210" height="76" rx="10" fill="#FCE3C8" stroke="#E8A24D" stroke-width="1.6"/>
# MAGIC <text x="125" y="74" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Search Space</text>
# MAGIC <text x="125" y="96" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Which Parameters)</text>
# MAGIC <rect x="270" y="40" width="210" height="76" rx="10" fill="#F6D2D2" stroke="#D98A8A" stroke-width="1.6"/>
# MAGIC <text x="375" y="74" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Trainables</text>
# MAGIC <text x="375" y="96" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Which objective to tune)</text>
# MAGIC <rect x="270" y="135" width="210" height="76" rx="10" fill="#FBEEBD" stroke="#E5C860" stroke-width="1.6"/>
# MAGIC <text x="375" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Search Algorithms</text>
# MAGIC <text x="375" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(How to tune)</text>
# MAGIC <rect x="270" y="230" width="210" height="76" rx="10" fill="#D6EBD3" stroke="#8FC08A" stroke-width="1.6"/>
# MAGIC <text x="375" y="264" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Schedulers</text>
# MAGIC <text x="375" y="286" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(When to stop)</text>
# MAGIC <rect x="560" y="135" width="210" height="76" rx="10" fill="#CBDDF5" stroke="#8AAEDD" stroke-width="1.6"/>
# MAGIC <text x="665" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Trials</text>
# MAGIC <text x="665" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Run your experiments)</text>
# MAGIC <rect x="810" y="135" width="210" height="76" rx="10" fill="#E0D6EF" stroke="#1B3139" stroke-width="4"/>
# MAGIC <text x="915" y="169" text-anchor="middle" font-size="17" font-weight="800" fill="#1B3139">Analysis</text>
# MAGIC <text x="915" y="191" text-anchor="middle" font-size="12.5" font-style="italic" fill="#4a5a62">(Analyze results)</text>
# MAGIC </svg></div><div style="display:flex;flex-wrap:wrap;gap:16px;align-items:stretch;margin-top:18px;"><div style="flex:1 1 100%;min-width:100%;background:#F4F5F6;border:1px solid #C9CDD1;border-radius:4px;padding:14px 16px;overflow-x:auto;"><div style="font-size:11px;font-weight:700;letter-spacing:0.7px;text-transform:uppercase;color:#618794;margin-bottom:8px;">Working with the ResultGrid</div><pre style="margin:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;color:#1C2733;white-space:pre;">results = tuner.fit()
# MAGIC &#8203;
# MAGIC best_result = results.get_best_result()      <span style="color:#6E8B6E;font-style:italic;"># Best result object</span>
# MAGIC best_config = best_result.config             <span style="color:#6E8B6E;font-style:italic;"># Best trial's hyperparameters</span>
# MAGIC best_logdir = best_result.path               <span style="color:#6E8B6E;font-style:italic;"># Best trial's directory</span>
# MAGIC best_checkpoint = best_result.checkpoint     <span style="color:#6E8B6E;font-style:italic;"># Best trial's checkpoint</span>
# MAGIC best_metrics = best_result.metrics           <span style="color:#6E8B6E;font-style:italic;"># Best trial's last results</span>
# MAGIC best_result_df = best_result.metrics_dataframe   <span style="color:#6E8B6E;font-style:italic;"># As a pandas DataFrame</span>
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># A DataFrame of results filtered by metric / mode</span>
# MAGIC df = results.get_dataframe(filter_metric=<span style="color:#2E8B57;">&quot;score&quot;</span>, filter_mode=<span style="color:#2E8B57;">&quot;max&quot;</span>)
# MAGIC &#8203;
# MAGIC <span style="color:#6E8B6E;font-style:italic;"># Plot a metric over training iterations for the best trial</span>
# MAGIC best_result.metrics_dataframe.plot(<span style="color:#2E8B57;">&quot;training_iteration&quot;</span>, <span style="color:#2E8B57;">&quot;mean_accuracy&quot;</span>)</pre></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>