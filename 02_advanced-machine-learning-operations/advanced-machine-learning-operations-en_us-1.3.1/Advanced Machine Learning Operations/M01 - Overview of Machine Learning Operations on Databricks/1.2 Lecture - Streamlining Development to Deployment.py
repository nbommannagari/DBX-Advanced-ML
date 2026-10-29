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
# MAGIC # Lecture - Streamlining Development to Deployment
# MAGIC
# MAGIC Great MLOps starts with sound architecture. This lecture introduces the guiding principles for organizing ML environments and moving work from development to production. You will compare direct and indirect environment separation, understand why isolated development, staging, and production workspaces are recommended, and explore why teams promote code rather than models between environments — along with the deploy-model, deploy-code, and hybrid patterns and their trade-offs.
# MAGIC
# MAGIC <br>
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this lecture, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Understand guiding principles of machine learning architectures.</li>
# MAGIC         <li>Understand the difference between direct and indirect environment separation.</li>
# MAGIC         <li>Understand why direct separation is recommended.</li>
# MAGIC         <li>Understand why deploying code instead of models is recommended.</li>
# MAGIC         <li>Understand guiding principles of machine learning operations.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## A. ML Architecture Guiding Principles
# MAGIC
# MAGIC ### Optimizing Development and Deployment
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:60%">
# MAGIC     <col style="width:40%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC         <ul>
# MAGIC         <li><b>Environment Setup</b></li>
# MAGIC             <ul> <li>Create separate environments for different stages.<br>Example: Development, Staging, Production.</li>
# MAGIC             <ul> <li>Other configurations can also be used to meet the specific needs of your organization.</li> </ul></ul> 
# MAGIC         <div style="height:12px;"></div>
# MAGIC         <li><b>Deploy code, not models</b></li>
# MAGIC             <ul><li>In most situations, promote code, not models, between environments.</li>
# MAGIC             <li>Production version of the model is trained using production code.</li> </ul>
# MAGIC         <div style="height:12px;"></div>
# MAGIC         <li><b>Versioning</b></li>
# MAGIC              <ul><li>Store pipelines and code in Git, with branch strategies reflecting environment transitions.</li>
# MAGIC             <li>Move ML logic between branches to parallel environment progression.</li> </ul>
# MAGIC         <div style="height:12px;"></div>
# MAGIC         <li><b>Access Control</b></li>
# MAGIC              <ul><li>Centralize management and unify governance of data, features, model development and model lifecycle.</li></ul>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 0px;">        
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M1L2_17.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## B. Environment Separation
# MAGIC
# MAGIC ### How many Databricks workspaces do we have?
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th style="font-size: 18px; text-align: center; border-right: 4px solid #ff5f46;">Direct Separation (Recommended)</th>
# MAGIC     <th style="font-size: 18px; text-align: center;">Indirect Separation</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="text-align: center; border-right: 4px solid #ff5f46;">
# MAGIC <div style="max-width: 480px; margin: 0 auto;">
# MAGIC <svg width="100%" viewBox="0 0 480 240" role="img" style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
# MAGIC   <title>Direct environment separation</title>
# MAGIC   <desc>Development, staging, and production each sit in their own isolated workspace, shown as three separately bordered groups linked left to right.</desc>
# MAGIC   <defs>
# MAGIC     <marker id="es-d-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-linecap="round"/></marker>
# MAGIC   </defs>
# MAGIC   <!-- per-box dashed groups (separate) -->
# MAGIC   <rect x="31" y="108" width="122" height="112" rx="8" fill="none" stroke="#6B7B83" stroke-width="1.6" stroke-dasharray="6 4"/>
# MAGIC   <rect x="181" y="108" width="122" height="112" rx="8" fill="none" stroke="#6B7B83" stroke-width="1.6" stroke-dasharray="6 4"/>
# MAGIC   <rect x="331" y="108" width="122" height="112" rx="8" fill="none" stroke="#6B7B83" stroke-width="1.6" stroke-dasharray="6 4"/>
# MAGIC   <!-- connector: dev -> staging -> prod -->
# MAGIC   <path d="M92 108 V84 H392" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-dasharray="6 4"/>
# MAGIC   <path d="M242 84 V104" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-dasharray="6 4" marker-end="url(#es-d-ar)"/>
# MAGIC   <path d="M392 84 V104" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-dasharray="6 4" marker-end="url(#es-d-ar)"/>
# MAGIC   <!-- dev -->
# MAGIC   <rect x="40" y="120" width="104" height="90" rx="4" fill="#93A7B3"/>
# MAGIC   <text x="92" y="152" text-anchor="middle" font-size="12" font-weight="700" fill="#22323A" letter-spacing="0.5">DEVELOPED</text>
# MAGIC   <text x="92" y="192" text-anchor="middle" font-size="13.5" font-family="'SFMono-Regular',Consolas,monospace" fill="#22323A">development</text>
# MAGIC   <!-- staging -->
# MAGIC   <rect x="190" y="120" width="104" height="90" rx="4" fill="#F3CE5B"/>
# MAGIC   <text x="242" y="152" text-anchor="middle" font-size="12" font-weight="700" fill="#5A4A15" letter-spacing="0.5">TESTED</text>
# MAGIC   <text x="242" y="192" text-anchor="middle" font-size="13.5" font-family="'SFMono-Regular',Consolas,monospace" fill="#5A4A15">staging</text>
# MAGIC   <!-- prod -->
# MAGIC   <rect x="340" y="120" width="104" height="90" rx="4" fill="#9BCBB0"/>
# MAGIC   <text x="392" y="152" text-anchor="middle" font-size="12" font-weight="700" fill="#24543A" letter-spacing="0.5">DEPLOYED</text>
# MAGIC   <text x="392" y="192" text-anchor="middle" font-size="13.5" font-family="'SFMono-Regular',Consolas,monospace" fill="#24543A">production</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC     </td>
# MAGIC     <td style="text-align: center;">
# MAGIC <div style="max-width: 480px; margin: 0 auto;">
# MAGIC <svg width="100%" viewBox="0 0 480 240" role="img" style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
# MAGIC   <title>Indirect environment separation</title>
# MAGIC   <desc>Development, staging, and production share a single workspace, shown as three boxes inside one bordered group fed from a common source.</desc>
# MAGIC   <defs>
# MAGIC     <marker id="es-i-ar" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-linecap="round"/></marker>
# MAGIC   </defs>
# MAGIC   <!-- single dashed group around all three -->
# MAGIC   <rect x="28" y="108" width="424" height="112" rx="10" fill="none" stroke="#1B3139" stroke-width="1.8" stroke-dasharray="6 4"/>
# MAGIC   <!-- common feed line with 3 down-arrows -->
# MAGIC   <path d="M92 84 H392" fill="none" stroke="#1B3139" stroke-width="1.8"/>
# MAGIC   <path d="M242 72 V84" fill="none" stroke="#1B3139" stroke-width="1.8"/>
# MAGIC   <path d="M92 84 V104"  fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#es-i-ar)"/>
# MAGIC   <path d="M242 84 V104" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#es-i-ar)"/>
# MAGIC   <path d="M392 84 V104" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#es-i-ar)"/>
# MAGIC   <!-- dev -->
# MAGIC   <rect x="40" y="120" width="104" height="90" rx="4" fill="#93A7B3"/>
# MAGIC   <text x="92" y="152" text-anchor="middle" font-size="12" font-weight="700" fill="#22323A" letter-spacing="0.5">DEVELOPED</text>
# MAGIC   <text x="92" y="192" text-anchor="middle" font-size="13.5" font-family="'SFMono-Regular',Consolas,monospace" fill="#22323A">development</text>
# MAGIC   <!-- staging -->
# MAGIC   <rect x="190" y="120" width="104" height="90" rx="4" fill="#F3CE5B"/>
# MAGIC   <text x="242" y="152" text-anchor="middle" font-size="12" font-weight="700" fill="#5A4A15" letter-spacing="0.5">TESTED</text>
# MAGIC   <text x="242" y="192" text-anchor="middle" font-size="13.5" font-family="'SFMono-Regular',Consolas,monospace" fill="#5A4A15">staging</text>
# MAGIC   <!-- prod -->
# MAGIC   <rect x="340" y="120" width="104" height="90" rx="4" fill="#9BCBB0"/>
# MAGIC   <text x="392" y="152" text-anchor="middle" font-size="12" font-weight="700" fill="#24543A" letter-spacing="0.5">DEPLOYED</text>
# MAGIC   <text x="392" y="192" text-anchor="middle" font-size="13.5" font-family="'SFMono-Regular',Consolas,monospace" fill="#24543A">production</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="border-right: 4px solid #ff5f46; font-size: 14px;">
# MAGIC         <ul>
# MAGIC         <li><b>Completely separate</b> Databricks workspaces for each environment</li>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li><b>Simpler environments</b></li>
# MAGIC             <ul><li>Easier to manage <b>security</b> and <b>compliance</b></li>
# MAGIC             <li>More straightforward to <b>track</b> changes and issues</li></ul>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li><b>Reduces the risk</b> of cross-environment interactions</li>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li><b>Scales</b> well to multiple projects</li>
# MAGIC             <ul><li>Might require more <b>administrative effort</b> to manage multiple workspaces</li>
# MAGIC             <li>Might lead to <b>higher costs</b> due to the need for separate resources for each workspace</li></ul>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC     <td style="font-size: 14px;">
# MAGIC         <ul>
# MAGIC         <li><b>One Databricks workspace</b> with enforced separation using <b>naming conventions</b> and <b>access control</b></li>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li><b>Simpler</b> overall infrastructure</li>
# MAGIC             <ul><li><b>Less permissions</b> required</li>
# MAGIC             <li>Can be more <b>cost-effective</b> needing fewer resources</li></ul>
# MAGIC             <div style="height:8px;"></div>
# MAGIC         <li>Easier cross-environment <b>collaboration</b> and sharing</li>
# MAGIC         <div style="height:8px;"></div>
# MAGIC         <li><b>Doesn’t scale well</b> to multiple projects</li>
# MAGIC             <ul><li><b>Complex</b> individual environment</li>
# MAGIC             <li>Requires management to <b>maintain</b> the separation and prevent accidental cross-environment interactions</li>
# MAGIC             <li>Can be more challenging to manage <b>security</b> and <b>compliance</b> requirements</li></ul>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## C. Deployment Patterns
# MAGIC
# MAGIC ### Moving from Deploy Model to Deploy Code
# MAGIC
# MAGIC <br>
# MAGIC <table style="width: 100% !important; max-width: 100% !important; border-collapse: collapse; table-layout: auto; margin: 0; padding: 0; border: 0px solid #ddd;">
# MAGIC   <tr>
# MAGIC     <td> </td>
# MAGIC     <td style="text-align: center;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M1L2_19_1.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC     <td style="text-align: center;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M1L2_19_2.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC     <td style="text-align: center;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M1L2_19_3.png" alt="Image description" style="max-width: 100%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC     <tr style="background-color: #e0e0e0;">
# MAGIC       <th style="padding: 12px; text-align: center; border: 1px solid #ddd;"><h4 style="margin: 0;"> </h4></th>
# MAGIC       <th style="padding: 12px; text-align: center; border: 1px solid #ddd;"><h4 style="margin: 0;">Deploy models</h4></th>
# MAGIC       <th style="padding: 12px; text-align: center; border: 1px solid #ddd;"><h4 style="margin: 0;">Deploy code</h4></th>
# MAGIC       <th style="padding: 12px; text-align: center; border: 1px solid #ddd;"><h4 style="margin: 0;">Hybrid</h4></th>
# MAGIC     </tr>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; text-align: left; vertical-align: top; border: 1px solid #ddd; background-color: #f5f5f5; white-space: nowrap;">
# MAGIC       <b>Process</b>
# MAGIC       </td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd;">
# MAGIC         <ol>
# MAGIC         <li>Models are trained in dev and promoted to staging</li>
# MAGIC         <li>Models are tested in staging</li>
# MAGIC         <li>Models are promoted to prod</li>
# MAGIC         </ol>
# MAGIC       </td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd;">
# MAGIC         <ol>
# MAGIC           <li>Code is promoted from dev to staging</li>
# MAGIC           <li>Models are retrained and tested in staging</li>
# MAGIC           <li>Code is promoted to prod</li>
# MAGIC           <li>Models are retrained in prod</li>
# MAGIC         </ol>
# MAGIC       </td>
# MAGIC       <td style="padding: 12px; vertical-align: top; border: 1px solid #ddd;">
# MAGIC         <ol>
# MAGIC         <li>Code is promoted from dev to staging</li>
# MAGIC         <li>Models are retrained and tested in staging</li>
# MAGIC         <li>Models are promoted to prod</li>
# MAGIC         </ol>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; text-align: left; vertical-align: top; border: 1px solid #ddd; background-color: #f5f5f5; white-space: nowrap;"><b>Trade-offs</b></td>
# MAGIC       <td style="padding: 20px; vertical-align: top; border: 1px solid #ddd;">
# MAGIC         <span style="color: green;">&#8593;</span> Simplicity<br>
# MAGIC         <span style="color: green;">&#8593;</span> DS familiarity<br>
# MAGIC         <span style="color: green;">&#8593;</span> Computational cost<br><br>
# MAGIC         <span style="color: red;">&#8595;</span> Automation<br>
# MAGIC         <span style="color: red;">&#8595;</span> Scalability<br>
# MAGIC         <span style="color: red;">&#8595;</span> Reproducibility<br>
# MAGIC       </td>
# MAGIC       <td style="padding: 20px; vertical-align: top; border: 1px solid #ddd;">
# MAGIC         <span style="color: green;">&#8593;</span> Only prod env needs prod data<br>
# MAGIC         <span style="color: green;">&#8593;</span> Automation<br>
# MAGIC         <span style="color: green;">&#8593;</span> Reproducibility<br>
# MAGIC         <span style="color: green;">&#8593;</span> Scalability<br>
# MAGIC         <span style="color: green;">&#8593;</span> Testing<br><br>
# MAGIC         <span style="color: red;">&#8595;</span> Engineering setup & maintenance<br>
# MAGIC         <span style="color: red;">&#8595;</span> Familiarity<br>
# MAGIC       </td>
# MAGIC       <td style="padding: 20px; vertical-align: top; border: 1px solid #ddd;">
# MAGIC         <span style="color: green;">&#8593;</span> Scalability<br>
# MAGIC         <span style="color: green;">&#8593;</span> Automation<br>
# MAGIC         <span style="color: green;">&#8593;</span> Reproducibility<br>
# MAGIC         <span style="color: green;">&#8593;</span> ML Eng familiarity<br>
# MAGIC         <span style="color: green;">&#8593;</span> Computational cost<br><br>
# MAGIC         <span style="color: red;">&#8595;</span> Simplicity<br>
# MAGIC         <span style="color: red;">&#8595;</span> Familiarity<br>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## D. Deeper dive into benefits of “Deploy Code”
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:20%">
# MAGIC     <col style="width:80%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="text-align: right; vertical-align: middle;"> <h1>Recommended: </h1></td>
# MAGIC     <td style="text-align: left;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M1L2_20.png" alt="Image description" style="max-width: 80%; height: auto; border-radius: 0px;">
# MAGIC     </td>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; text-align: left; vertical-align: top; border: 1px solid #ddd; background-color: #f5f5f5; white-space: nowrap;"><b>Automation</b></td>
# MAGIC       <td style="padding: 10px; vertical-align: top; border: 1px solid #ddd;">
# MAGIC         <span style="color: green;">&#8593;</span> Supports automated retraining in secure and controlled environments.
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; text-align: left; vertical-align: top; border: 1px solid #ddd; background-color: #f5f5f5; white-space: nowrap;"><b>Data access control</b></td>
# MAGIC       <td style="padding: 10px; vertical-align: top; border: 1px solid #ddd;">
# MAGIC         <span style="color: green;">&#8593;</span> Restricts data access efficiently, where only the production environment requires read permissions to sensitive training data.
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; text-align: left; vertical-align: top; border: 1px solid #ddd; background-color: #f5f5f5; white-space: nowrap;"><b>Reproducible models</b></td>
# MAGIC       <td style="padding: 10px; vertical-align: top; border: 1px solid #ddd;">
# MAGIC         <span style="color: green;">&#8593;</span> Every step of the model generation process can be replicated consistently across environments, allowing teams to recreate models consistently.
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; text-align: left; vertical-align: top; border: 1px solid #ddd; background-color: #f5f5f5; white-space: nowrap;"><b>Support for large projects</b></td>
# MAGIC       <td style="padding: 10px; vertical-align: top; border: 1px solid #ddd;">
# MAGIC         <span style="color: green;">&#8593;</span> Scales well for larger projects by promoting modularized, organized, reusable code and structured testing workflows.
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; text-align: left; vertical-align: top; border: 1px solid #ddd; background-color: #f5f5f5; white-space: nowrap;"><b>Testing</b></td>
# MAGIC       <td style="padding: 10px; vertical-align: top; border: 1px solid #ddd;">
# MAGIC         <span style="color: green;">&#8593;</span> First unit and integration testing in the development environment followed by unit and integration testing in staging, which closely mimics production.
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; text-align: left; vertical-align: top; border: 1px solid #ddd; background-color: #f5f5f5; white-space: nowrap;"><b>Data science familiarity</b></td>
# MAGIC       <td style="padding: 10px; vertical-align: top; border: 1px solid #ddd;">
# MAGIC         <span style="color: red;">&#8595;</span> Requires the data science team to write production-ready, modular code. ensuring smooth hand-offs to engineering teams.
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 12px; text-align: left; vertical-align: top; border: 1px solid #ddd; background-color: #f5f5f5; white-space: nowrap;"><b>Eng setup & maintenance</b></td>
# MAGIC       <td style="padding: 10px; vertical-align: top; border: 1px solid #ddd;">
# MAGIC         <span style="color: red;">&#8595;</span> Requires robust CI/CD infrastructure, supporting consistent unit and integration testing for all models.
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## E. Dimensions of Architecture
# MAGIC
# MAGIC ### Differentiating initial organization/setup and ongoing workflows
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <th style="font-size: 18px; text-align: center; border-right: 2px solid #ccc;">Infrastructure </th>
# MAGIC     <th style="font-size: 18px; text-align: center;">Workflow </th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="border-right: 2px solid #ccc; font-size: 16px;">
# MAGIC         The organization, governance, and setup of environments, data, compute and other resources.
# MAGIC         <ul>
# MAGIC         <li>Set up one time (per project or per team/organization)</li>
# MAGIC         <li>Crucial to downstream success of project(s)</li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC     <td style="font-size: 16px;">
# MAGIC         The processes that ML practitioners follow within a defined architecture to achieve success on an ML project.
# MAGIC         <ul>
# MAGIC         <li>Repeatable, fluid processes specific to a project</li>
# MAGIC         <li>Aligned to organizational best practices</li>
# MAGIC         </ul>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## F. Machine Learning Architecture
# MAGIC
# MAGIC **Involves designing and implementing an end-to-end pipeline involving the tasks and tools to solve specific business problems using machine learning.**
# MAGIC
# MAGIC <br>
# MAGIC <div style="text-align: center; margin-top: 0px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/advanced-machine-learning-operations-en_us-1.3.1/images/20261023T141558Z/Advanced Machine Learning Operations/Includes/images/M1L2_22.png" alt="Image description" style="max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## G. ML Operations Guiding Principles
# MAGIC
# MAGIC ### Optimizing Development and Deployment
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC - Automated and Orchestrated **Continuous Integration and Continuous Deployment** 
# MAGIC     - Set up CI/CD pipelines to automate the machine learning lifecycle.
# MAGIC     - Reducing manual steps and ensuring consistency.
# MAGIC     - Break down ML workflows into manageable modules.
# MAGIC
# MAGIC - Automate Comprehensive **Testing**
# MAGIC     - Implement a testing strategy including: unit, integration, and end-to-end tests.
# MAGIC     - Incorporate canary tests and blue/green deployment with CI/CD Pipelines.
# MAGIC
# MAGIC - **Monitoring** and **Alerts**
# MAGIC     - Monitor model performance and operations with visuals, custom metrics, and insights.
# MAGIC     - Set up automated alerts to detect issues early.
# MAGIC
# MAGIC - Build ML **Assets as Code**
# MAGIC     - Standardize and automate ML asset creation using Infrastructure as Code and predefined templates.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>