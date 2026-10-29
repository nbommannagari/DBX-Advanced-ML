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
# MAGIC # Lecture - Automate Comprehensive Testing
# MAGIC
# MAGIC Reliable models depend on rigorous testing. This lecture presents a structured approach to testing ML code and pipelines using the testing pyramid of unit, integration, and end-to-end tests. You will learn how to organize functions and tests, keep test code separate from notebooks with %run, choose frameworks across Python, Scala, R, and SQL, and decide what to test in an ML system while keeping testing efficient within a CI/CD process.
# MAGIC
# MAGIC <br>
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <span style="font-size: 24px;">🎯</span>
# MAGIC     <div>
# MAGIC       <strong style="color: #ff8f00; font-size: 1.1em;">Learning Objectives</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">By the end of this lecture, you will be able to:</p>
# MAGIC       <ul style="margin: 8px 0 0 20px; color: #333;">
# MAGIC         <li>Explain the structured approach to testing and best practices.</li>
# MAGIC         <li>Describe how Databricks allows for keeping test code separate from your notebook.</li>
# MAGIC         <li>Understand common testing frameworks for various languages, e.g. unittest.</li>
# MAGIC         <li>Interpret Databricks for end-to-end testing and online evaluation.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## A. Testing Pyramid
# MAGIC
# MAGIC **Illustrating how the test effort should be optimized towards simpler tests, reserving more complex/expensive tests for edge cases**
# MAGIC
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
# MAGIC         <li><b>Unit tests</b></li>
# MAGIC             <ul><li>Test only one specific function, on small amount of data</li>
# MAGIC             <li>Fast, able to run locally</li></ul>
# MAGIC             <div style="height:8px;"></div>
# MAGIC         <li><b>Integration tests</b></li>
# MAGIC             <ul><li>Test business logic on limited amount of data</li>
# MAGIC             <li>Usually run in a separate environment, as part of CI implementation</li></ul>
# MAGIC             <div style="height:8px;"></div>
# MAGIC         <li><b>Acceptance/End-to-End tests</b></li>
# MAGIC             <ul><li>Run on data, close to production (volume/velocity/…)</li>
# MAGIC             <li>Run in environment, close to production</li></ul>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 0px;">        
# MAGIC         <h3 style="text-align:center;">Testing Pyramid...</h3>
# MAGIC         <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1218 748" font-family="Helvetica, Arial, sans-serif">
# MAGIC         <defs>
# MAGIC             <!-- Left arrow: white (top) -> red (bottom) -->
# MAGIC             <linearGradient id="gradDown" x1="0" y1="0" x2="0" y2="1">
# MAGIC             <stop offset="0%" stop-color="#ffffff"/>
# MAGIC             <stop offset="35%" stop-color="#f6d6d4"/>
# MAGIC             <stop offset="70%" stop-color="#ef8a84"/>
# MAGIC             <stop offset="100%" stop-color="#e5302a"/>
# MAGIC             </linearGradient>
# MAGIC             <!-- Right arrow: red (top) -> white (bottom) -->
# MAGIC             <linearGradient id="gradUp" x1="0" y1="0" x2="0" y2="1">
# MAGIC             <stop offset="0%" stop-color="#e5302a"/>
# MAGIC             <stop offset="30%" stop-color="#ef8a84"/>
# MAGIC             <stop offset="65%" stop-color="#f6d6d4"/>
# MAGIC             <stop offset="100%" stop-color="#ffffff"/>
# MAGIC             </linearGradient>
# MAGIC             <!-- Box fill: light blue (left) -> white (right) -->
# MAGIC             <linearGradient id="gradBox" x1="0" y1="0" x2="1" y2="0">
# MAGIC             <stop offset="0%" stop-color="#d7e2f0"/>
# MAGIC             <stop offset="45%" stop-color="#eef3fa"/>
# MAGIC             <stop offset="100%" stop-color="#ffffff"/>
# MAGIC             </linearGradient>
# MAGIC             <filter id="softShadow" x="-20%" y="-20%" width="140%" height="140%">
# MAGIC             <feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#7a94b5" flood-opacity="0.45"/>
# MAGIC             </filter>
# MAGIC         </defs>
# MAGIC         <!-- Blue pyramid -->
# MAGIC         <polygon points="545,42 918,690 195,690" fill="#5f82ab"/>
# MAGIC         <!-- Stacked boxes -->
# MAGIC         <g filter="url(#softShadow)">
# MAGIC             <rect x="556" y="103" width="480" height="159" rx="16" fill="url(#gradBox)" stroke="#a9c0dd" stroke-width="1.5"/>
# MAGIC             <rect x="556" y="283" width="480" height="165" rx="16" fill="url(#gradBox)" stroke="#a9c0dd" stroke-width="1.5"/>
# MAGIC             <rect x="556" y="470" width="480" height="165" rx="16" fill="url(#gradBox)" stroke="#a9c0dd" stroke-width="1.5"/>
# MAGIC         </g>
# MAGIC         <g fill="#1a1a1a" font-size="52" text-anchor="middle">
# MAGIC             <text x="796" y="200">End-To-End</text>
# MAGIC             <text x="796" y="383">Integration</text>
# MAGIC             <text x="796" y="570">Unit</text>
# MAGIC         </g>
# MAGIC         <!-- Left arrow (points down) -->
# MAGIC         <path d="M 40,40 L 92,40 L 92,635 L 117,635 L 66,722 L 15,635 L 40,635 Z" fill="url(#gradDown)"/>
# MAGIC         <text x="66" y="381" fill="#ffffff" font-size="34" font-weight="bold" text-anchor="middle"
# MAGIC                 dominant-baseline="central" transform="rotate(-90 66 381)">Efficiency</text>
# MAGIC         <!-- Right arrow (points up) -->
# MAGIC         <path d="M 1108,700 L 1108,95 L 1083,95 L 1134,38 L 1185,95 L 1160,95 L 1160,700 Z" fill="url(#gradUp)"/>
# MAGIC         <text x="1134" y="400" fill="#ffffff" font-size="34" font-weight="bold" text-anchor="middle"
# MAGIC                 dominant-baseline="central" transform="rotate(-90 1134 400)">Comprehensiveness<text>
# MAGIC         </svg>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## B. Unit Tests / Small Tests
# MAGIC
# MAGIC **You can use unit testing to help improve the quality and consistency of your notebooks’ code.**
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:50%">
# MAGIC     <col style="width:50%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC         <br>
# MAGIC         <ul>
# MAGIC         <li>Checks a single component of an application</li>
# MAGIC         <li>Focuses on testing functionality of individual units only</li>
# MAGIC         <li>Does not uncover issues arising when different modules interact</li>
# MAGIC         </ul>
# MAGIC
# MAGIC         <table style="padding-left: 20px; border: 0px solid #ccc; border-collapse: collapse;">
# MAGIC           <tr style="border: 2px solid #ccc;">
# MAGIC             <th style="border: 2px solid #ccc;">Language</th>
# MAGIC             <th>Example Testing Frameworks</th>
# MAGIC           </tr>
# MAGIC           <tr style="border: 2px solid #ccc;">
# MAGIC             <td style="border: 2px solid #ccc;">Python</td>
# MAGIC             <td><a href="https://docs.databricks.com/aws/en/notebooks/test-notebooks" target="_blank" style="color:#e5302a;">unittest</a>, <a href="https://docs.databricks.com/aws/en/dev-tools/vscode-ext/pytest" target="_blank" style="color:#e5302a;">pytest</a></td>
# MAGIC           </tr>
# MAGIC           <tr style="border: 2px solid #ccc;">
# MAGIC             <td style="border: 2px solid #ccc;">Scala</td>
# MAGIC             <td><a href="https://docs.pytest.org/en/stable/" target="_blank" style="color:#e5302a;">ScalaTest</a></td>
# MAGIC           </tr>
# MAGIC           <tr style="border: 2px solid #ccc;">
# MAGIC             <td style="border: 2px solid #ccc;">R</td>
# MAGIC             <td><a href="https://testthat.r-lib.org/" target="_blank" style="color:#e5302a;">testthat</a></td>
# MAGIC           </tr>
# MAGIC         </table>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 0px;">  
# MAGIC         <h3 style="text-align:center;"><a href="https://docs.databricks.com/aws/en/notebooks/test-notebooks" target="_blank" style="color:#e5302a;">unittest</a></h3>
# MAGIC         <div class="code-block-dark" data-language="python" data-source="raw-python-1"></div>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC
# MAGIC <textarea id="raw-python-1" style="display:none;">
# MAGIC # This function takes a string and returns it reversed
# MAGIC def reverse(s): 
# MAGIC     return s[::-1]
# MAGIC
# MAGIC import unittest
# MAGIC
# MAGIC # Checks if the reverse function correctly reverses
# MAGIC class TestHelpers(unittest.TestCase):
# MAGIC     def test_reverse(self):
# MAGIC         self.assertEqual(reverse('abc'), 'cba')
# MAGIC
# MAGIC # Run the unit test and Assert the tests success. 
# MAGIC r = unittest.main(argv=[''], verbosity=2, exit=False)
# MAGIC assert r.result.wasSuccessful(), 'Test failed; see logs above'
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
# MAGIC ## C. Organize functions and unit tests
# MAGIC
# MAGIC ### A Few Common Approaches for Organizing Functions and Unit Tests
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:33%">
# MAGIC     <col style="width:34%">
# MAGIC     <col style="width:33%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em; vertical-align: top; border-right: 4px solid #ddd;">
# MAGIC      <b>Store functions and unit tests outside of notebooks.</b>
# MAGIC     </td>
# MAGIC     <td style="padding-right: 10px; font-size: 1em; vertical-align: top; border-right: 4px solid #ddd;">
# MAGIC      <b>Store functions in one notebook and unit tests in a separate notebook.</b>
# MAGIC     </td>
# MAGIC     <td style="padding-right: 10px; font-size: 1em; vertical-align: top;">
# MAGIC      <b>Store functions and unit tests within the same notebook.</b>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding-left: 0px; border-right: 4px solid #ddd;">        
# MAGIC         <h4>Benefits:</h4>
# MAGIC         <ul><li>Functions can be reused across multiple environments.</li>
# MAGIC             <li>Test frameworks are better designed to run tests outside of notebooks.</li></ul>
# MAGIC         <h4>Challenges:</h4>
# MAGIC         <ul><li>Not supported for Scala notebooks.</li>
# MAGIC             <li>Increases the number of files to track and maintain.</li></ul>
# MAGIC     </td>
# MAGIC     <td style="padding-left: 10px; border-right: 4px solid #ddd; vertical-align: top;"> 
# MAGIC         <h4>Benefits:</h4>
# MAGIC         <ul><li>Functions are reusable across multiple notebooks.</li>
# MAGIC             <li>Clear separation of logic and testing, making it easier to manage and debug.</li></ul>
# MAGIC         <h4>Challenges:</h4>
# MAGIC         <ul><li>ncreases the number of notebooks to manage.</li>
# MAGIC             <li>Functions are limited to use within notebooks.</li>
# MAGIC             <li>Can be more difficult to test outside of notebooks.</li></ul>
# MAGIC     </td>  
# MAGIC     <td style="padding-left: 10px;">
# MAGIC         <h4>Benefits:</h4>
# MAGIC         <ul><li>Single notebook for easier tracking and maintenance.</li></ul><br>
# MAGIC         <h4>Challenges:</h4>
# MAGIC         <ul><li>Functions are harder to reuse across notebooks.</li>
# MAGIC             <li>Functions are not easily usable outside of the notebook context.</li>
# MAGIC             <li>Testing is more confined to the notebook environment.</li></ul>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## D. Testing notebooks: using %run
# MAGIC
# MAGIC ### Keep Your Test Code Separate From Your Notebook
# MAGIC
# MAGIC <br>
# MAGIC <table>
# MAGIC   <colgroup>
# MAGIC     <col style="width:40%">
# MAGIC     <col style="width:60%">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="padding-right: 10px; font-size: 1em;">
# MAGIC         <br>
# MAGIC         <b>Workflow:</b>
# MAGIC         <ul>
# MAGIC         <li>Create separate notebook with functions that you want to test</li>
# MAGIC         <li>Create separate notebook for tests</li>
# MAGIC         <li>Use %run to import the functions into test notebook</li>
# MAGIC         <li>Depending on the language, defined test functions or test classes</li>
# MAGIC         <li>Execute tests</li>
# MAGIC         </ul>
# MAGIC    </td>
# MAGIC     <td style="padding-left: 0px;">  
# MAGIC         <h3 style="text-align:center;">Example: Model Regression Testing</h3>
# MAGIC         <div class="code-block-dark" data-language="python" data-source="raw-python-1"></div>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC
# MAGIC <textarea id="raw-python-1" style="display:none;">
# MAGIC
# MAGIC # Import the evaluate_model function from the main notebook
# MAGIC %run ./main_notebook
# MAGIC
# MAGIC # Load baseline and new model predictions from Unity Catalog
# MAGIC baseline_pred = 
# MAGIC    spark.read.table("catalog.schema.baseline_predictions")
# MAGIC new_pred = 
# MAGIC    spark.read.table("catalog.schema.new_model_predictions")
# MAGIC
# MAGIC # Evaluate RMSE
# MAGIC baseline_rmse = evaluate_model(baseline_pred, "label", "prediction")
# MAGIC new_rmse = evaluate_model(new_pred, "label", "prediction")
# MAGIC
# MAGIC # Check if the new model is within 5% of the baseline RMSE
# MAGIC assert new_rmse <= baseline_rmse * 1.05, f"Test failed: New RMSE {new_rmse} exceeds baseline {baseline_rmse}"
# MAGIC
# MAGIC print(f"Test passed: New RMSE = {new_rmse}, Baseline RMSE = {baseline_rmse}")
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
# MAGIC ## E. What Should You Test in ML?
# MAGIC
# MAGIC ### Essential Aspects to Validate in Machine Learning Pipelines
# MAGIC
# MAGIC <table style="width: 100% !important; max-width: 100% !important; border-collapse: collapse; table-layout: fixed; margin: 0; padding: 0;">
# MAGIC   <colgroup>
# MAGIC     <col style="width: 50%;">
# MAGIC     <col style="width: 50%;">
# MAGIC   </colgroup>
# MAGIC   <tr>
# MAGIC     <td style="width: 50%; vertical-align: top; padding-left: 20px; box-sizing: border-box;">
# MAGIC       <h4 style="text-align: left;">Data Validation</h4>
# MAGIC             Data is in the correct form and free from errors.
# MAGIC       <div style="text-align: center; margin-top: 20px;">
# MAGIC         <!-- LEFT: Block 1 -->
# MAGIC         <div class="code-block-dark" data-language="python" data-source="raw-a9-left-1"></div>
# MAGIC       </div>
# MAGIC     </td>
# MAGIC     <td style="width: 50%; vertical-align: top; padding-left: 20px; box-sizing: border-box;">
# MAGIC         <h4 style="text-align: left;">Data Transformations</h4>
# MAGIC         Ensure transformations are carried out properly.
# MAGIC       <div style="text-align: center; margin-top: 20px;">
# MAGIC         <!-- RIGHT: Block 1 -->
# MAGIC         <div class="code-block-dark" data-language="python" data-source="raw-a9-right-1"></div>
# MAGIC       </div>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="width: 50%; vertical-align: top; padding-left: 20px; box-sizing: border-box;">
# MAGIC       <h4 style="text-align: left; margin-top: 24px;">Modeling Functions</h4>
# MAGIC         Test functions written to build and operate your model.
# MAGIC       <div style="text-align: center; margin-top: 20px;">
# MAGIC         <!-- LEFT: Block 2 -->
# MAGIC         <div class="code-block-dark" data-language="python" data-source="raw-a9-left-2"></div>
# MAGIC       </div>
# MAGIC     </td>
# MAGIC     <td style="width: 50%; vertical-align: top; padding-left: 20px; box-sizing: border-box;">
# MAGIC       <h4 style="text-align: left; margin-top: 24px;">Model Integration</h4>
# MAGIC         Model integrates and functions well with the system
# MAGIC       <div style="text-align: center; margin-top: 20px;">
# MAGIC         <!-- RIGHT: Block 2 -->
# MAGIC         <div class="code-block-dark" data-language="python" data-source="raw-a9-right-2"></div>
# MAGIC       </div>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC
# MAGIC <!-- Hidden textareas: one per block -->
# MAGIC <textarea id="raw-a9-left-1" style="display:none;">
# MAGIC def test_data_no_missing_values():
# MAGIC     data = load_data()
# MAGIC     assert data.isna().sum().sum() == 0, 
# MAGIC  "Data contains missing values"
# MAGIC </textarea>
# MAGIC
# MAGIC <textarea id="raw-a9-left-2" style="display:none;">
# MAGIC def test_loss_function():
# MAGIC     y_true, y_pred = [0, 1, 0], [0.1, 0.9, 0.2]
# MAGIC     loss = calculate_loss(y_true, y_pred)
# MAGIC     assert abs(loss - expected_loss) < 
# MAGIC  tolerance, "Loss calculation is incorrect"
# MAGIC </textarea>
# MAGIC
# MAGIC <textarea id="raw-a9-right-1" style="display:none;">
# MAGIC def test_data_normalization():
# MAGIC     data = load_data()
# MAGIC     normalized_data = normalize(data)
# MAGIC     assert normalized_data.max() <= 1 and 
# MAGIC  normalized_data.min() >= 0, 
# MAGIC  "Normalization failed"
# MAGIC </textarea>
# MAGIC
# MAGIC <textarea id="raw-a9-right-2" style="display:none;">
# MAGIC def test_model_api_integration():
# MAGIC     resp = client.post('/predict',json={'input': sample})
# MAGIC     assert resp.status_code == 200, "Model API 
# MAGIC  integration failed"
# MAGIC     assert resp.json()['prediction'] == expected_output, 
# MAGIC  "Incorrect prediction from API"
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
# MAGIC ## F. Testing notebooks Using Testing Frameworks
# MAGIC
# MAGIC - [<span style="color:#e5302a;">**Pytest**</span>](https://docs.pytest.org/en/stable/)
# MAGIC     - Popular testing framework for Python.
# MAGIC     - Supports fixtures, parametrized tests, and built-in assertions.
# MAGIC     - Works well with Databricks Python notebooks by importing functions for isolated tests.
# MAGIC
# MAGIC - [<span style="color:#e5302a;">**Testthat**</span>](https://testthat.r-lib.org/)
# MAGIC     - Primarily used for R notebooks.
# MAGIC     - Allows robust unit testing with easy-to-write test cases.
# MAGIC     - Supports reproducibility and detailed error messages for R code.
# MAGIC
# MAGIC - [<span style="color:#e5302a;">**ScalaTest**</span>](https://www.scalatest.org/)
# MAGIC     - Widely used for testing Scala code in Databricks notebooks.
# MAGIC     - Supports behavior-driven and test-driven development.
# MAGIC     - Provides support for both unit testing and integration testing of Spark jobs.
# MAGIC
# MAGIC - [<span style="color:#e5302a;">**SQL**</span>](https://docs.databricks.com/aws/en/notebooks/testing#sql)
# MAGIC     - SQL queries can be validated using assertions in notebooks.
# MAGIC     - Useful for testing data transformations, aggregations, and schema validation.
# MAGIC     - Ensures query outputs match expected results in unit tests.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## G. Optimize Tests in the CI/CD Process
# MAGIC
# MAGIC ### Balancing Speed, Cost, and Comprehensiveness
# MAGIC
# MAGIC Running every test on every change at every stage of the pipeline is inefficient. Aiming for "zero defects in production" leads to diminishing returns and poor ROI. Instead, be **selective**: prioritize high-value tests, catch defects as early as possible, and keep the ability to detect and roll back issues that reach production.
# MAGIC
# MAGIC - **"Zero-defect in production" is unrealistic.** Prioritize catching critical defects while accepting that minor, low-impact issues may occur.
# MAGIC - **Use a defect funnel — catch defects early.** The later a defect is detected, the more expensive it is to handle. Rely on unit and integration tests to address issues before they escalate.
# MAGIC - **Cover what earlier stages miss.** Add end-to-end and scenario-based tests to capture defects that cannot be caught in earlier stages.
# MAGIC - **Detect and roll back in production.** Ensure workflows can detect and mitigate issues quickly using approaches such as **canary deployments**, **probing and monitoring**, and **controlled experiments**.
# MAGIC - **Be selective and data-driven.** Automate the majority of tests, reserve manual testing for critical workflows, and use analytics to remove redundant, low-value tests.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>