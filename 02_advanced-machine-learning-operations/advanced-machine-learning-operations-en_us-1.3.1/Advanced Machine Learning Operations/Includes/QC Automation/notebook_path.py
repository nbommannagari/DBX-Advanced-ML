# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Course-specific inputs

# COMMAND ----------

# Dynamic variables computed at runtime for parameterized notebooks
_user_email = spark.sql("SELECT current_user()").collect()[0][0]
_user_schema = _user_email.split("@")[0].replace(".", "_").replace("-", "_")

# Change the course name as per your course name
COURSE_NAME = "Advanced Machine Learning Operations"
RUN_QA_CHECKER = True
QA_TASK_RELATIVE_PATH = "./qa_content_checker"
QA_TASK_NAME = "QA Content Checker — Grammar, Deprecated, UI Steps"

# Tester emails — notified on job failure and success. Add as many as you like with comma separated emails
TESTER_EMAILS = [
    "gaganpreet.kaur@databricks.com",
    "s.kumar@databricks.com"
]

# Add your lab notebook paths relative from the CourseRunner folder as per your course
LAB_NOTEBOOKS = [
    # M02 — Streamlining MLOps with Databricks
    "../../M02 - Streamlining MLOps with Databricks/2.4 Lab - Building a CI-CD Pipeline with Databricks CLI",
    "../../M02 - Streamlining MLOps with Databricks/2.4 Lab - Pipeline workflow notebooks/01 - Data Cleaning and Transformation",
    "../../M02 - Streamlining MLOps with Databricks/2.4 Lab - Pipeline workflow notebooks/02 - Feature Engineering and Model Training",
    "../../M02 - Streamlining MLOps with Databricks/2.4 Lab - Pipeline workflow notebooks/04 - Model Evaluation and Testing with Accuracy Check",
    # M03 — Model Rollout Strategies with Databricks
    "../../M03 - Model Rollout Strategies with Databricks/3.6 Lab - Rollout Strategies with Lakeflow Jobs",
    "../../M03 - Model Rollout Strategies with Databricks/3.6 Lab - Model Training Pipeline/01 - Silver to Feature Store",
    "../../M03 - Model Rollout Strategies with Databricks/3.6 Lab - Model Training Pipeline/02 - Data Validation Tests",
    "../../M03 - Model Rollout Strategies with Databricks/3.6 Lab - Model Training Pipeline/03 - Normalization Validation",
    "../../M03 - Model Rollout Strategies with Databricks/3.6 Lab - Model Training Pipeline/04  - Train Models on Validated Features",
    "../../M03 - Model Rollout Strategies with Databricks/3.6 Lab - Model Training Pipeline/05 - Serve the Model",
    # M04 — Data Profiling
    "../../M04 - Data Profiling/4.3 Lab - Monitoring Drift with Data Profiling",
]

# Define the course notebook DAG here so this can be used for job scheduling as per your course
COURSE_TASKS = [
    # ── M02: Streamlining MLOps with Databricks ───────────────────────────────
    {
        "task_key": "0 - Generate Tokens",
        "name": "0 - Generate Tokens",
        "relative_path": "../../M02 - Streamlining MLOps with Databricks/0 - Generate Tokens",
        "depends_on": [],
        "env_key": "m02_gen_tokens",
        "use_classic": True,
    },
    {
        "task_key": "2.3 Demo - Classroom Setup Only",
        "name": "2.3 Demo - Classroom Setup Only (Classroom-Setup-2)",
        "relative_path": "../Classroom-Setup-2",
        "depends_on": [
            {"task_key": "0 - Generate Tokens"},
        ],
        "env_key": "m02_demo_cicd",
        "use_classic": True,
    },
    {
        "task_key": "2.4 Lab - Classroom Setup Only",
        "name": "2.4 Lab - Classroom Setup Only (Classroom-Setup-2)",
        "relative_path": "../Classroom-Setup-2",
        "depends_on": [
            {"task_key": "2.3 Demo - Classroom Setup Only"},
        ],
        "env_key": "m02_lab_cicd",
        "use_classic": True,
    },
    # ── M03: Model Rollout Strategies with Databricks ─────────────────────────
    {
        "task_key": "3.2 Demo - Classroom Setup Only",
        "name": "3.2 Demo - Classroom Setup Only (Classroom-Setup-3.2)",
        "relative_path": "../Classroom-Setup-3.2",
        "depends_on": [
            {"task_key": "2.4 Lab - Classroom Setup Only"},
        ],
        "env_key": "m03_demo_testing",
        "use_classic": True,
    },
    {
        "task_key": "3.2 Demo - Wrapper",
        "name": "3.2 Demo - Common Testing Strategies (Wrapper)",
        "relative_path": "./qc_wrapper_3_2_demo",
        "depends_on": [
            {"task_key": "3.2 Demo - Classroom Setup Only"},
        ],
        "env_key": "m03_demo_testing",
        "use_classic": True,
    },
    {
        "task_key": "3.3.1 Training Pipeline - 01 Silver to Feature Store",
        "name": "3.3.1 Training Pipeline - 01 Silver to Feature Store",
        "relative_path": "../../M03 - Model Rollout Strategies with Databricks/3.3.1 - Model Training Pipeline/01 Silver to Feature Store",
        "depends_on": [
            {"task_key": "3.2 Demo - Wrapper"},
        ],
        "env_key": "m03_demo_pipe01",
        "use_classic": True,
        "base_parameters": {
            "catalog": "dbacademy",
            "schema": _user_schema,
            "silver_table_name": "diabetes",
            "column": "Age",
            "target_column": "Diabetes_binary",
            "primary_key": "id",
        },
    },
    {
        "task_key": "3.3.1 Training Pipeline - 02 Features Validation",
        "name": "3.3.1 Training Pipeline - 02 Features Validation",
        "relative_path": "../../M03 - Model Rollout Strategies with Databricks/3.3.1 - Model Training Pipeline/02 Features Validation",
        "depends_on": [
            {"task_key": "3.3.1 Training Pipeline - 01 Silver to Feature Store"},
        ],
        "env_key": "m03_demo_pipe02",
        "use_classic": True,
        "base_parameters": {
            "catalog": "dbacademy",
            "schema": _user_schema,
            "normalized_column": "Age",
        },
    },
    {
        "task_key": "3.3.1 Training Pipeline - 03 Train Model on Validated Features",
        "name": "3.3.1 Training Pipeline - 03 Train Model on Validated Features",
        "relative_path": "../../M03 - Model Rollout Strategies with Databricks/3.3.1 - Model Training Pipeline/03  Train Model on Validated Features",
        "depends_on": [
            {"task_key": "3.3.1 Training Pipeline - 02 Features Validation"},
        ],
        "env_key": "m03_demo_pipe03",
        "use_classic": True,
        "base_parameters": {
            "catalog": "dbacademy",
            "schema": _user_schema,
            "primary_key": "id",
            "target_column": "Diabetes_binary",
            "username": _user_email,
            "silver_table_name": "diabetes",
        },
    },
    {
        "task_key": "3.5 Demo - Model Rollout Strategies with Databricks Model Serving",
        "name": "3.5 Demo - Model Rollout Strategies with Databricks Model Serving",
        "relative_path": "../../M03 - Model Rollout Strategies with Databricks/3.5 Demo - Model Rollout Strategies with Databricks Model Serving",
        "depends_on": [
            {"task_key": "3.3.1 Training Pipeline - 03 Train Model on Validated Features"},
        ],
        "env_key": "m03_demo_rollout",
        "use_classic": True,
    },
    {
        "task_key": "3.6 Lab - Classroom Setup Only",
        "name": "3.6 Lab - Classroom Setup Only (Classroom-Setup-3.Lab)",
        "relative_path": "../Classroom-Setup-3.Lab",
        "depends_on": [
            {"task_key": "3.5 Demo - Model Rollout Strategies with Databricks Model Serving"},
        ],
        "env_key": "m03_lab_rollout",
        "use_classic": True,
    },
    {
        "task_key": "3.6 Lab Training Pipeline - 01 - Silver to Feature Store",
        "name": "3.6 Lab Training Pipeline - 01 - Silver to Feature Store",
        "relative_path": "../../M03 - Model Rollout Strategies with Databricks/3.6 Lab - Model Training Pipeline/01 - Silver to Feature Store",
        "depends_on": [
            {"task_key": "3.6 Lab - Classroom Setup Only"},
        ],
        "env_key": "m03_lab_pipe01",
        "use_classic": True,
        "base_parameters": {
            "catalog": "dbacademy",
            "schema": _user_schema,
            "silver_table_name": "diabetes",
            "column": "Age",
            "target_column": "Diabetes_binary",
            "primary_key": "id",
        },
    },
    {
        "task_key": "3.6 Lab Training Pipeline - 02 - Data Validation Tests",
        "name": "3.6 Lab Training Pipeline - 02 - Data Validation Tests",
        "relative_path": "../../M03 - Model Rollout Strategies with Databricks/3.6 Lab - Model Training Pipeline/02 - Data Validation Tests",
        "depends_on": [
            {"task_key": "3.6 Lab Training Pipeline - 01 - Silver to Feature Store"},
        ],
        "env_key": "m03_lab_pipe02",
        "use_classic": True,
        "base_parameters": {
            "catalog": "dbacademy",
            "schema": _user_schema,
            "silver_table_name": "diabetes",
        },
    },
    {
        "task_key": "3.6 Lab Training Pipeline - 03 - Normalization Validation",
        "name": "3.6 Lab Training Pipeline - 03 - Normalization Validation",
        "relative_path": "../../M03 - Model Rollout Strategies with Databricks/3.6 Lab - Model Training Pipeline/03 - Normalization Validation",
        "depends_on": [
            {"task_key": "3.6 Lab Training Pipeline - 02 - Data Validation Tests"},
        ],
        "env_key": "m03_lab_pipe03",
        "use_classic": True,
        "base_parameters": {
            "catalog": "dbacademy",
            "schema": _user_schema,
            "normalized_column": "Age",
            "silver_table_name": "diabetes",
        },
    },
    {
        "task_key": "3.6 Lab Training Pipeline - 04 - Train Models on Validated Features",
        "name": "3.6 Lab Training Pipeline - 04 - Train Models on Validated Features",
        "relative_path": "../../M03 - Model Rollout Strategies with Databricks/3.6 Lab - Model Training Pipeline/04  - Train Models on Validated Features",
        "depends_on": [
            {"task_key": "3.6 Lab Training Pipeline - 03 - Normalization Validation"},
        ],
        "env_key": "m03_lab_pipe04",
        "use_classic": True,
        "base_parameters": {
            "catalog": "dbacademy",
            "schema": _user_schema,
            "silver_table_name": "diabetes",
            "delete_column": "BMI",
            "primary_key": "id",
            "target_column": "Diabetes_binary",
        },
    },
    {
        "task_key": "3.6 Lab Training Pipeline - 05 - Serve the Model",
        "name": "3.6 Lab Training Pipeline - 05 - Serve the Model",
        "relative_path": "../../M03 - Model Rollout Strategies with Databricks/3.6 Lab - Model Training Pipeline/05 - Serve the Model",
        "depends_on": [
            {"task_key": "3.6 Lab Training Pipeline - 04 - Train Models on Validated Features"},
        ],
        "env_key": "m03_lab_pipe05",
        "use_classic": True,
        "base_parameters": {
            "catalog": "dbacademy",
            "schema": _user_schema,
        },
    },
    # ── M04: Data Profiling ───────────────────────────────────────────────────
    {
        "task_key": "4.2 Demo - Data Profiling Model Quality",
        "name": "4.2 Demo - Data Profiling Model Quality",
        "relative_path": "../../M04 - Data Profiling/4.2 Demo - Data Profiling Model Quality",
        "depends_on": [
            {"task_key": "3.6 Lab Training Pipeline - 05 - Serve the Model"},
        ],
        "env_key": "m04_demo_profiling",
        "use_classic": True,
    },
    {
        "task_key": "4.3 Lab - Monitoring Drift with Data Profiling",
        "name": "4.3 Lab - Monitoring Drift with Data Profiling",
        "relative_path": "../../M04 - Data Profiling/4.3 Lab - Monitoring Drift with Data Profiling",
        "depends_on": [
            {"task_key": "4.2 Demo - Data Profiling Model Quality"},
        ],
        "env_key": "m04_lab_profiling",
        "use_classic": True,
    },
    # ── M05: Build ML Assets as Code ─────────────────────────────────────────
    {
        # Writes var/credentials.cfg (job-run token) and runs M05 "0 - Generate Tokens";
        # the widget-based token entry cannot be filled in during a job run.
        "task_key": "5.0 - Generate Tokens",
        "name": "5.0 - Generate Tokens (M05, QC Wrapper)",
        "relative_path": "./qc_wrapper_5_0_generate_tokens",
        "depends_on": [
            {"task_key": "4.3 Lab - Monitoring Drift with Data Profiling"},
        ],
        "env_key": "m05_gen_tokens",
        "use_classic": True,
    },
    {
        # Pre-creates my_project via `bundle init`, waits for the files to be visible,
        # then runs the 5.2 Demo (job runs execute cells too fast for the new files to appear).
        "task_key": "5.2 Demo - Working with Declarative Automation Bundles",
        "name": "5.2 Demo - Working with Declarative Automation Bundles (QC Wrapper)",
        "relative_path": "./qc_wrapper_5_2_demo_dabs",
        "depends_on": [
            {"task_key": "5.0 - Generate Tokens"},
        ],
        "env_key": "m05_demo_dabs",
        "use_classic": True,
    },
]

# COMMAND ----------

# ── Auto-Fill Lab Notebooks ──────────────────────────────────────────────────
# This script finds unresolved lab placeholders in lab notebooks and replaces
# them with solution code extracted from the nearby solution blocks.
#
# HOW IT WORKS:
# 1. Exports each lab notebook as .ipynb JSON
# 2. Scans code cells for unresolved markers such as <FILL_IN>, <FILL-IN>,
#    <ToDo>, and TODO-only scaffolds
# 3. Looks ahead in subsequent markdown cells for <code>...</code> blocks
# 4. Extracts the solution and replaces the original cell source
# 5. Re-imports the patched notebook
#
# Safe to re-run: it only patches cells that still contain unresolved markers.
# ──────────────────────────────────────────────────────────────────────────────

import base64
import json
import os
import re
import html as html_lib
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

w = WorkspaceClient()

# Derive course_root from this notebook's own path
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
course_root = "/".join(_nb_path.split("/")[:-1])

# LAB_NOTEBOOKS is configured in the course-specific inputs cell above.
# Resolve to absolute workspace paths
lab_abs_paths = [os.path.normpath(f"{course_root}/{p}") for p in LAB_NOTEBOOKS]

PLACEHOLDER_PATTERNS = [
    re.compile(r'<\s*fill[-_ ]?in\s*>', re.IGNORECASE),
    re.compile(r'--\s*fill[-_ ]?in\s*--', re.IGNORECASE),
    re.compile(r'<\s*todo\s*>', re.IGNORECASE),
    re.compile(r'(?mi)^\s*#+\s*todo\b'),
    re.compile(r'(?mi)^\s*#+.*\bTODO\b'),
    # SQL lab pattern: -- TODO comment at the start of a cell (used in Databricks Academy SQL labs)
    re.compile(r'(?mi)^\s*--\s*TODO\b'),
    # Bare FILL_IN used as a Python identifier (e.g., payments_schema = FILL_IN)
    re.compile(r'\bFILL_IN\b'),
]


def has_unresolved_placeholder(source: str) -> bool:
    """Return True when a code cell still contains a fill-in or TODO scaffold."""
    return any(pattern.search(source) for pattern in PLACEHOLDER_PATTERNS)


def extract_solution_from_source(source: str) -> str | None:
    """Extract code from a Databricks solution cell.

    Handles:
    - Fenced code blocks: ```python\\n...\\n``` (used in current labs)
    - HTML <div class="cb"> blocks with <br/> line separators (Databricks Academy format)
    - HTML <code> tags: <code>...</code> (legacy fallback)
    - Strips %md / %md-sandbox magic prefixes before parsing.
    """
    # Strip Databricks magic prefix (%md, %md-sandbox, etc.)
    clean = re.sub(r'^%md[a-zA-Z-]*[ \t]*\n', '', source.lstrip(), count=1)

    # 1. Fenced code block (most common in current labs)
    match = re.search(r'```(?:python|sql|scala|r|sh)?\s*\n(.*?)\n[ \t]*```', clean, re.DOTALL)
    if match:
        code = match.group(1)
        lines = code.split('\n')
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        return '\n'.join(lines) if lines else None

    # 2. Databricks Academy <div class="cb"> format (uses <br/> as line separators)
    match = re.search(r'<div\s+class=["\']cb["\'][^>]*>\s*\n?(.*?)\n?\s*</div>', clean, re.DOTALL)
    if match:
        raw = match.group(1)
        # Replace <br/> variants with newlines
        raw = re.sub(r'<br\s*/?>', '\n', raw)
        # Unescape HTML entities (&gt; &lt; &amp; etc.)
        code = html_lib.unescape(raw)
        lines = code.split('\n')
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        return '\n'.join(lines) if lines else None

    # 3. HTML <code> tags (fallback for older lab format)
    match = re.search(r'<code[^>]*>\s*\n?(.*?)\n?\s*</code>', clean, re.DOTALL)
    if match:
        code = html_lib.unescape(match.group(1))
        lines = code.split('\n')
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        return '\n'.join(lines) if lines else None

    return None


def extract_answer_cell(source: str) -> str | None:
    """Extract solution from a SQL '-- ANSWER' code cell.

    Strips the leading '-- ANSWER' comment line and returns the remaining code.
    Used in Databricks Academy SQL lab notebooks where answers follow TODO cells
    directly as sibling code cells (rather than being embedded in markdown hint blocks).
    """
    lines = source.split('\n')
    result_lines = []
    skipping_answer_header = True
    for line in lines:
        if skipping_answer_header and re.match(r'\s*--\s*ANSWER\b', line, re.IGNORECASE):
            continue
        skipping_answer_header = False
        result_lines.append(line)
    # Strip leading/trailing blank lines
    while result_lines and not result_lines[0].strip():
        result_lines.pop(0)
    while result_lines and not result_lines[-1].strip():
        result_lines.pop()
    return '\n'.join(result_lines) if result_lines else None


def extract_skip_cell(source: str) -> str | None:
    """Extract solution from a '%skip' code cell.

    Strips the leading '%skip' line and returns the remaining code.
    Used in Databricks Academy labs where solution cells are marked with %skip magic.
    """
    lines = source.split('\n')
    result_lines = []
    skipping_header = True
    for line in lines:
        if skipping_header and re.match(r'\s*%skip\b', line, re.IGNORECASE):
            continue
        skipping_header = False
        result_lines.append(line)
    # Strip leading/trailing blank lines
    while result_lines and not result_lines[0].strip():
        result_lines.pop(0)
    while result_lines and not result_lines[-1].strip():
        result_lines.pop()
    return '\n'.join(result_lines) if result_lines else None


def is_solution_candidate(cell: dict) -> bool:
    """Return True if a cell is a potential answer/solution block.

    In Databricks ipynb exports, %md-sandbox cells are stored as code cells
    (not markdown), so we must check both cell_type=='markdown' and code cells
    that start with a %md magic prefix.
    """
    source = ''.join(cell.get("source", []))
    has_code_block = '```' in source or '<code>' in source or 'class="cb"' in source or "class='cb'" in source
    is_markdown_type = cell.get("cell_type") == "markdown"
    is_md_magic = cell.get("cell_type") == "code" and bool(re.match(r'\s*%md', source))
    return (is_markdown_type or is_md_magic) and has_code_block


def fill_notebook(notebook_path: str) -> dict:
    """Fill all unresolved placeholder cells in a single notebook. Returns stats."""
    stats = {"path": notebook_path, "blanks_found": 0, "filled": 0, "no_answer": 0}

    # Export notebook as ipynb
    export_resp = w.workspace.export(path=notebook_path, format=ExportFormat.JUPYTER)
    nb_json = json.loads(base64.b64decode(export_resp.content))
    cells = nb_json.get("cells", [])

    for i, cell in enumerate(cells):
        source = ''.join(cell.get("source", []))

        # Check if this cell still has unresolved fill-in / TODO markers
        if not has_unresolved_placeholder(source):
            continue

        # Skip markdown cells (we only fill code cells)
        if cell.get("cell_type") != "code":
            continue

        # Skip %md / %md-sandbox magic cells (they are code type in ipynb but render as markdown)
        if re.match(r'\s*%md', source):
            continue

        stats["blanks_found"] += 1

        # Look ahead up to 8 cells for a solution block.
        # %md-sandbox solution cells may appear as 'code' type in ipynb exports,
        # so is_solution_candidate() checks both cell_type and %md magic prefix.
        solution = None
        for j in range(i + 1, min(i + 9, len(cells))):
            next_cell = cells[j]
            next_source = ''.join(next_cell.get("source", []))
            # SQL lab pattern: -- ANSWER code cell immediately follows -- TODO cell
            if (next_cell.get("cell_type") == "code"
                    and re.match(r'\s*--\s*ANSWER\b', next_source, re.IGNORECASE)):
                solution = extract_answer_cell(next_source)
                if solution:
                    break
            # %skip pattern: solution cell with %skip magic prefix
            elif (next_cell.get("cell_type") == "code"
                    and re.match(r'\s*%skip\b', next_source, re.IGNORECASE)):
                solution = extract_skip_cell(next_source)
                if solution:
                    break
            elif is_solution_candidate(next_cell):
                solution = extract_solution_from_source(next_source)
                if solution:
                    break
            # Stop looking once we hit a real (non-magic, non-ANSWER) code cell
            elif (next_cell.get("cell_type") == "code"
                  and not re.match(r'\s*%', next_source)
                  and not re.match(r'\s*--\s*ANSWER\b', next_source, re.IGNORECASE)):
                break

        if solution:
            # Check if solution has %sql magic — if so, handle cell language
            if solution.strip().startswith('%sql'):
                # Remove %sql prefix, keep the SQL code
                solution_lines = solution.strip().split('\n')
                # Keep %sql as first line for the cell magic
                cell["source"] = [solution + '\n']
            else:
                cell["source"] = [solution + '\n']
            stats["filled"] += 1
        else:
            stats["no_answer"] += 1
            print(f"  ⚠️  NO ANSWER FOUND for cell {i} in {notebook_path}")

    # ── Post-process: replace <schema-name> placeholders with actual user schema ──
    # Some cells (and their solutions) contain <`schema-name`> or <schema-name> as a
    # literal placeholder for the user's schema. Replace across ALL cells.
    _schema_pats = [
        re.compile(r'<`schema[-_ ]?name`>', re.IGNORECASE),
        re.compile(r'<schema[-_ ]?name>', re.IGNORECASE),
    ]
    for cell in cells:
        source = ''.join(cell.get("source", []))
        new_source = source
        for pat in _schema_pats:
            new_source = pat.sub(f'`{_user_schema}`', new_source)
        if new_source != source:
            cell["source"] = [new_source]
            stats["filled"] += 1

    # ── Post-process: replace unbound SQL query parameters with defaults ───────
    # SQL cells using :`param name` syntax throw UNBOUND_SQL_PARAMETER in jobs.
    # Replace known interactive parameters with sensible defaults.
    _param_defaults = {
        "Time Window Start": "1970-01-01T00:00:00",
        "Time Window End":   "2099-12-31T23:59:59",
    }
    for cell in cells:
        source = ''.join(cell.get("source", []))
        new_source = source
        for _pname, _pval in _param_defaults.items():
            new_source = new_source.replace(f":`{_pname}`", f"'{_pval}'")
        if new_source != source:
            cell["source"] = [new_source]
            stats["filled"] += 1

    # Re-import the patched notebook
    if stats["filled"] > 0:
        patched_content = base64.b64encode(json.dumps(nb_json).encode()).decode()
        w.workspace.import_(
            path=notebook_path,
            content=patched_content,
            format=ImportFormat.JUPYTER,
            overwrite=True,
            language=Language.PYTHON,
        )

    return stats


# ── Run the filler ────────────────────────────────────────────────────────────
print("═" * 70)
print("AUTO-FILL LAB NOTEBOOKS")
print("═" * 70)

all_stats = []
for path in lab_abs_paths:
    print(f"\n▶ Processing: {path.split('/')[-1]}")
    try:
        s = fill_notebook(path)
        all_stats.append(s)
        print(f"   Blanks found: {s['blanks_found']}  |  Filled: {s['filled']}  |  No answer: {s['no_answer']}")
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        all_stats.append({"path": path, "blanks_found": 0, "filled": 0, "no_answer": 0, "error": str(e)})

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "═" * 70)
total_blanks = sum(s["blanks_found"] for s in all_stats)
total_filled = sum(s["filled"] for s in all_stats)
total_no_ans = sum(s["no_answer"] for s in all_stats)
print(f"TOTAL:  {total_blanks} blanks found  |  {total_filled} filled  |  {total_no_ans} blockers")
print("═" * 70)