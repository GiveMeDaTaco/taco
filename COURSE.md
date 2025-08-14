# TLPTACO Developer Training

Welcome to the **TLPTACO** (Teradata-Linked Progressible Templated Automated Campaign Orchestrator) developer course.  
This self-contained guide will teach you how the library works, how to configure and run a full campaign, and how to extend or test the code-base.  
It is designed for developers with **low-to-medium Python proficiency** and basic SQL knowledge.

*Total duration: 2 half-day sessions* &nbsp;•&nbsp; *Hands-on time: ~2.5 h*

---

## Table of Contents

1. [Prerequisites & Environment](#1-prerequisites--environment)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Quick-Start Walk-through](#3-quick-start-walk-through)
4. [Python Warm-up](#4-python-warm-up)
5. [Configuration Deep-Dive](#5-configuration-deep-dive)
6. [Engine Internals](#6-engine-internals)
7. [Utilities & Logging](#7-utilities--logging)
8. [Extending TLPTACO](#8-extending-tlptaco)
9. [Testing & Debugging](#9-testing--debugging)
10. [Operational Checklist](#10-operational-checklist)
11. [Waterfall Excel Report Anatomy](#11-waterfall-excel-report-anatomy)
12. [Teradata & SQL Primer](#12-teradata--sql-primer)
13. [Reference Cheat-Sheet](#13-reference-cheat-sheet)

> **How to use this file**  
> Read sequentially or jump to sections as needed.  
> Inline **labs** are numbered; complete them in order for the best learning curve.

---

## 1. Prerequisites & Environment

| Requirement | Version / Note |
|-------------|----------------|
| Python      | 3.10 +         |
| Git         | Any            |
| CLI         | Bash / PowerShell / CMD |

### 1.1 Installation Steps

```bash
# Clone course repository (this one)
git clone <your-fork-or-clone>
cd tlptaco

# Create & activate virtual environment
python -m venv venv
source venv/bin/activate  # Powershell: .\venv\Scripts\Activate.ps1

# Install dependencies
pip install -e .[dev]   # installs tlptaco plus testing & rich extras

# Verify tests run (uses SQLite stub, no Teradata needed)
pytest -q
```

### 1.2 Folder Layout after Install

```
tlptaco/            ← library source code
tests/              ← pytest suite (great for examples)
examples/
  example_campaign.yaml
  example_sql.sql
COURSE.md           ← you are here
```

---

## 2. High-Level Architecture

```text
┌────────────────────────────┐
│         CLI (click)        │  tlptaco/cli.py
└──────────────┬─────────────┘
               ▼
┌──────────────┴─────────────┐
│         PreSQLEngine       │  executes prep scripts
└──────────────┬─────────────┘
               ▼
┌──────────────┴─────────────┐
│      EligibilityEngine     │  builds smart eligibility table
└──────────────┬─────────────┘
               ▼
┌──────────────┴─────────────┐
│       WaterfallEngine      │  calculates metrics & Excel
└──────────────┬─────────────┘
               ▼
┌──────────────┴─────────────┐
│        OutputEngine        │  final lists / DB tables
└────────────────────────────┘
```

All heavy SQL is rendered from **Jinja2 templates** under `tlptaco/sql/templates`.  
Database access goes through `DBRunner → DBConnection` (Teradata by default).

---

## 3. Quick-Start Walk-through

Run a minimal campaign end-to-end.

```bash
# from repo root
python -m tlptaco.cli \
  --config example_campaign.yaml \
  --output-dir runs/first_try \
  --progress
```

Observe:

* **Spinner → progress bars** (Rich) in terminal.
* `runs/first_try/logs/` with emoji-rich logs & rendered SQL.
* `runs/first_try/waterfall/*.xlsx` consolidated report.
* Output files per channel (CSV/Parquet or DB tables).

> ##### Lab 0 – Explore the artefacts (10 min)
> 1. Open the generated Excel; identify sections, templates, metrics.  
> 2. Open the SQL log file and find the query that built the smart table.

---

## 4. Python Warm-up

| Topic | Why it matters | Mini-exercise |
|-------|----------------|---------------|
| **Pydantic v2** models | Validates YAML → `schema.py` | Modify `unique_identifiers`, observe validation error. |
| **Jinja2** templates | SQL generation | Render `eligibility.sql.j2` with dummy context. |
| **Click / argparse** | CLI options | Run `python -m tlptaco.cli --help`. |
| **Pandas basics** | DataFrames used for OutputEngine, analytics | Load a CSV & select two columns. |

Code snippet – render SQL manually:

```python
from tlptaco.sql.generator import SQLGenerator

ctx = {
    "eligibility_table": "sandbox.demo_elig",
    "unique_identifiers": ["c.id"],
    "unique_without_aliases": ["id"],
    "tables": [],
    "where_clauses": [],
    "checks": [
        {"name": "main_BA_1", "sql": "c.flag = 1"},
    ],
}

gen = SQLGenerator("tlptaco/sql/templates")
print(gen.render("eligibility.sql.j2", ctx)[:400])
```

> **Tip – Pandas one-liner**  
> `import pandas as pd; pd.read_parquet('reports/sms/sms_list.parquet').head()` to sanity-check a generated file.

---

## 5. Configuration Deep-Dive

### 5.1 Top-level Anatomy (`AppConfig`)

```yaml
offer_code: RUN01
logging:
  level: INFO
  file: logs/run.log
database:
  user: alice
eligibility:
  unique_identifiers: [c.customer_id]
  # … tables & conditions …
waterfall:
  count_columns: [customer_id, [customer_id, account_id]]
output:
  channels:
    email:
      columns: [c.customer_id, c.email]
      file_location: reports/email
      file_base_name: email_list
      output_options: {format: csv}
pre_sql:
  - path: pre/cleanup.sql
```

### 5.2 Conditions Grammar

* **BA** = Base Audience filter (all must pass).  
* **segments** (optional) refine BA.

```yaml
conditions:
  main:
    BA:
      - sql: "c.has_accounts = 1"          # auto-named main_BA_1
  channels:
    sms:
      BA: []                                # now optional ✓
      segments:
        loyalty:
          - sql: "c.loyalty_flag = 1"
```

> ##### Lab 1 – Build Your Config (30 min)
> 1. Copy `config_minimal.yaml` → `my_campaign.yaml`.  
> 2. Add a **push** channel with no BA and two segments.  
> 3. Run pipeline, inspect output files.

---

## 6. Engine Internals

### 6.1 PreSQLEngine

```text
Input : list of .sql files (+ optional analytics)
Output: executed statements, distinct-count logs
```

Key points
* Splits SQL on `;` naïvely – avoid PL/SQL blocks.  
* Analytics snippet:

```yaml
pre_sql:
  - path: prepare_tables.sql
    analytics:
      table: sandbox.tmp_eligibility
      unique_counts: [customer_id, [company_id, site_id]]
```

### 6.2 EligibilityEngine

1. Collects **all** checks (main + channels).  
2. Renders `eligibility.sql.j2`.  
3. Executes statements, logs row counts & distincts.

### 6.3 WaterfallEngine

* Groups defined by `waterfall.count_columns`.
* For each group:
  * Base → Channel BA → Segment breakdown.
  * Data pivoted and consolidated.
* Excel writer adds history comparison from SQLite.

### 6.4 OutputEngine

* For each channel:
  * Renders `output.sql.j2` (CASE logic for segments).
  * Writes **table** or **file** (csv/parquet/excel).  
  * Optional `output_options.custom_function` post-processing.
* Generates **failed_records** file/table if enabled.

> ##### Lab 2 – Investigate SQL Rendering (25 min)
> 1. Set `--verbose` and run.  
> 2. Open the SQL log; map each section to template file.

---

## 7. Utilities & Logging

### 7.1 Logging

* Uses **Rich** for colourful console when `--verbose`.  
* Emoji prefixes per level (🐛, ⚠️ …).  
* Dedicated *SQL* logger writes raw text for copy-paste.  
* Exclude sections via `logging.sql_exclude_sections` list.

### 7.2 Progress UX

* `utils.loading_bar.LoadingSpinner` for startup.  
* `ProgressManager` – single overall bar + per-stage bars.

### 7.3 File-system helper

`utils.fs.grant_group_rwx(path)` – ensures group write/execute on Linux (shared dirs).

---

## 8. Extending TLPTACO

### 8.1 Adding a New Output Format (JSON-Lines)

1. Implement writer in `tlptaco/iostream/writer.py`:
   ```python
   elif fmt == "jsonl":
       df.to_json(path, orient="records", lines=True, **kwargs)
   ```
2. Reference in config:
   ```yaml
   output_options:
     format: jsonl
   ```

### 8.2 Custom Transform

```python
# my_transforms.py
def redact_emails(df):
    df["email"] = "***REDACTED***"
    return df
```

```yaml
output:
  channels:
    email:
      output_options:
        format: csv
        custom_function: my_transforms.redact_emails
```

### 8.3 Supporting another Database

* Subclass `DBConnection` (e.g. for PostgreSQL).  
* Inject via monkey-patch or extend `DBRunner`.

---

## 9. Testing & Debugging

### 9.1 Running Tests

```bash
pytest -vv -k waterfall
```

### 9.2 Writing a New Test (example skeleton)

```python
def test_output_sql_contains_segment(tmp_path):
    # load minimal config
    cfg = load_config("example_campaign.yaml")
    runner = DummyRunner()
    elig = EligibilityEngine(cfg.eligibility, runner)
    out  = OutputEngine(cfg.output, runner)

    out.num_steps(elig)  # prepares SQL
    sql_texts = [j["sql"] for j in out._output_jobs]
    assert any("loyalty" in s for s in sql_texts)
```

### 9.3 Mocking the DB Layer

```python
class DummyRunner(DBRunner):
    def __init__(self):
        pass
    def run(self, sql):
        return None
    def to_df(self, sql):
        import pandas as pd
        return pd.DataFrame()
```

---

## 10. Operational Checklist

| Item | Recommendation |
|------|----------------|
| Kerberos vs password | Use `logmech: KRB5` in prod; store tickets via kinit. |
| Scheduling | Wrap CLI with Airflow/Bamboo; one output-dir per run. |
| Waterfall history | Purge or vacuum SQLite periodically. |
| Log rotation | Point `logging.file` to a rotating handler (logrotate). |

---

## 11. Reference Cheat-Sheet

### Common CLI Flags

| Flag | Purpose | Example |
|------|---------|---------|
| `-c, --config` | path to YAML/JSON config | `-c campaign.yaml` |
| `-o, --output-dir` | root directory for artefacts | `-o runs/20250812` |
| `-m, --mode` | `full` or `presizing` | `-m presizing` |
| `-v, --verbose` | rich DEBUG console logs | `-v` |
| `-p, --progress` | show progress bars | `-p` |

### Template Directory Map

| Template | Engine | Description |
|----------|--------|-------------|
| `eligibility.sql.j2` | Eligibility | Creates smart table with flag cols |
| `waterfall_full.sql.j2` | Waterfall | Computes BA waterfall |
| `waterfall_segments.sql.j2` | Waterfall | Segment detail drop metrics |
| `output.sql.j2` | Output | Channel SELECT + CASE template logic |
| `failed_records.sql.j2` | Output | One-row-per-failed-identifier list |

### Key Python Entry Points

| Path | Role |
|------|------|
| `tlptaco/cli.py` | Main CLI orchestrator |
| `tlptaco/config/schema.py` | Pydantic models |
| `tlptaco/engines/*` | Core engines |
| `tlptaco/sql/generator.py` | Jinja wrapper |

---

### End of Course

You are now equipped to run, troubleshoot, and extend TLPTACO campaigns.  
For questions or contributions open a pull-request or reach out on the project chat.  

Happy campaigning! 🚀
