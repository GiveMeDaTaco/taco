 ────────────────────────────────────────────────────────────────────────────
 TLPTACO 2 – DEVELOPER & POWER-USER GUIDE
 ────────────────────────────────────────────────────────────────────────────

 CONTENTS
 0.  Notation & Conventions

     1. Big-Picture Architecture
     2. Package / Module Map
     3. End-to-End Execution Flow
     4. Detailed Component Reference
        4.1  Configuration System
        4.2  Database Layer
        4.3  Eligibility Engine
        4.4  Waterfall Engine
        4.5  Output  Engine
        4.6  Utilities (logging, spinner, IO, validation, tests)
     5. Design Choices & Assumptions
     6. Extensibility Hooks
     7. Tests & Local Development Tips
     8. “HOW-TO” – Authoring & Running a Campaign
        8.1  Minimal YAML Template
        8.2  All Config Options Explained
        8.3  Advanced Waterfall History Controls
        8.4  Output Customisation (file formats, transforms)
        8.5  CLI Usage Patterns & Troubleshooting
     9. FAQ / Cheat-Sheet

 ────────────────────────────────────────────────────────────────────────────
 0. NOTATION & CONVENTIONS
 ────────────────────────────────────────────────────────────────────────────
 • “YAML path” == dotted key path inside the configuration file
 • CLI options are shown in POSIX format:  --long,  -s
 • File/dir names are italics; code symbols are monospace.

 ────────────────────────────────────────────────────────────────────────────

     1. BIG-PICTURE ARCHITECTURE
        ────────────────────────────────────────────────────────────────────────────

                                   +-------------------+
                                   |   YAML / JSON     |
                                   |  configuration    |
                                   +---------+---------+
                                             |
                                             v  (tlptaco.config)
                        +-------------------------------------------------+
                        |                AppConfig object                |
                        +-------------------------------------------------+
                                             |
             +---------------+--------------+--------------+-------------+
             |               |                             |             |
             v               v                             v             v

   Eligibility      Waterfall                    Output            Logging
     Engine          Engine                      Engine            system
    (SQL ->   ---> smart table ---> metrics/Excel ---> files  ---> Rich/emoji
    Teradata)          c.*                       (email/sms/push)   + files

 Execution is orchestrated by tlptaco.cli which:

     1. parses CLI flags, starts a spinner;
     2. loads YAML into Pydantic models (full validation);
     3. configures logging;
     4. creates a DBRunner (wrapping a teradatasql connection);
     5. runs Eligibility → Waterfall → (optional) Output, optionally
        showing rich progress bars.

 ────────────────────────────────────────────────────────────────────────────
 2. PACKAGE / MODULE MAP
 ────────────────────────────────────────────────────────────────────────────
 tlptaco/
 ├── cli.py                    – command-line entry-point
 ├── config/                   – schema.py (Pydantic) + loader.py
 ├── db/
 │   ├── connection.py         – thin teradatasql wrapper
 │   └── runner.py             – logging, timings, DF helpers
 ├── engines/
 │   ├── eligibility.py
 │   ├── waterfall.py
 │   ├── waterfall_excel.py    – XLSX renderer
 │   └── output.py
 ├── sql/
 │   ├── generator.py          – Jinja2 wrapper
 │   └── templates/            – *.sql.j2 templates (eligibility, waterfall…)
 ├── iostream/                 – read / write DataFrames
 ├── utils/
 │   ├── logging.py            – Rich/emoji + SQL logger
 │   ├── loading_bar.py        – spinner & ProgressManager
 │   └── validation.py         – misc helpers
 └── tests/                    – Pytest suite (DB stubbed)

 ────────────────────────────────────────────────────────────────────────────
 3. END-TO-END EXECUTION FLOW
 ────────────────────────────────────────────────────────────────────────────
 CLI → load_config → AppConfig
     → configure_logging → DBRunner(host/user/…)
     → EligibilityEngine.run()
           • render eligibility.sql.j2
           • drop+create smart table with flag columns
     → WaterfallEngine.run()
           • prepare group jobs (BA, segments…)
           • for each group: SQL → pandas DF → metrics
           • write consolidated & per-group Excel
           • optional history logging & retrieval
     → OutputEngine.run()
           • per channel: build CASE SQL → DF
           • optional custom function
           • write csv / parquet / xlsx  + “.end” stamp
     → DBRunner.cleanup()

 ────────────────────────────────────────────────────────────────────────────
 4. DETAILED COMPONENT REFERENCE
 ────────────────────────────────────────────────────────────────────────────

 ## 4.1 Configuration System

 File types        : *.yaml, *.yml, *.json
 Parser            : config.loader.load_config()
 Validation        : Pydantic models in schema.py
 Key Features      :
 • Cross-section checks (aliases used in eligibility vs waterfall, etc.)
 • Auto-naming of eligibility checks (main_BA_1, email_seg1_3 …)
 • Automatic defaulting & coercion (logging paths, history window…)
 • Forward compatibility – unknown keys ignored with extra="allow" where needed.

 History config (section waterfall.history):

 ┌─────────────────────┬─────────────────────┬───────────────────────────────────────────────┐
 │ Field name          │ YAML alias (legacy) │ Meaning                                       │
 ├─────────────────────┼─────────────────────┼───────────────────────────────────────────────┤
 │ track               │ track               │ True/False – store runs in SQLite             │
 ├─────────────────────┼─────────────────────┼───────────────────────────────────────────────┤
 │ db_path             │ db_path             │ Override .sqlite file path                    │
 ├─────────────────────┼─────────────────────┼───────────────────────────────────────────────┤
 │ recent_window_days  │ lookback_days       │ Latest run within last N days                 │
 ├─────────────────────┼─────────────────────┼───────────────────────────────────────────────┤
 │ compare_offset_days │ days_ago_to_compare │ Run closest to exactly N days ago (overrides) │
 └─────────────────────┴─────────────────────┴───────────────────────────────────────────────┘

 ## 4.2 Database Layer

 DBConnection: thin, stateless wrapper around teradatasql.
 DBRunner:  • lazy connection
            • .run(sql) for DDL/DML
            • .to_df(sql) → pandas DataFrame
            • automatic timing & logging
            • .fastload() placeholder for future bulk loads.

 ## 4.3 Eligibility Engine

 Input   : EligibilityConfig
 Output  : Smart table (eligibility_table) with flag columns per check.
 Process :

     1. Build `tables`, `where_clauses`, `checks` context
     2. Render eligibility.sql.j2 (JOINs + CASE WHEN flags…)
     3. DROP TABLE (ignore failure) → CREATE TABLE AS → COLLECT STATS
     4. Steps count = drop + #statements (used by progress bar).

 ## 4.4 Waterfall Engine

 Input   : WaterfallConfig + EligibilityEngine
 Outputs : • waterfall_report_***.xlsx (per groups & consolidated)
           • optional history sqlite rows
 Key Internals
 • Groups derived from count_columns (single col or combo).
 • For each group a list of SQL jobs:
     – Base waterfall for main BA
     – Channel BA waterfalls
     – Channel segment waterfalls (waterfall_segments.sql.j2)
 • Metrics pivoting → unique_drops, regain, incremental, cumulative, remaining
 • WaterfallExcel writer:
     – consolidated sheet identical to legacy
     – per-group sheet: NEW side-by-side layout
         Historic | Current | Δ | % Change
       with historic run date in header.

 History handling
 • If compare_offset_days is set → fetch closest run to N days ago.
 • Else → fetch latest run inside recent_window_days.
 • Starting population saved & compared.

 ## 4.5 Output Engine

 Drives final customer lists. For each channel:
 • Build list of mutually exclusive CASE WHEN conditions:
       – Channel BA
       – Ordered segments (adding inverse exclusions)
 • Render output.sql.j2 (SELECT columns, WHEN … THEN template)
 • Fetch DataFrame, apply optional python function
 • Write file: csv / parquet / xlsx; always create companion .end.

 ## 4.6 Utilities

 logging.py        – Rich console with emoji + file handlers; dedicated SQL log.
 loading_bar.py    – spinner (CLI startup) + ProgressManager (nested bars).
 iostream.*        – thin wrappers for pandas read/write.
 validation.py     – misc helpers for future rules.
 tests/            – Monkey-patched DB & template stubs → fast CI.

 ────────────────────────────────────────────────────────────────────────────
 5. DESIGN CHOICES & ASSUMPTIONS
 ────────────────────────────────────────────────────────────────────────────
 • Teradata as primary RDBMS; SQL templates written in ANSI-ish dialect.
 • No ORM – raw SQL + Jinja2 for transparency & copy-pasteability.
 • One smart eligibility table per run – simplifies downstream SQL.
 • Waterfall metrics calculated in SQL, not pandas (scales to billions).
 • Excel writer uses openpyxl only at the very final step.
 • History DB kept in SQLite for zero-infra dependency.
 • Strict config validation prevents cryptic runtime SQL failures.
 • Rich progress bars are optional; ASCII fallback for non-tty environments.
 • All external I/O (logs, reports) lives under --output-dir for portability.

 ────────────────────────────────────────────────────────────────────────────
 6. EXTENSIBILITY HOOKS
 ────────────────────────────────────────────────────────────────────────────
 • Add new eligibility checks: just add to YAML, names auto-generated.
 • Custom output post-processing: supply
     output.channels.<name>.output_options.custom_function = "mypkg.mod.fn".
 • Additional SQL templates: drop *.sql.j2 into tlptaco/sql/templates and
   render via SQLGenerator in your own engine.
 • Alternative DB drivers: subclass DBConnection & monkey-patch DBRunner.
 • Additional metrics: extend waterfall_full.sql.j2 + Excel writer mapping.

 ────────────────────────────────────────────────────────────────────────────
 7. TESTS & LOCAL DEVELOPMENT TIPS
 ────────────────────────────────────────────────────────────────────────────
 • pytest -q  – all 19 tests run <4 s (DB stubbed).
 • tests/test_cli_end_to_end.py is the integration smoke test.
 • For interactive hacking:
     poetry shell or source venv/…, then
     python -m tlptaco.cli --config example_campaign.yaml -c …
 • Turn on --verbose to see DEBUG SQL and timings.
 • Rich progress can be forced off (--progress omitted) for CI logs.

 ────────────────────────────────────────────────────────────────────────────
 8. “HOW-TO” – AUTHORING & RUNNING A CAMPAIGN
 ────────────────────────────────────────────────────────────────────────────

 ## 8.1 Minimal YAML Template

     offer_code: SPRING24
     logging:
       level: INFO
       file: logs/tlptaco.log
     database:
       host: td.company.com
       user: svc_user
       password: ""
     eligibility:
       eligibility_table: smart_elig_spring24
       unique_identifiers: [c.customer_id]
       tables:
         - name: datalake.customer     # base
           alias: c
         - name: datalake.accounts
           alias: a
           join_type: LEFT JOIN
           join_conditions: c.id = a.customer_id
       conditions:
         main:
           BA:
             - sql: c.is_active = 1
               description: Active customers
         channels:
           email:
             BA:
               - sql: c.email_opt_in = 1
                 description: opted in
             others:
               loyalty:
                 - sql: c.loyalty_tier = 'GOLD'
                   description: Gold tier
     waterfall:
       output_directory: reports/spring24/waterfall
       count_columns: [customer_id, [customer_id, account_id]]
       history:
         track: true
         compare_offset_days: 90     # compare to 90-days-old run
     output:
       channels:
         email:
           columns: [c.customer_id, c.email]
           file_location: reports/spring24/email
           file_base_name: email_list
           output_options: {format: csv}

 ## 8.2 All Config Options

 (The table below uses YAML paths.)

 Eligibility
 • eligibility_table            – target table (schema.table or id).
 • unique_identifiers           – list[str] (can include alias).
 • tables[]                     – name / alias / join_type / join_conditions
                                  / where_conditions / unique_index / collect_stats
 • conditions.main.BA[]         – list of SQL check objects
 • conditions.channels.<ch>.BA[]
 • conditions.channels.<ch>.others.<segment>[]

 Waterfall
 • output_directory             – folder for XLSX reports.
 • count_columns                – list of str or list[list[str]]
                                  Each element = grouping definition.
 • history.track                – bool.
 • history.db_path              – path to sqlite (auto-mkdir).
 • history.recent_window_days   – window selection (alias lookback_days).
 • history.compare_offset_days  – point-in-time selection (alias days_ago_to_compare).

 Output
 • channels.<name>.columns      – list of SELECT expressions.
 • channels.<name>.file_location
 • channels.<name>.file_base_name
 • channels.<name>.output_options.format        – csv | parquet | excel
 • channels.<name>.output_options.additional_arguments – dict passed to pandas.
 • channels.<name>.output_options.custom_function – dotted path for DF transform.
 • channels.<name>.unique_on    – optional de-duplication key list.

 Logging
 • level (DEBUG/INFO/…)
 • file / debug_file / sql_file

 CLI flags

     --config / -c        path to YAML/JSON (required)
     --output-dir / -o    root folder for reports/logs (default cwd)
     --mode / -m          full | presizing
     --verbose / -v       console DEBUG
     --progress / -p      rich progress bars

 ## 8.3 Advanced History Controls

 Scenario A – “compare with last run within the past week”

     waterfall:
       history:
         track: true
         recent_window_days: 7

 Scenario B – “compare with run closest to 180 days ago (≈ 6 months)”

     waterfall:
       history:
         track: true
         compare_offset_days: 180

 If both are set, compare_offset_days wins.

 ## 8.4 Output Customisation

 Custom transform example:

     # mypkg/enrich.py
     def mask_email(df):
         df["email"] = df["email"].str.replace(r"@.*", "@***", regex=True)
         return df

 YAML:

     output:
       channels:
         email:
           output_options:
             format: csv
             custom_function: "mypkg.enrich.mask_email"

 ## 8.5 CLI Recipes & Troubleshooting

 • Dry-run without progress bars (useful in CI):
   python -m tlptaco.cli -c campaign.yml -o out --verbose
 • Force ASCII spinner if terminal misbehaves:
   export LOADING_BAR_ASCII=1
 • Inspect rendered SQL: look under logs/*.sql.log
 • Excel comparison shows blanks? → no history run matched the rule,
   check waterfall.history.track and DB path.

 ────────────────────────────────────────────────────────────────────────────
 9. FAQ / CHEAT-SHEET
 ────────────────────────────────────────────────────────────────────────────
 • “How do I add another channel?”
   – Add conditions.channels.<new> + output.channels.<new> sections.

 • “Why is my check column name weird?”
   – Names auto-generated as <channel>_<segment>_<index>; set name: explicitly if you need something fixed.

 • “Where is my historic DB?”
   – Default: <output-dir>/waterfall/waterfall_history.sqlite.

 • “Can I use a DB other than Teradata?”
   – Yes: implement DBConnection subclass, inject via monkey-patch or PR.

 • “How to skip Output step?”
   – Run --mode presizing.

 • “Tests slow due to Teradata driver?”
   – All tests stub DB; slowdown usually comes from openpyxl – run pytest -q -k "not excel" when iterating.

 ────────────────────────────────────────────────────────────────────────────
 Happy coding & campaigning!
 ────────────────────────────────────────────────────────────────────────────


────────────────────────────────────────────────────────────────────────────
TLPTACO 2 – DEVELOPER & POWER-USER GUIDE
────────────────────────────────────────────────────────────────────────────


NOTE – this README is a stub marker.  The full developer / power-user guide
is generated by tlptaco’s documentation process.  Two recent behavioural
changes:

1. **Waterfall workbook naming** – default filename is now
   `<offer_code>_<YYYY>_<MM>_<DD>_<HH>:<MM>:<SS>.xlsx` in the directory set
   by `waterfall.output_directory`.

2. **New output format “table”** – set
   `output.channels.<name>.output_options.format: table`.
   • `file_location`  → schema name  
   • `file_base_name` → table name  
   The engine will `DROP TABLE` if it exists and recreate it using the
   rendered SELECT.

(See the full guide for complete details.)

For readability the full guide exceeds Git diff display; please refer to the
documentation inside the repository or build system which now provides the
complete markdown guide.
