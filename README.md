# tlptaco v2
Elegant, extensible pipeline for Teradata-driven eligibility, waterfall reporting, and output file generation.

## Overview
`tlptaco` is a command-line tool that:
1. Builds an _eligibility_ table with pass/fail flags for each condition.
2. Generates a _waterfall_ of counts (unique drops, incremental drops, remaining, cumulative drops) entirely in-database.
3. Exports final data files per channel (CSV, Excel, Parquet, etc.) with optional user-supplied transformations.

All configuration is defined in a single YAML or JSON file and validated with Pydantic. SQL is stored in Jinja2 templates. The code is organized into clear, single-responsibility “engine” classes and simple I/O and database layers.

## Features
- **Config-driven**: single file defines conditions, tables, waterfall columns, output channels, logging, and database connection.
- **In-database computation**: waterfall metrics are computed with a single templated SQL query (no row-by-row Python loops).
- **Extensible SQL**: all core SQL lives in `sql/templates/*.sql.j2`, editable without touching Python.
- **Modular engines**: `EligibilityEngine`, `WaterfallEngine`, `OutputEngine` implement clearly separated stages.
- **Flexible output**: write CSV/Excel/Parquet and call user-defined Python functions on the result.
- **Standard logging**: console + file + debug-file; verbose mode for development.

## Installation & Prerequisites
1. **Python 3.8+**
2. Clone repository:
   ```bash
   git clone https://github.com/yourorg/tlptaco.git
   cd tlptaco
   ```
3. Create virtual environment and install dependencies:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

### Required Packages
- `pydantic` (v2) for config validation
- `jinja2` for SQL templating
- `pyyaml` for YAML parsing
- `teradatasql` for Teradata connectivity (replacing `teradataml`)
- `pandas`, `openpyxl` for DataFrame exports
- `pyarrow` (or `fastparquet`) for Parquet support

## Project Structure
```
tlptaco/                         # root package
├─ NEW v2 modules ───────────────────────────────────────────────────
│  ├── cli.py                     # [NEW] entrypoint wiring all engines
│  ├── config/                    # [NEW] Pydantic schemas & config loader
│  │   ├── schema.py
│  │   └── loader.py
│  ├── db/                        # [NEW] DBConnection & runner abstraction
│  │   ├── connection.py
│  │   └── runner.py
│  ├── sql/                       # [NEW] SQLGenerator + Jinja2 templates
│  │   ├── generator.py
│  │   └── templates/
│   │       ├── eligibility.sql.j2
│   │       ├── waterfall_full.sql.j2
│   │       └── output.sql.j2
│  ├── engines/                   # [NEW] core pipeline logic
│   │   ├── eligibility.py
│   │   ├── waterfall.py
│   │   └── output.py
│  ├── io/                        # [NEW] pandas-based I/O helpers
│   │   ├── loader.py
│   │   └── writer.py
│  └── utils/                     # [NEW] logging setup & validation helpers
│      ├── logging.py
│      └── validation.py
│
├─ LEGACY modules (to be deprecated) ─────────────────────────────────
│  ├── clean_up/
│  ├── connections/
│  ├── construct_sql/
│  ├── eligibility/
│  ├── input_file/
│  ├── logging/
│  ├── output/
│  ├── process/
│  ├── tools/
│  ├── validations/
│  └── waterfall/
└── README.md
``` 

## Configuration
- **waterfall.count_columns** may be a list of column strings or lists of column strings:
  e.g. `count_columns: ['c.id', ['c.id','c.group']]` produces two waterfall files.
- **output.channels.*.unique_on**: optional list of output columns to deduplicate on.
Define your process in a single YAML or JSON file. Top-level schema:
- `logging`: level, file, debug_file
- `database`: Teradata host, user, password, logmech
- `eligibility`:
  - `eligibility_table`: name for the output eligibility table
  - `conditions`: main & per-channel conditions (BA + others)
  - `tables`: list of join definitions (`name`, `alias`, `join_type`, `join_conditions`, `where_conditions`)
  - `unique_identifiers`: list of `alias.column` strings
 - `waterfall`:
    - `output_directory`: directory for waterfall reports
    - `count_columns`: list of identifier columns or lists of columns
 - `output`: per-channel output instructions:
    - `sql`: SQL template or file path
    - `file_location`, `file_base_name`: output path
    - `output_options.format`: csv, excel, parquet
    - `output_options.custom_function`: (optional) module.fn to transform DataFrame
    - `unique_on`: (optional) list of columns to drop duplicates by

See `tlptaco/config/schema.py` for full Pydantic definitions.

## SQL Templates
All database logic lives in `sql/templates` as Jinja2 files:
- **eligibility.sql.j2**: creates the eligibility table with per-check flags and collects stats.
-- **waterfall_full.sql.j2**: single CTE + UNION of four SELECTs computing unique, incremental, remaining, cumulative drops.
   Supports optional `pre_filter` context (e.g. to filter by channel section).
- **output.sql.j2**: final per-channel SELECT with CASE…END creating `template_id` column.

Each template declares its expected context variables at the top—update them to match your data model.

## Engines
Each engine accepts its Pydantic config model, a `DBRunner`, and a logger:
1. **EligibilityEngine**: `run()` renders `eligibility.sql.j2`, executes DDL & DML statements.
2. **WaterfallEngine**: `run(eligibility_engine)` renders & executes `waterfall_full.sql.j2`, fetches metrics DF, writes Excel to `waterfall.output_directory`.
3. **OutputEngine**: `run(eligibility_engine)` renders & executes each channel’s `output.sql.j2`, fetches DF, applies custom transformations, writes final files.

## Usage
```bash
python3 -m tlptaco.cli \
  --config path/to/config.yaml \
  [--mode full|presizing] [--verbose]
```

Logs appear on-screen and in configured files.

## Extending Functionality
1. **Add new SQL**: place a Jinja2 template in `sql/templates/`, update engine to render it.
2. **Custom transforms**: set `output_options.custom_function` to `module.fn`, and that function will be applied to the DataFrame.
3. **New DBs**: extend `db/connection.py` & `db/runner.py`.
4. **Add CLI flags**: modify `cli.py`.

## Development & Testing
- Install dev dependencies: `pip install -r requirements-dev.txt`.
- Write tests for: config parsing, template rendering, engine logic (mock `DBRunner`).
- Run `pytest` and use `pre-commit` for linting.
Additional test scripts are provided:
  - `prep_test_db.py`: create or refresh test tables.
  - `test_connection.py`: validate DBRunner/Vantage connectivity.
  - `test_teradatasql.py`: validate direct teradatasql driver connectivity.

## Contributing
Open issues and PRs. Follow code style and add tests.

## License
MIT © Your Organization