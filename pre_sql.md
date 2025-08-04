# Pre-SQL Execution Feature

The **pre_sql** feature lets you run one or more plain‐text `.sql` files *before*
tlptaco executes the Eligibility → Waterfall → Output pipeline.  Use it to
create staging tables, refresh dimensions, set session options, etc.

## Configuration

Add a top-level list to your YAML/JSON config:

```yaml
pre_sql:
  - sql/setup_temp_tables.sql   # executed first
  - sql/refresh_dim_items.sql   # executed second
```

Rules & behaviour
* **Optional** – if the key is absent (or an empty list) nothing happens.
* Paths can be absolute or relative to the working directory.
* Each file is read, naïvely split on the `;` character, and each resulting
  statement is executed *in order* via the pipeline’s `DBRunner`.
* Any error raised by the database stops the pipeline immediately—identical to
  the error handling used for Eligibility/Waterfall SQL.

## Progress-bar Integration

When you run the CLI with `--progress` the pre-SQL phase appears as an
additional layer named **SQL Statements**.  The bar tracks the exact number of
individual statements (not files) being executed.

## Logging

Each source file is dumped to the dedicated SQL log (if configured) under a
section header:

```
################################################################################
# PRE-SQL sql/setup_temp_tables.sql
################################################################################
-- file contents …
```

This makes it easy to review or re-run the set-up scripts outside tlptaco.

## Tips

* Keep the files *pure* SQL; no Jinja templating or variables are substituted.
* To ensure the splitter works reliably, terminate each statement with a `;`
  on its own or followed by whitespace/newline.
* If you need complex procedural code containing semicolons (e.g., BTEQ,
  PL/SQL) consider wrapping it in a single‐statement shell script or run it
  separately before invoking tlptaco.
