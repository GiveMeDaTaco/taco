# Failed Records Output Feature

Starting with **tlptaco&nbsp;v2.1** the pipeline can optionally produce a
comprehensive *why-they-failed* dataset that lists every record which did **not**
make it into a final outbound segment **and** the exact reason for that
failure.

## 1. Configuration

Add a new `failed_records` block under the existing `output` section of your
YAML/JSON configuration.

```yaml
output:
  failed_records:
    enabled: true                  # default: false (feature OFF)
    first_reason_only: false       # true = keep only the first failure reason
    file_location: reports/poc/output   # directory or DB schema (see *format*)
    file_base_name: failed_list    # base filename (".csv" / ".parquet" etc.)
    output_options:                # identical structure to channel options
      format: parquet              # csv | excel | parquet | table
      additional_arguments: {}     # forwarded to pandas writer
```

* `enabled` – master switch.  When `false` the pipeline behaves exactly as
  before.
* `first_reason_only` –
  * `false` (default) – emit **one row per failure reason**, so the same
    customer may appear multiple times if they tripped several checks.
  * `true` – restrict to the *first* reason encountered (based on check order
    and channel order).
* `file_location` / `file_base_name` / `output_options` – follow the same
  conventions used by standard channel outputs.  Use `format: table` to write
  directly back to Teradata instead of a file.

## 2. What counts as a *failure*?

1. **Flag Failure** – the record failed any check column (BA or segment).
2. **Un-bucketed** – the record passed main BA **and** the channel-specific BA
   checks but did **not** match any non-BA segment template within that
   channel.

Both cases are gathered into a single dataset with the following columns:

| identifier columns … | `fail_reason`             | `fail_logic` |
|----------------------|---------------------------|--------------|
| (from config)        | e.g. `main_BA_1` or `email_unbucketed` | SQL predicate used |


## 3. Progress Bar & Logging

When progress mode (`--progress`) is enabled the failed-records generation is
tracked as an additional step inside the **Output** layer.  All rendered SQL
is dumped to the dedicated SQL log (if configured) under a `#### FAILED
RECORDS SQL` header.

## 4. Backwards Compatibility

Existing configs remain valid.  If the new block is absent or `enabled:
false`, no extra work is performed and no files/tables are created.
