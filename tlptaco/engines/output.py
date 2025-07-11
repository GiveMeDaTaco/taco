"""
Output engine: exports final data to files with optional transforms.
"""
from tlptaco.config.schema import OutputConfig
from tlptaco.db.runner import DBRunner
from tlptaco.utils.logging import get_logger
from tlptaco.io.writer import write_dataframe

class OutputEngine:
    def __init__(self, cfg: OutputConfig, runner: DBRunner, logger=None):
        self.cfg = cfg
        self.runner = runner
        self.logger = logger or get_logger("output")
        
    def num_steps(self) -> int:
        """Return the number of output channels to process."""
        return len(self.cfg.channels)

    def run(self, eligibility_engine, progress=None):
        """
        Execute each channel's output SQL, fetch results as DataFrame,
        apply any custom transformations, and write to file.
        """
        import importlib
        elig_table = eligibility_engine.cfg.eligibility_table
        for channel, out_cfg in self.cfg.channels.items():
            # Prepare SQL
            sql_raw = out_cfg.sql
            if sql_raw.lower().endswith('.sql'):
                with open(sql_raw, 'r') as f:
                    sql_raw = f.read()
            sql = sql_raw.format(eligibility_table=elig_table)
            self.logger.info(f"Running output SQL for channel {channel}")
            self.logger.debug(sql)
            # Fetch data
            df = self.runner.to_df(sql)
            # Enforce uniqueness if specified
            unique_cols = out_cfg.__dict__.get('unique_on') or []
            if unique_cols:
                missing = [c for c in unique_cols if c not in df.columns]
                if missing:
                    self.logger.warning(f"Cannot unique on missing columns {missing}")
                else:
                    df = df.drop_duplicates(subset=unique_cols)
            self.logger.info(f"Fetched {len(df)} rows for channel {channel}")
            # Apply custom function if provided
            cf = out_cfg.output_options.custom_function
            if cf:
                module_name, fn_name = cf.rsplit('.', 1)
                mod = importlib.import_module(module_name)
                func = getattr(mod, fn_name)
                self.logger.info(f"Applying custom function {fn_name} to channel {channel}")
                df = func(df)
            # Write file
            fmt = out_cfg.output_options.format
            path = f"{out_cfg.file_location}/{out_cfg.file_base_name}.{fmt}"
            self.logger.info(f"Writing output file for channel {channel} to {path}")
            write_dataframe(df, path, fmt, **(out_cfg.output_options.additional_arguments or {}))
            # Log file write details
            try:
                rows, cols = df.shape
                self.logger.info(f'Output file saved to {path} ({rows} rows, {cols} columns)')
            except Exception:
                self.logger.info(f'Output file saved to {path}')
            # Update progress bar for this channel
            if progress:
                progress.update("Output")
            # Update progress bar for this channel
            if progress:
                progress.update("Output")