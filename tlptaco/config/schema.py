"""
Pydantic models for tlptaco configuration with built-in validation.
"""
from pydantic import BaseModel, model_validator
from typing import List, Dict, Optional, Any, Union

# --- Base Models ---

class ConditionCheck(BaseModel):
    name: str
    sql: str
    description: Optional[str]

class TemplateConditions(BaseModel):
    BA: List[ConditionCheck]
    others: Optional[Dict[str, List[ConditionCheck]]]

class ConditionsConfig(BaseModel):
    main: TemplateConditions
    channels: Dict[str, TemplateConditions]

class TableConfig(BaseModel):
    name: str
    alias: str
    sql: Optional[str] # Made optional as it might not always be used
    join_type: Optional[str]
    join_conditions: Optional[str]
    where_conditions: Optional[str]
    unique_index: Optional[str]
    collect_stats: Optional[List[str]]

class OutputOptions(BaseModel):
    format: str
    additional_arguments: Optional[Dict[str, Any]] = {}
    custom_function: Optional[str]

# --- Config Sections with Validation ---

class EligibilityConfig(BaseModel):
    eligibility_table: str
    conditions: ConditionsConfig
    tables: List[TableConfig]
    unique_identifiers: List[str]

    @model_validator(mode='after')
    def check_identifier_aliases_are_valid(self) -> 'EligibilityConfig':
        valid_aliases = {t.alias for t in self.tables}
        for identifier in self.unique_identifiers:
            if '.' in identifier:
                alias = identifier.split('.')[0]
                if alias not in valid_aliases:
                    raise ValueError(
                        f"In 'eligibility.unique_identifiers', '{identifier}' uses an invalid alias '{alias}'. "
                        f"Valid aliases are: {valid_aliases}"
                    )
        return self

class WaterfallConfig(BaseModel):
    output_directory: str
    count_columns: List[Union[str, List[str]]]

class OutputChannelConfig(BaseModel):
    columns: List[str]
    file_location: str
    file_base_name: str
    output_options: OutputOptions
    unique_on: Optional[List[str]] = []

    @model_validator(mode='after')
    def check_unique_on_are_in_columns(self) -> 'OutputChannelConfig':
        if self.unique_on:
            if not set(self.unique_on).issubset(set(self.columns)):
                missing = set(self.unique_on) - set(self.columns)
                raise ValueError(
                    f"'unique_on' columns {missing} are not present in the selected 'columns'."
                )
        return self

class OutputConfig(BaseModel):
    channels: Dict[str, OutputChannelConfig]

class LoggingConfig(BaseModel):
    level: str
    file: Optional[str]
    debug_file: Optional[str]

class DatabaseConfig(BaseModel):
    host: str
    user: str
    password: Optional[str]
    logmech: Optional[str] = "KRB5"

# --- Top-Level App Config with Cross-Section Validation ---

class AppConfig(BaseModel):
    logging: LoggingConfig
    database: DatabaseConfig
    eligibility: EligibilityConfig
    waterfall: WaterfallConfig
    output: OutputConfig

    @model_validator(mode='after')
    def check_cross_config_dependencies(self) -> 'AppConfig':
        # 1. Check WaterfallConfig -> EligibilityConfig dependency
        valid_ids = {uid.split('.')[-1] for uid in self.eligibility.unique_identifiers}
        waterfall_ids = set()
        for item in self.waterfall.count_columns:
            cols = [item] if isinstance(item, str) else item
            for col in cols:
                waterfall_ids.add(col.split('.')[-1])
        if not waterfall_ids.issubset(valid_ids):
            invalid_cols = waterfall_ids - valid_ids
            raise ValueError(
                f"Waterfall 'count_columns' contain invalid identifiers: {invalid_cols}. "
                f"They must be a subset of eligibility 'unique_identifiers': {valid_ids}"
            )

        # 2. Check OutputConfig -> EligibilityConfig dependency (for aliases)
        valid_aliases = {t.alias for t in self.eligibility.tables}
        for channel, out_cfg in self.output.channels.items():
            for column in out_cfg.columns:
                if '.' in column:
                    alias = column.split('.')[0]
                    if alias not in valid_aliases:
                        raise ValueError(
                            f"In output channel '{channel}', column '{column}' uses an invalid alias '{alias}'. "
                            f"Valid aliases are: {valid_aliases}"
                        )
        return self