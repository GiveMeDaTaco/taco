"""
Pydantic models for tlptaco configuration.
"""
from pydantic import BaseModel
from typing import List, Dict, Optional, Any, Union

# TODO check the format for other templates besides BA

class ConditionCheck(BaseModel):
    name: str
    sql: str
    description: Optional[str]
    pass_all_prior: Optional[bool] = False
    fail_all_prior: Optional[bool] = False

class TemplateConditions(BaseModel):
    BA: List[ConditionCheck]
    others: Optional[Dict[str, List[ConditionCheck]]]

class ConditionsConfig(BaseModel):
    main: TemplateConditions
    channels: Dict[str, TemplateConditions]

class TableConfig(BaseModel):
    name: str
    alias: str
    sql: str
    join_type: Optional[str]
    join_conditions: Optional[str]
    where_conditions: Optional[str]
    unique_index: Optional[str]
    collect_stats: Optional[List[str]]

class EligibilityConfig(BaseModel):
    # Table name to use for eligibility results
    eligibility_table: str
    conditions: ConditionsConfig
    tables: List[TableConfig]
    unique_identifiers: List[str]

class WaterfallConfig(BaseModel):
    output_directory: str
    # List of identifier(s) to group by: each entry is either a column str or list of column strs
    count_columns: List[Union[str, List[str]]]

class OutputOptions(BaseModel):
    format: str
    additional_arguments: Optional[Dict[str, Any]] = {}
    custom_function: Optional[str]

class OutputChannelConfig(BaseModel):
    sql: str
    file_location: str
    file_base_name: str
    output_options: OutputOptions
    # columns to enforce uniqueness on (subset of selected columns)
    unique_on: Optional[List[str]] = []

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

class AppConfig(BaseModel):
    logging: LoggingConfig
    database: DatabaseConfig
    eligibility: EligibilityConfig
    waterfall: WaterfallConfig
    output: OutputConfig