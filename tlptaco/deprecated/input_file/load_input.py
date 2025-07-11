from tlptaco.connections.teradata import TeradataHandler
from tlptaco.logging.logging import CustomLogger, call_logger
import pandas as pd


class FileLoader:
    def __init__(self, file_path: str, file_type: str, file_kwargs: dict, table_name: str, schema_name: str,
                 teradata_connection: TeradataHandler, logger: CustomLogger):
        self.file_path = file_path
        self.file_type = file_type.lower()
        self.file_kwargs = file_kwargs
        self.table_name = table_name
        self.schema_name = schema_name
        self.teradata_connection = teradata_connection
        self.logger = CustomLogger

        self.df = None
        self.fastload_kwargs = {
            'table_name': self.table_name,
            'schema_name': self.schema_name
        }

    def _read_file(self):
        if self.file_type == 'csv':
            self.df = pd.read_csv(self.file_path, **self.file_kwargs)
        elif self.file_type == 'parquet':
            self.df = pd.read_parquet(self.file_path, **self.file_kwargs)
        elif self.file_type == 'xlsx':
            self.df = pd.read_excel(self.file_path, **self.file_kwargs)

    def load_file(self, create_table_query=None):
        self.fastload_kwargs.update(self.file_kwargs)
        if self.df is None:
            self._read_file()

        if create_table_query is not None:
            self.teradata_connection.execute_query(create_table_query)
            self.fastload_kwargs['if_exists'] = 'append'
            self.teradata_connection.fastload(self.df, **self.fastload_kwargs)
        else:
            self.teradata_connection.fastload(self.df, **self.fastload_kwargs)