from datetime import datetime  # 'datetime.datetime' imported but unused
import pandas as pd
from tlptaco.connections.teradata import TeradataHandler
from tlptaco.logging.logging import call_logger, CustomLogger
from tlptaco.waterfall.waterfall import Waterfall
from tlptaco.eligibility.eligibility import Eligible
import os
import inspect
from typing import Callable


class Output:
    def __init__(
            self,
            sqlconstructor,
            logger: CustomLogger,
            teradata_connection: TeradataHandler = None
    ):
        self.logger = logger
        self.teradata_connection = teradata_connection
        self.sqlconstructor = sqlconstructor
        # initialize output queries
        self._output_instructions = None
        self._output_queries = {}

    @property
    def output_instructions(self):
        return self._output_instructions

    @output_instructions.setter
    def output_instructions(self, output_instructions: dict[str:dict[str:str]]) -> None:
        """
        File structure should be:
        {
            'channel_name': {
                'sql': 'PLACE SQL HERE',
                'file_location': '/path/to/file', <- does not end with a forward slash
                'file_base_name': 'base_name_without_file_extension',
                'output_options': {
                    'format': 'parquet', 'csv', 'excel' <- only three options
                    'additional_arguments': {...} <-- these must match the python arguments for pandas
                                                     pandas.to_parquet, pandas.to_csv, pandas.to_excel
                },
                'custom_function': python_function <-- this is a custom function that
                                                       accepts a dataframe and returns a dataframe
            },
        }

        Make sure that your sql uses the FROM statement: "FROM eligibility_table" (use any alias for joins)
        For example:
            SELECT ...
            FROM eligibility_table a
            LEFT JOIN some_other_table b
                ON a.some_column = b.some_column

        :param output_instructions:
        :return:
        """
        self._output_instructions = output_instructions

    @classmethod
    def from_waterfall(cls, waterfall: Waterfall) -> "Output":
        """
        Load the details from an instance of waterfall

        :param waterfall: the instance of waterfall that you want to read in
        :type waterfall: tlptaco.waterfall.waterfall.Waterfall
        :return: an instance of output
        :rtype: tlptaco.output.output.Output
        """
        return Output(
            waterfall._sqlconstructor,
            waterfall.logger,
            waterfall._teradata_connection
        )

    @classmethod
    def from_eligibility(cls, eligibility: Eligible) -> "Output":
        """
        Load the details from an instance of eligibility

        :param eligibility: the instance of eligibility that you want to read from
        :type eligibility: tlptaco.eligibility.eligibility.Eligibility
        :return:
        :rtype: tlptaco.output.output.Output
        """
        return Output(
            eligibility._sqlconstructor,
            eligibility.logger,
            eligibility._teradata_connection
        )

    def _create_channel_eligibility(self, channels):
        """
        DEPRECATED
        Create the SQL output queries from the output criteria
        :param channels:
        :return:
        """
        self.sqlconstructor.output_file.generate_output_queries(channels)

    def _call_custom_function(self, custom_function: Callable, df: pd.DataFrame) -> pd.DataFrame:
        """
        If the user defined custom functions, pass the pd.DataFrame through the functions

        :param custom_function: the custom function to alter the output file
        :type custom_function: Callable
        :param df: the pandas dataframe to alter
        :type df: pd.DataFrame
        :return: the altered DataFrame
        :rtype: pd.DataFrame
        """
        signature = inspect.signature(custom_function)
        if 'logger' in signature.parameters:
            return custom_function(df=df, logger=self.logger)
        else:
            return custom_function(df=df)

    @call_logger()
    def create_output_file(self, save_file=True, return_details: bool = False) -> None | dict:
        """
        Output the file for the specified channel(s)

        :param save_file: Tells the script to save the file or not.
        :type save_file: bool
        :param return_details: Tells the script if it should return the details of the file
        :type return_details: bool
        :return if return_details is True, then it will return the details fo the saved file
        """
        # extract just the queries from the channels
        for channel, details in self.output_instructions.items():  # TODO: add metaclass check to make sure output_instructions is not null on call of this function
            channel_sql: str = details.get('sql')
            # if the file ends in .sql, this means they provided a sql file; read it
            if channel_sql.lower().endswith('.sql'):
                with open(channel_sql, 'r') as f:
                    channel_sql = f.read()
            self._output_queries[channel] = channel_sql  # blank line contains whitespace

        self.sqlconstructor.output_queries = self._output_queries
        queries: dict = self.sqlconstructor.output_file.generate_output_sql()

        for channel, query in queries.items():
            self.logger.debug(f"--{self.__class__}.create_output_file\n--Output SQL for {channel}\n%s;", query)
            if self.teradata_connection is not None:
                try:
                    df = self.teradata_connection.fastexport(query)
                finally:
                    self.logger.error(f"There was an issue with the output SQL for {channel}")

                custom_functions = self._output_instructions.get(channel).get('output_options').get('custom_function')
                if custom_functions is not None:
                    # included if statement to maintain compatibility with campaigns that did not put functions in lists
                    if callable(custom_functions):
                        self.logger.info(
                            f"{self.__class__}.create_output_file running custom function: \"{custom_functions.__name__}\"")
                        df = self._call_custom_function(custom_functions, df)
                    else:
                        for custom_function in custom_functions:
                            self.logger.info(
                                f"{self.__class__}.create_output_file running custom function: \"{custom_function.__name__}\"")
                            df = self._call_custom_function(custom_function, df)

                self.logger.info(f"{self.__class__}.create_output_file number of records: {len(df)}")
                if save_file:
                    file_name = self._save_output_file(df, channel)
                else:
                    file_name = None

                if return_details:
                    return_values = {'data': df}
                    if file_name is None:
                        return_values['filename'] = ''
                        return return_values
                    else:
                        return_values['filename'] = file_name
                        return return_values

    @call_logger()
    def _save_output_file(self, df: pd.DataFrame, channel: str) -> str:
        """
        Saves down output file data located in a pandas dataframe

        :param df: dataframe to save
        :param channel: channel corresponding to the dataframe
        :return: str
        """
        file_location = self._output_instructions.get(channel).get('file_location')
        output_options = self._output_instructions.get(channel).get('output_options')
        file_extension = output_options.get('format')

        base_file_name: str = self._output_instructions.get(channel).get('file_base_name')
        file_name: str = f'{file_location}/{base_file_name}.{file_extension}'
        end_file_name: str = f'{file_location}/{base_file_name}.end'

        if os.path.exists(file_name):
            self.logger.warning(
                f"{self.__class__}._save_output_file {file_name} already exists; overwriting existing file")

        self.logger.info(f"{self.__class__}._save_output_file {file_location=}")
        self.logger.info(f"{self.__class__}._save_output_file {file_extension=}")
        self.logger.info(f"{self.__class__}._save_output_file {file_name=}")
        self.logger.info(f"{self.__class__}._save_output_file {end_file_name=}")
        self.logger.info(f"{self.__class__}._save_output_file {output_options=}")

        if file_extension == 'csv' or file_extension == 'txt':  # TODO: add validation check for channel arguments
            df.to_csv(file_name, index=False, **output_options.get('additional_arguments'))
            self.logger.info("Saved output in csv format")
        elif file_extension == 'excel' or file_extension == 'xlsx':
            df.to_excel(file_name, index=False, **output_options.get('additional_arguments'))
            self.logger.info("Saved output as excel")
        elif file_extension == 'parquet':
            df.to_parquet(file_name, index=False, **output_options.get('additional_arguments'))
            self.logger.info("Saved output as parquet")
        elif file_extension == 'pickle':
            df.to_pickle(file_name, **output_options.get('additional_arguments'))
        elif file_extension == 'json':
            df.to_json(file_name, **output_options.get('additional_arguments'))
        elif file_extension == 'feather':
            df.to_feather(file_name, **output_options.get('additional_arguments'))

        # write the .end file
        with open(end_file_name, 'w') as f:
            f.write(str(len(df)))

        # ensure the files have 770 permissions
        os.chmod(file_name, mode=0o0770)
        os.chmod(end_file_name, mode=0o0770)

        return file_name

# blank line at end of file