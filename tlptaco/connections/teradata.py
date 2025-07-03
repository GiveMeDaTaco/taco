import teradataml
from tlptaco.logging.logging import CustomLogger, call_logger
from packaging import version
from typing import Dict
import pandas as pd
from tlptaco.clean_up.clean_up import TrackSQL


class TeradataHandler:
    """
    Handles the creation of a Teradata connection, SQL execution, data exports, tracking tables created
    as a part of the process, and cleaning up Teradata when the process is finished
    """

    def __init__(self, logger: CustomLogger, host: str, user: str, password: str = 'placeholder',
                 logmech: str = 'KRB5'):
        """
        :param logger: the instance of CustomLogger that is used to logging during the process
        :param host: the Teradata instance to connect to (i.e. "rchtera.bankofamerica.com")
        :param user: the NBK used to connect to Teradata
        :param password: password for logging into Teradata (if logmech="KRB5", you can leave password as "placeholder")
        :param logmech: the method used to log into Teradata (i.e. "KRB5" = Kerberos, LDAP = password)
        """
        self.logger = logger
        self.host = host
        self.user = user
        self.password = password
        self.context = None
        self.connection = None
        self.logmech = logmech
        self.tracking = TrackSQL(self, logger)
        self.teradataml_version = teradataml.__version__

    @call_logger()
    def connect(self):
        """
        Creates a connection to Teradata
        """
        self.context = teradataml.create_context(
            host=self.host,
            username=self.user,
            password=self.password,
            logmech=self.logmech
        )
        self.connection = teradataml.get_connection()

    @call_logger()
    def disconnect(self):
        """
        Removes connection from Teradata
        """
        if self.context:
            teradataml.remove_context()
            self.context = None

    @call_logger()
    def execute_query(self, query: str):
        """
        Executes a query using Teradata connection. If the connection does not exist, it creates one with the
        information provided when the instance of the class was initiated (i.e. __init__)
        """
        if not self.context:
            self.connect()
        # Check the teradataml version
        if version.parse(self.teradataml_version) > version.parse("17.20.0.03"):
            return teradataml.execute_sql(query)
        else:
            return self.connection.execute(query)

    @call_logger()
    def to_pandas(self, query: str):
        """
        Exports data with SQL query directly to pandas DataFrame; Teradata recommends using this for
        datasets with less than 1,000,000 rows

        :param query: SQL query used to export data
        """
        if not self.context:
            self.connect()
        tf = teradataml.DataFrame.from_query(query)
        return tf.to_pandas()

    @call_logger()
    def fastexport(self, query: str):
        """
        Uses a SQL query and Teradata's FastExport utility to export data to a pandas DataFrame;
        Teradata recommends using this for datasets with 1,000,000 or more
        """
        if not self.context:
            self.connect()
        tf = teradataml.DataFrame.from_query(query)
        return teradataml.fastexport(tf, catch_errors_warnings=False)

    @call_l_logger()
    def fastload(self, df: pd.DataFrame, fastload_kwargs: Dict):
        """
        Loads a pandas DataFrame to a table on Teradata

        :param df: DataFrame to load
        :param fastload_kwargs: a dictionary that contains the arguments that will be passed to teradataml.fastload
        """
        if not self.context:
            self.connect()
        return teradataml.fastload(df, **fastload_kwargs)

    @call_logger()
    def cleanup(self):
        """
        Triggers a mass drop of all the tables that were tracked during the process
        """
        # make sure your Teradata connection is active
        if not self.context:
            self.connect()
        # attempt to clean up tracked tables
        try:
            self.tracking.clean_up()
        except Exception as e:
            self.logger.info(f"An error occurred during cleanup: {e}")
        finally:
            # remove connection to the database (since we assume this is the last thing you want to do with your connection)
            self.disconnect()