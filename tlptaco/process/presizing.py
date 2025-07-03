from typing import List, Dict
from tlptaco.logging.logging import CustomLogger
from tlptaco.eligibility.eligibility import Eligible
from tlptaco.waterfall.waterfall import Waterfall
from tlptaco.connections.teradata import TeradataHandler
import getpass
from collections import OrderedDict
import time

class Presizing:
    """
    Provides a pipeline to create eligibility and a waterfall WITHOUT generating an output file.
    Default uses Kerberos to connect to Teradata. LDAP is available, but is not extensively tested.
    """
    def __init__(self, conditions: OrderedDict, tables: Dict, offer_code: str, campaign_planner: str, lead: str,
                 waterfall_count_columns: List, waterfall_location: str, teradata_database: str = 'rchtera',
                 log_file: str = None, debug_log_file: str = None, teradata_conn_method: str = 'KRB5', teradata_password: str = None):
        """
        Initializes Presizing

        :param conditions: conditions for eligibility
        :type conditions: OrderedDict
        :param tables: tables used in eligibility
        :type tables: Dict
        :param offer_code: offer code for presizing
        :type offer_code: str
        :param campaign_planner: campaign planner
        :type campaign_planner: str
        :param lead: lead
        :type lead: str
        :param waterfall_count_columns: the columns from your tables you want counts on in your waterfall
        :type waterfall_count_columns: list, i.e. ['a.pty_id', 'a.pty_id, b.accno', 'b.ml_id']
        :param waterfall_location: directory where you want to store the waterfall
        :type waterfall_location: str
        :param teradata_database: the teradata database you want to connect to
        :type teradata_database: str, one of {'rchtera', 'aprtera'}
        :param log_file: the path and filename where you'd like to store your log file; if nothing is provided, no log is saved
        :type log_file: str, i.e '/listprod/execution/sb/some_log.log'
        :param debug_log_file: the path and filename where you'd like to store any SQL that is run; if nothing is provided, no file is saved
        :type debug_log_file: str, i.e. '/listprod/execution/sb/some_file.sql'
        :param teradata_conn_method: the method you'd like to use to connect to Teradata (password or Kerberos); Kerberos is default and preferred
        :type teradata_conn_method: str, one of {'KRB5', 'LDAP'}
        :param teradata_password: if you put teradata_conn_method as LDAP, then you must provide this password; please DO NOT write it in plain text; use getpass.getpass() or some other method to capture and pass the password
        :type teradata_password: str
        """
        self.logger: CustomLogger = CustomLogger(offer_code, log_file=log_file, debug_file=debug_log_file)
        self.username = getpass.getuser()
        self.conditions = conditions
        self.tables = tables
        self.offer_code = offer_code
        self.campaign_planner = campaign_planner
        self.lead = lead
        self.waterfall_location = waterfall_location
        self.waterfall_count_columns = waterfall_count_columns

        if teradata_conn_method == 'KRB5':
            self.teradata_connection = TeradataHandler(self.logger, teradata_database, self.username, '1')
        elif teradata_conn_method == 'LDAP':
            self.teradata_connection = TeradataHandler(self.logger, teradata_database, self.username, teradata_password, logmech='LDAP')

        self.eligible = None
        self.waterfall = None

    def generate_presizing(self):
        start_time = time.time()
        try:
            self.eligible = Eligible(self.campaign_planner, self.lead, self.username, self.offer_code, self.conditions,
                                     self.tables, self.waterfall_count_columns,
                                     self.logger, self.teradata_connection)
            self.waterfall = Waterfall.from_eligible(self.eligible, self.waterfall_location)
            self.eligible.generate_eligibility()
            self.waterfall.generate_waterfall()
        finally:
            self.teradata_connection.cleanup()
            end_time = time.time()
            elapsed_time = end_time - start_time
            self.logger.info(f"Time taken to complete {self.offer_code}: {elapsed_time}")

            # this formats the debug log file (contains the sql code)
            self.logger.format_debug_file()