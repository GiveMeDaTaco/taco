from tlptaco.validations.eligibility import EligibleValidator
from tlptaco.validations.general import BaseValidator
from tlptaco.connections.teradata import TeradataHandler
from tlptaco.construct_sql.construct_sql import SQLConstructor
from tlptaco.logging.logging import call_logger, CustomLogger
from collections import OrderedDict
from typing import Dict, Callable, List


class Eligible(EligibleValidator, BaseValidator):
    """
    A class to handle eligibility operations in a campaign, using SQL generation and Teradata connections.

    Attributes:
        campaign_planner (str): The campaign planner.
        lead (str): The lead person.
        username (str): The username.
        offer_code (str): The offer code.
        conditions (OrderedDict): The conditions for eligibility.
        tables (dict): The tables involved in the eligibility check.
        unique_identifiers (list): Unique identifiers used in the eligibility check.
        _teradata_connection (TeradataHandler): The Teradata connection handler.
        _sqlconstructor (SQLConstructor): An instance of SQLConstructor to build SQL queries.
        _validators (Dict[str, Callable]): a dictionary with each of the attributes that can be set in the
            class + the function used to validate the entry
    """
    _validators: Dict[str, Callable] = {
        'campaign_planner': EligibleValidator.validate_campaign_planner,
        'lead': EligibleValidator.validate_lead,
        'username': EligibleValidator.validate_username,
        'offer_code': EligibleValidator.validate_offer_code,
        'conditions': EligibleValidator.validate_conditions,
        'tables': EligibleValidator.validate_tables,
        'unique_identifiers': EligibleValidator.validate_unique_identifiers,
        'logger': BaseValidator.validate_logger,
        '_teradata_connection': BaseValidator.validate_teradata_connection,
        '_sqlconstructor': BaseValidator.validate_sqlconstructor
    }

    def __init__(
            self,
            campaign_planner: str,
            lead: str,
            username: str,
            offer_code: str,
            conditions: OrderedDict,
            tables: Dict,
            unique_identifiers: List,
            logger: CustomLogger,
            teradata_connection: TeradataHandler or None = None
    ):
        """
        Initializes the class

        :param campaign_planner: Name of the campaign planner
        :type campaign_planner: str
        :param lead: Name of the developer
        :type lead: str
        :param username: NBK of the person running the script
        :type username: str
        :param offer_code: Offer Code
        :type offer_code: str
        :param conditions: Conditions that each entity must pass to be eligible
        :type conditions: OrderedDict
        :param tables: Tables that the conditions pull from
        :type tables: Dict
        :param unique_identifiers: The unique identifiers that will be used to generate counts for the
            waterfall and the identifiers what you need available for the output file
        :type unique_identifiers: List
        :param logger: the logger for logging
        :type logger: CustomLogger
        :param teradata_connection: Teradata connection to use for the process
        :type teradata_connection: TeradataHandler or None
        """
        self.logger = logger
        self.campaign_planner = campaign_planner
        self.lead = lead
        self.username = username
        self.offer_code = offer_code

        # this will trigger the @setters for each of these variables below
        self.conditions = conditions
        self.tables = tables
        self.unique_identifiers = unique_identifiers

        # prep SQLConstructor property
        self._sqlconstructor = SQLConstructor(self.conditions, self.tables, self.unique_identifiers, self.username,
                                              self.logger)
        self._teradata_connection = teradata_connection

    @property
    def campaign_planner(self):
        """Getter for campaign_planner."""
        return self._campaign_planner

    @campaign_planner.setter
    def campaign_planner(self, value):
        """Setter for campaign_planner."""
        self._campaign_planner = value

    @property
    def lead(self):
        """Getter for lead."""
        return self._lead

    @lead.setter
    def lead(self, value):
        """Setter for lead."""
        self._lead = value

    @property
    def username(self):
        """Getter for username."""
        return self._username

    @username.setter
    def username(self, value):
        """Setter for username."""
        self._username = value

    @property
    def offer_code(self):
        """Getter for offer_code."""
        return self._offer_code

    @offer_code.setter
    def offer_code(self, value):
        """Setter for offer_code."""
        self._offer_code = value

    @property
    def conditions(self):
        """Getter for conditions."""
        return self._conditions

    @conditions.setter
    def conditions(self, conditions):
        """Setter for conditions."""
        self._conditions = conditions

    @property
    def tables(self):
        """Getter for tables."""
        return self._tables

    @tables.setter
    def tables(self, tables):
        """Setter for tables."""
        self._tables = tables

    @property
    def unique_identifiers(self):
        """Getter for unique_identifiers."""
        return self._unique_identifiers

    @unique_identifiers.setter
    def unique_identifiers(self, unique_identifiers):
        """Setter for unique_identifiers."""
        self._unique_identifiers = unique_identifiers

    @call_logger()
    def _create_work_tables(self):
        """
        Creates work tables based on the SQL generated by the SQLConstructor.
        Executes the SQL queries to create the work tables and track them.
        """
        work_queries = self._sqlconstructor.eligible.generate_work_table_sql()
        for query in work_queries:
            work_sql = query.get('query')
            collect_sql = query.get('collect_query')
            table_name = query.get('table_name')

            # only attempt to execute if the Teradata connection isn't set
            if self._teradata_connection is not None:
                if work_sql:
                    self.logger.info(f"{self.__class__}._create_work_tables created work table {table_name}")
                    self.logger.debug(f"\n--{self.__class__}._create_work_tables\n--SQL for {table_name}\n%s;\n",
                                      work_sql)
                    self._teradata_connection.execute_query(work_sql)
                    self._teradata_connection.tracking.track_table(table_name)
                if collect_sql:
                    for sql in collect_sql:
                        self.logger.info(f"{self.__class__}._create_work_tables collected statistics on {table_name}")
                        self.logger.debug(f"\n--{self.__class__}._create_work_tables\n--Collect SQL;\n%s", sql)
                        self._teradata_connection.execute_query(sql)
            else:
                self.logger.warning(
                    "Eligible._teradata_connection is None, so _create_work_table queries are not executed")

    @call_logger()
    def generate_eligibility(self):
        """
        Generates eligibility by creating work tables and executing the eligibility SQL.
        Executes the main eligibility SQL and tracks the resulting table.
        """
        self._create_work_tables()
        eligibility_query = self._sqlconstructor.eligible.generate_eligible_sql()

        sql = eligibility_query.get('query')
        collect_sql = eligibility_query.get('collect_query')
        table_name = eligibility_query.get('table_name')

        self.logger.debug(f"--{self.__class__}.generate_eligibility {table_name=}")
        self.logger.debug(f"--{self.__class__}.generate_eligibility \n%s;", sql)
        self.logger.debug(f"--{self.__class__}.generate_eligibility \n--Collect SQL\n{collect_sql};")

        if self._teradata_connection is not None:
            self._teradata_connection.execute_query(sql)
            self.logger.info(f"{self.__class__}.generate_eligibility created eligibility in {table_name}")

            self._teradata_connection.tracking.track_table(table_name)

            for query in collect_sql:
                self._teradata_connection.execute_query(query)
                self.logger.info(f"{self.__class__}.generate_eligibility created statistics on {table_name}")
        else:
            self.logger.warning(
                "Eligible._teradata_connection is None, so generate_eligibility queries are not executed")