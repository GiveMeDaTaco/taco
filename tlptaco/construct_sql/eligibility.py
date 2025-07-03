from typing import Dict, List, Any, Optional  # typing.Optional imported but unused
from tlptaco.validations.construct_sql import EligibilityConstructSQLValidator
from tlptaco.logging.logging import call_logger, CustomLogger


class EligibilitySQLConstructor(EligibilityConstructSQLValidator):
    """
    A class to generate SQL queries for the Eligibility process in eligibility checks.
    This class is handled by the `tlptaco.construct_sql.construct_sql.SQLConstructor` class and is used by `tlptaco.eligibility.eligibility.Eligible`.
    Generates Teradata SQL based on requirements provided by `tlptaco.eligibility.eligibility.Eligible`.

    Attributes:
        conditions (Dict[str, Dict[str, Any]]): Conditions for eligibility checks.
        tables (Dict[str, List[Dict[str, Any]]]): Tables involved in the eligibility process.
        eligibility_table (str): The eligibility table.
        unique_identifiers (Dict[str, List[str]]): Unique identifiers used in the process.
        work_tables (List[Dict[str, Any]]): work tables involved in the eligibility process.
        _eligibility_sql (Optional[Dict[str, str]]): SQL queries for eligibility.

    Methods:
        generate_eligible_sql: generates the SQL to create a table with 1 row per each unique pairing
            of unique_identifiers and 1 column for each condition; a 0 will mean a row doesn't pass that particular
            column and a 1 will mean it does pass
        generate_work_table_sql: generates the SQL to create the user work tables provided by the user
    """
    _validators = {
        'work_tables': EligibilityConstructSQLValidator.validate_work_tables
    }

    def __init__(self, conditions: Dict[str, Dict[str, Any]], tables: List[Dict[str, Any]],
                 work_tables: List[Dict[str, Any]], eligibility_table: str,
                 unique_identifiers: Dict[str, List[str]], logger: CustomLogger) -> None:
        """
        Initializes the EligibilitySQLConstructor class with the provided parameters.

        :param conditions: conditions for eligibility checks
        :type conditions: Dict[str, Dict[str, Any]]
        :param tables: tables that conditions are pulling from
        :type tables: Dict[str, List[Dict[str, Any]]]
        :param work_tables: user work tables defined by the user that are used by the conditions
        :type work_tables: List[Dict[str, Any]]
        :param eligibility_table: the eligibility table
        :type eligibility_table: Dict[str, str]
        :param unique_identifiers: unique identifiers used to identify eligibility and create waterfall counts (i.e. party id)
        :type unique_identifiers: Dict[str, List[str]]
        :param logger: the logger that will be used to log
        :type logger: CustomLogger
        """
        self.logger = logger
        self.conditions = conditions
        self.tables = tables
        self.eligibility_table = eligibility_table
        self.unique_identifiers = unique_identifiers
        self.work_tables = work_tables
        # prep properties
        self._eligibility_sql = None

    @staticmethod
    def _replace_keywords(check_sql: str, previous_checks: List) -> str:
        """
        Checks the provided sql for certain keywords provided by the user and replaces them with the appropriate values.

        :param check_sql: SQL string to check for keywords
        :type check_sql: str
        :param previous_checks: checks conducted prior to the current check
        :type previous_checks: List
        :return: the modified SQL string IF there are any keywords found; otherwise, it's returned unmodified
        :rtype: str
        """
        # 'pass_all_prior' means all checks prior to this must pass; main_BA_1 = 1 AND main_BA_2 = 1, etc.
        if previous_checks and check_sql.find('{pass_all_prior}') != -1:
            addt_sql = [f'{x} = 1' for x in previous_checks]
            check_sql = check_sql.format(pass_all_prior=' AND '.join(addt_sql))

        # 'fail_all_prior' is the same as 'pass_all_prior', only they must all fail
        if previous_checks and check_sql.find('{fail_all_prior}') != -1:
            addt_sql = [f'{x} = 0' for x in previous_checks]
            check_sql = check_sql.format(fail_all_prior=' AND '.join(addt_sql))

        return check_sql

    @call_logger()
    def generate_eligible_sql(self) -> Dict[str, Any]:
        """
        Generates the SQL used to create the eligibility table with the necessary checks.

        :returns: The SQL to create the table used to identify who passes each check, the table name, and the collect statistics query
        :rtype: Dict[str, Any]
        """
        # loop through each check and create CASE statements to identify who passes each check
        # 'previous_checks' is used to hold prior criteria in case a condition has `pass_previous: True`
        # where we need to add passing all previous suppressions to the CASE STATEMENT
        base_previous_checks: List[str] = list()
        select_sql: List[str] = list()
        column_names: List[str] = list()

        previous_channel = str()
        for channel, templates in self.conditions.items():
            base_channel_previous_checks: List[str] = list()
            previous_template = str()
            for template, checks in templates.items():
                template_previous_checks: List[str] = list()
                for check in checks:
                    check_column_name = check.get('column_name')
                    column_names.append(check_column_name)
                    check_sql = check.get('sql')

                    # check for keywords in check_sql to replace
                    previous_checks = base_previous_checks + base_channel_previous_checks + template_previous_checks
                    check_sql = self._replace_keywords(check_sql, previous_checks)

                    select_sql.append(f'CASE WHEN {check_sql} THEN 1 ELSE 0 END AS {check_column_name}')

                    if channel == 'main':
                        base_previous_checks.append(check_column_name)
                    elif channel == previous_channel and template == 'BA':
                        base_channel_previous_checks.append(check_column_name)
                    elif channel == previous_channel and template == previous_template:
                        template_previous_checks.append(check_column_name)
            previous_channel = channel
            previous_template = template

        # loop through tables and create FROM and JOIN statements
        table_sql: List[str] = []
        where_sql: List[str] = []
        for table in self.tables:
            table_name = table.get('table_name')
            table_alias = table.get('alias')
            table_join_conditions = table.get('join_conditions')
            table_where_conditions = table.get('where_conditions')
            table_join_type = table.get('join_type')

            join_condition_sql = f' ON {table_join_conditions}' if table_join_conditions else ''
            table_sql.append(f'\n{table_join_type} {table_name} {table_alias}{join_condition_sql}')
            where_sql.append(f'({table_where_conditions})') if table_where_conditions else None

        select_sql_str = ',\n'.join(select_sql)
        table_sql_str = '\n'.join(table_sql)

        # create WHERE statements
        where_sql_str = f"\nWHERE {' AND '.join(where_sql)}" if where_sql else ''

        # create the CREATE TABLE statement by piecing together elements above
        sql = f"""
CREATE TABLE {self.eligibility_table} AS (
    SELECT {', '.join(self.unique_identifiers.get('with_aliases'))},
    {select_sql_str}
    {table_sql_str}
    {where_sql_str}
) WITH DATA PRIMARY INDEX ({', '.join(self.unique_identifiers.get('without_aliases'))});"""

        # create COLLECT STATISTICS sql
        collect_statistics_sql = f'COLLECT STATISTICS INDEX prindx ON {self.eligibility_table};'
        collect_statistics_sql_columns = f'COLLECT STATISTICS COLUMN ({", ".join(column_names)}) ON {self.eligibility_table}'

        # store all queries + table name
        queries = {
            'query': sql,
            'collect_query': [collect_statistics_sql, collect_statistics_sql_columns],
            'table_name': self.eligibility_table
        }
        self._eligibility_sql = queries

        return queries

    @call_logger()
    def generate_work_table_sql(self) -> List[Dict[str, Any]]:
        """
        Generates the SQL used to create the user work tables.

        :returns: table name, SQL for creating user work tables, and SQL for COLLECT STATISTICS
        """
        # loop through work_tables and get the SQL provided by the user
        queries: List[Dict[str, Any]] = []
        for table in self.work_tables:
            sql: str = table.get('sql')
            table_name = table.get('table_name')
            unique_index = table.get('unique_index')
            collect_stats_columns = table.get('collect_stats')

            # create CREATE TABLE sql
            query = f"""
CREATE TABLE {table_name} AS (
    {sql}
) WITH DATA
"""
            collect_queries = []

            # create UNIQUE INDEX if provided by user
            if unique_index is not None:
                query += f" PRIMARY INDEX ({unique_index})"
                collect_query = f'COLLECT STATISTICS INDEX prindx ON {table_name};'
                collect_queries.append(collect_query)

            # create additional COLLECT STATISTICS if provided by user
            if collect_stats_columns is not None:
                for collect_stat in collect_stats_columns:
                    collect_query = f'COLLECT STATISTICS ON {table_name} COLUMN ({collect_stat});'
                    collect_queries.append(collect_query)

            # store values in queries
            queries.append({
                'query': query,
                'collect_query': collect_queries,
                'table_name': table_name
            })

        return queries