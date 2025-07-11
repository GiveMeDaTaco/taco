import inspect
from typing import Dict, List, Any, Optional
from tlptaco.logging.logging import call_logger, CustomLogger


class WaterfallSQLConstructor:
    """
    A class to generate SQL queries for the waterfall process in eligibility checks.
    This class is handled by the `tlptaco.construct_sql.construct_sql.SQLConstructor` class and is used by `tlptaco.waterfall.waterfall.Waterfall`.
    Generates Teradata SQL based on requirements provided by `tlptaco.waterfall.waterfall.Waterfall`.

    Attributes:
        conditions (Dict[str, Dict[str, Any]]): Conditions for eligibility checks.
        _backend_tables (Dict[str, str]): Backend table details.
        parsed_unique_identifiers (Dict[str, Any]): Parsed unique identifiers.
        _conditions_column_mappings (Dict[str, Any]): Mappings of conditions to columns.
        _regain_sql (Optional[Dict[str, str]]): SQL queries for regaining records.
        _incremental_drops_sql (Optional[Dict[str, str]]): SQL queries for incremental drops.
        _unique_drops_sql (Optional[Dict[str, str]]): SQL queries for unique drops.
        _remaining_sql (Optional[Dict[str, str]]): SQL queries for remaining records.
        logger (CustomLogger): logger used to log
    """

    def __init__(
            self,
            conditions: Dict[str, Dict[str, Any]],
            conditions_column_mappings: Dict[str, Any],
            backend_tables: Dict[str, str],
            parsed_unique_identifiers: Dict[str, Any],
            logger: CustomLogger
    ) -> None:
        """
        Initializes class

        :param conditions: contains conditions for the waterfall
        :type conditions: Dict
        :param conditions_column_mappings: contains the related checks for each check found in conditions
        :type conditions_column_mappings: Dict
        :param backend_tables: the tables that will contain the waterfall counts for each unique identifier (i.e. party id, account number, etc.)
        :type backend_tables: Dict
        :param parsed_unique_identifiers: various formats (e.g. with table alias, without table alias) of the unique identifiers that the waterfall will use for counts
        :type parsed_unique_identifiers: Dict
        :param logger: logger to use for logging
        :type logger: CustomLogger
        """
        self.logger = logger
        self.conditions = conditions
        self._backend_tables = backend_tables
        self.parsed_unique_identifiers = parsed_unique_identifiers
        self._conditions_column_mappings = conditions_column_mappings
        self._column_names = self._extract_column_names(conditions)
        self._regain_sql = None
        self._incremental_drops_sql = None
        self._unique_drops_sql = None
        self._remaining_sql = None

    @call_logger()
    def generate_unique_identifier_details_sql(self) -> Dict[str, Dict[str, str]]:
        """
        Generates the SQL for waterfall counts for each unique identifier provided

        :return: SQL queries to create the tables for each unique identifier waterfall counts
        :rtype: Dict[str, Dict[str, str]]
        """
        queries: Dict[str, Dict[str, str]] = {}
        column_names: List[str] = []
        for channel, templates in self.conditions.items():
            for template, checks in templates.items():
                for check in checks:
                    column_names.append(check.get('column_name'))

        max_columns = [f'\nMAX({check}) AS max_{check}' for check in column_names]
        select_sql = max_columns.copy()

        for identifier in self.parsed_unique_identifiers.get('original_without_aliases', []):
            group_by = [str(x) for x in range(1, len(identifier.split('.')) + 1)]
            identifier_details_table = self._backend_tables.get(identifier)
            sql = f"""
CREATE TABLE {identifier_details_table} AS (
    SELECT
        {identifier},
        {','.join(select_sql)}
    FROM
        {self._backend_tables.get('eligibility')}
    GROUP BY {','.join(group_by)}
) WITH DATA PRIMARY INDEX ({identifier})
"""
            collect_stats = f'COLLECT STATISTICS INDEX prindx ON {identifier_details_table}'
            queries[identifier] = {
                'sql': sql,
                'table_name': identifier_details_table,
                'collect_stats': collect_stats
            }
        return queries

    @staticmethod
    def _extract_column_names(conditions: dict) -> List:
        column_names = []
        for channel, templates in conditions.items():
            for template, checks in templates.items():
                for check in checks:
                    column_names.append(check.get('column_name'))

        # sort the columns by their column number (i.e. the last number in the string)
        column_names = sorted(column_names, key=lambda x: int(x.split('_')[-1]))
        return column_names

    @call_logger()
    def generate_unique_drops_sql(self) -> Dict[str, str]:
        """
        Generate the SQL to generate the counts for Unique Drops

        :return: SQL queries to identify counts for each unique identifier
        :rtype: Dict[str, str]
        """
        queries: Dict[str, str] = {}
        for identifier in self.parsed_unique_identifiers.get('original_without_aliases', []):
            case_statements: List[str] = []
            conditions_keys = self._conditions_column_mappings.keys()
            # sort the conditions by the last value found in the column name (i.e. the check number)
            conditions_keys = sorted(conditions_keys, key=lambda x: int(x.split('_')[-1]))
            for check in conditions_keys:
                case_statement = f"SUM(CASE WHEN max_{check} = 0 THEN 1 ELSE 0 END) AS {check}"
                case_statements.append(case_statement)

            query = f"SELECT\n CAST('{inspect.currentframe().f_code.co_name}' AS VARCHAR(30)) AS stat_name,\n"
            query += ',\n'.join(case_statements)
            query += f'\nFROM {self._backend_tables.get(identifier)}'

            queries[identifier] = query

        self._unique_drops_sql = queries
        return queries

    @call_logger()
    def generate_regain_sql(self) -> Dict[str, str]:
        """
        Generate the SQL to generate the counts for number regained if a condition is removed

        :return: SQL queries to identify counts for each unique identifier
        :rtype: Dict[str, str]
        """
        # This method's implementation was not present in the provided screenshots.
        queries: Dict[str, str] = {}
        return queries

    @call_logger()
    def generate_incremental_drops_sql(self) -> Dict[str, str]:
        """
        SQL for generating the counts for the number of entities incrementally dropped for each check

        :return: SQL queries to identify counts for each unique identifier
        :rtype: Dict[str, str]
        """
        queries: Dict[str, str] = {}
        for identifier in self.parsed_unique_identifiers.get('original_without_aliases', []):
            case_statements: List[str] = []

            # MAIN WATERFALL CASE STATEMENTS
            main_checks = self.conditions.get('main').get('BA')
            main_checks = [x.get('column_name') for x in main_checks]
            main_checks_list = list()
            for col in main_checks:
                temp_list = [f'max_{col} = 0']
                temp_list.extend(main_checks_list)
                statement = f"SUM(CASE WHEN {' AND '.join(temp_list)} THEN 1 END) AS {col}"
                case_statements.append(statement)
                main_checks_list.append(f'max_{col} = 1')

            # prep main_checks_list for use in channels
            main_checks_list = [f'max_{col} = 1' for col in main_checks]

            # CHANNEL STATEMENTS
            channels = [x for x in self.conditions.keys() if x != 'main']
            for channel in channels:
                channel_dict = self.conditions.get(channel)
                channel_templates = channel_dict.keys()

                if 'BA' in channel_templates:
                    channel_base_list = list()
                    channel_base_checks = [check.get('column_name') for check in channel_dict.get('BA')]
                    for col in channel_base_checks:
                        temp_list = [f'max_{col} = 0']
                        temp_list.extend(channel_base_list)
                        temp_list.extend(main_checks_list)
                        statement = f"SUM(CASE WHEN {' AND '.join(temp_list)} THEN 1 END) AS {col}"
                        case_statements.append(statement)
                        channel_base_list.append(f'max_{col} = 1')
                    # prep channel_base_list for use in templates
                    channel_base_list = [f'max_{col} = 1' for col in channel_base_checks]
                else:
                    channel_base_list = main_checks_list.copy()

                previous_templates_list = list()
                for template in [x for x in channel_templates if x != 'BA']:
                    channel_segment_checks = [check.get('column_name') for check in channel_dict.get(template)]
                    for col in channel_segment_checks:
                        temp_list = [f'max_{x} = 1' if x != col else f'max_{x} = 0' for x in channel_segment_checks]
                        temp_list.extend(channel_base_list)

                        if previous_templates_list:
                            temp_prevs = list()
                            for prev in previous_templates_list:
                                temp_prev = f"({' OR '.join(prev)})"
                                temp_prevs.append(temp_prev)
                            temp_statement = f" AND {' AND '.join(temp_prevs)}"
                            statement = f"SUM(CASE WHEN {' AND '.join(temp_list)}{temp_statement} THEN 1 END) AS {col}"
                        else:
                            statement = f"SUM(CASE WHEN {' AND '.join(temp_list)} THEN 1 END) AS {col}"
                        case_statements.append(statement)

                    # prep list for previous_templates_list
                    temp_list = [f'max_{x} = 0' for x in channel_segment_checks]
                    previous_templates_list.append(temp_list.copy())

            # CREATE QUERY
            query = f"SELECT\n CAST('{inspect.currentframe().f_code.co_name}' AS VARCHAR(30)) AS stat_name,\n"
            query += ',\n'.join(case_statements)
            query += f'\nFROM {self._backend_tables.get(identifier)}'

            queries[identifier] = query

        self._incremental_drops_sql = queries
        return queries

    @call_logger()
    def generate_remaining_sql(self) -> Dict[str, str]:
        """
        Generate the SQL to count the entities remaining after each check

        :return: SQL queries to identify counts for each unique identifier
        :rtype: Dict[str, str]
        """
        queries: Dict[str, str] = {}
        for identifier in self.parsed_unique_identifiers.get('original_without_aliases', []):
            case_statements: List[str] = []

            # MAIN WATERFALL CASE STATEMENTS
            main_checks = self.conditions.get('main').get('BA')
            main_checks = [x.get('column_name') for x in main_checks]
            main_checks_list = list()
            for col in main_checks:
                temp_list = [f'max_{col} = 1']
                temp_list.extend(main_checks_list)
                statement = f"SUM(CASE WHEN {' AND '.join(temp_list)} THEN 1 END) AS {col}"
                case_statements.append(statement)
                main_checks_list.append(f'max_{col} = 1')

            # CHANNEL STATEMENTS
            channels = [x for x in self.conditions.keys() if x != 'main']
            for channel in channels:
                channel_dict = self.conditions.get(channel)
                channel_templates = channel_dict.keys()
                if 'BA' in channel_templates:
                    channel_base_list = list()
                    channel_base_checks = [check.get('column_name') for check in channel_dict.get('BA')]
                    for col in channel_base_checks:
                        temp_list = [f'max_{col} = 1']
                        temp_list.extend(channel_base_list)
                        temp_list.extend(main_checks_list)
                        statement = f"SUM(CASE WHEN {' AND '.join(temp_list)} THEN 1 END) AS {col}"
                        case_statements.append(statement)
                        channel_base_list.append(f'max_{col} = 1')
                else:
                    channel_base_list = main_checks_list.copy()

                previous_templates_list = list()
                for template in [x for x in channel_templates if x != 'BA']:
                    channel_segment_list = list()
                    channel_segment_checks = [check.get('column_name') for check in channel_dict.get(template)]
                    for col in channel_segment_checks:
                        temp_list = [f'max_{col} = 1']
                        temp_list.extend(channel_segment_list)
                        temp_list.extend(channel_base_list)
                        temp_list.extend(main_checks_list)

                        if previous_templates_list:
                            temp_prevs = list()
                            for prev in previous_templates_list:
                                temp_statement = f"({' OR '.join(prev)})"
                                temp_prevs.append(temp_statement)
                            temp_statement = f" AND {' AND '.join(temp_prevs)}"
                            statement = f"SUM(CASE WHEN {' AND '.join(temp_list)}{temp_statement} THEN 1 END) AS {col}"
                        else:
                            statement = f"SUM(CASE WHEN {' AND '.join(temp_list)} THEN 1 END) AS {col}"
                        case_statements.append(statement)
                    channel_segment_list.append(f'max_{col} = 1')

                    # prep template for following templates
                    previous_templates_list.append([f'max_{col} = 0' for col in channel_segment_checks])

            query = f"SELECT\n CAST('{inspect.currentframe().f_code.co_name}' AS VARCHAR(30)) AS stat_name,\n"
            query += ',\n'.join(case_statements)
            query += f'\nFROM {self._backend_tables.get(identifier)}'
            queries[identifier] = query

        self._remaining_sql = queries
        return queries

    def generate_all_sql(self) -> Dict:
        """
        Generates all the SQL for all the waterfall counts

        :return: SQL queries to identify ALL counts for each unique identifier
        :rtype: Dict
        """
        remain_sql = self.generate_remaining_sql()
        increm_sql = self.generate_incremental_drops_sql()
        unique_sql = self.generate_unique_drops_sql()
        regain_sql = self.generate_regain_sql()
        queries = dict()
        for identifier in self.parsed_unique_identifiers.get('original_without_aliases', []):
            regain = regain_sql.get(identifier)
            increm = increm_sql.get(identifier)
            remain = remain_sql.get(identifier)
            unique = unique_sql.get(identifier)

            query = f"""
{regain}
UNION ALL
{increm}
UNION ALL
{remain}
UNION ALL
{unique}
"""
            queries[identifier] = query

        return queries