from tlptaco.logging.logging import CustomLogger, call_logger
import re
from typing import Dict, List


class OutputFileSQLConstructor:
    """
    Class for handling SQL construction for the output file.
    This class is handled by the `tlptaco.construct_sql.construct_sql.SQLConstructor` class and is used by `tlptaco.output.output.Output`.
    Generates Teradata SQL based on requirements provided by `tlptaco.output.output.Output`.

    Attributes:
        logger (CustomLogger): logger for logs
        output_queries (Dict): output queries for each channel
        conditions (Dict): conditions entities are required to pass
        eligibility_table (str): table name (schema_name.table_name) where the eligibility was created

    Methods:
        _extract_eligibility_table_columns: extracts the alias the user used on the eligibility table
            in the output query and uses it to identify the columns that need to be pulled from this table
        generate_base_eligible_sql: generates the SQL to pull eligible records for each channel
        generate_output_sql: calls generate_base_eligible_sql, adds the eligibility table name to the
            query, and stores all queries in a dictionary
    """

    def __init__(
            self,
            output_queries: Dict,
            conditions: Dict,
            eligibility_table: str,
            logger: CustomLogger
    ):
        self.logger = logger
        self.output_queries = output_queries
        self.conditions = conditions
        self.eligibility_table = eligibility_table

    @staticmethod
    def _extract_eligibility_table_columns(query: str) -> List:
        """
        extracts the alias the user used on the eligibility table in the output query and uses it to
        identify the columns that need to be pulled from this table

        :param query: output query to parse
        :return: the columns that will be pulled from the eligibility table
        :rtype: List
        """
        from_pattern = re.compile(r"FROM\s+\{eligibility_table\}\s+(?:AS\s+)?(\w+)", re.IGNORECASE)
        alias_match = from_pattern.search(query)

        if not alias_match:
            return []  # TODO: add a validation that checks to see if this list is returned empty; if so, raise an error to ask the user to provide fields

        alias = alias_match.group(1)
        # regular expression to find all columns starting with alias in the SELECT statement
        select_pattern = re.compile(rf'{alias}\.(\w+)', re.IGNORECASE)
        columns = select_pattern.findall(query)

        # remove duplicates and return the list
        unique_columns = set(x.lower() for x in columns) if x.lower() != 'template_id' else None
        unique_columns = list(unique_columns)

        return unique_columns

    @call_logger()
    def generate_base_eligible_sql(self) -> Dict:
        """
        Generates the SQL to pull eligible records for each channel

        :return: the output SQL queries for each channel
        :rtype: Dict
        """
        sql_statements = {}

        # Extract the WHERE conditions from 'main'
        where_conditions = []
        for template, checks in self.conditions.get('main', {}).items():
            for check in checks:
                where_conditions.append(check['column_name'] + ' = 1')

        # Generate CASE statements for each channel and template
        for channel, templates in self.conditions.items():
            if channel == 'main':
                continue
            elif channel not in self.output_queries.keys():
                self.logger.warning(f"Channel {channel} found in conditions, but not in output instructions")
                continue
            else:
                self.logger.info(f"Prepping output instructions for {channel}")

            case_statements = []
            possible_templates = set(f"'{x}'" for x in templates.keys() if x != 'BA')
            self.logger.info(f"{channel} output file will contain {possible_templates=}")

            template_checks = {
                'base': [],
                'previous': []
            }

            for template, checks in templates.items():
                if template == 'BA':
                    template_checks['base'] = [x.get('column_name') for x in checks]
                else:
                    checks_conditions: str = " AND ".join([check['column_name'] + ' = 1' for check in checks])

                    # setup previous template checks
                    prev_template_conditions_sql = ""
                    if template_checks.get('previous'):
                        prev_template_conditions_sql = f" AND ({' OR '.join([f'{x} = 0' for x in template_checks.get('previous')])})"

                    case_statement: str = f"WHEN {checks_conditions}{prev_template_conditions_sql} THEN '{template}'"
                    case_statements.append(case_statement)

                    # add current template checks as a list to previous
                    template_checks['previous'].append([check['column_name'] for check in checks][0])

            # add channel BA checks to WHERE clause
            all_where_conditions = where_conditions + [f'{x} = 1' for x in template_checks.get('base')]

            # grab columns from eligibility_table from output queries
            output_query = self.output_queries.get(channel)
            elig_tbl_columns = self._extract_eligibility_table_columns(output_query)
            self.logger.info(f"{self.__class__}.generate_base_eligible_sql {elig_tbl_columns=}")

            # Combine the CASE statements for each channel
            select_sql = f"SELECT {', '.join(elig_tbl_columns)},"
            case_sql = "CASE " + " ".join(
                case_statements) + " END AS template_id"  # f-string is missing whitespace around operator
            where_sql = "WHERE " + " AND ".join(all_where_conditions)
            full_sql = f"{select_sql} \n{case_sql} \nFROM {{eligibility_table}} \n{where_sql} AND template_id IN ({', '.join(possible_templates)})"  # TODO: add a validation to make sure the eligibility_table in the user query uses an alias and ALL columns in the output query have an alias

            sql_statements[channel] = full_sql

        self.logger.info(f"Able to parse instructions for these channels: {', '.join(list(sql_statements.keys()))}")
        return sql_statements

    @call_logger()
    def generate_output_sql(self) -> Dict:
        """
        Calls generate_base_eligible_sql, adds the eligibility table name to the query, and stores all
        queries in a dictionary

        :return: completed queries for output files for each channel
        :rtype: Dict
        """
        queries = {}
        base_tables = self.generate_base_eligible_sql()
        for channel, query in self.output_queries.items():
            channel_eligible = base_tables.get(channel)
            if channel_eligible is None:
                self.logger.error(f"There was an issue with the output query for {channel}")
                continue
            query = query.format(eligibility_table=channel_eligible)
            queries[channel] = query

        return queries  # blank line at end of file