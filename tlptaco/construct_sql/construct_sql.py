# Imports inferred from the code context
from collections import deque, OrderedDict, defaultdict
from typing import Dict, List, Any, Tuple, Optional


# --- Forward Class Declarations (inferred from property type hints) ---
# These would typically be imported from other modules.
class WaterfallSQLConstructor: pass


class OutputFileSQLConstructor: pass


class EligibilitySQLConstructor: pass


# The class definition is not shown in the images, but the methods belong inside a class.
# I've named it ConstructSQL based on the filename.
class ConstructSQL:
    # __init__ method is not pictured, but would likely initialize attributes like:
    # self._conditions
    # self._parsed_unique_identifiers
    # self._backend_tables
    # self._tables
    # self._work_tables
    # self._unique_identifiers
    # self._output_queries
    # self._WaterfallSQLConstructor = None
    # self._OutputFileSQLConstructor = None
    # self._EligibilitySQLConstructor = None
    # self.logger

    @call_logger()
    def _generate_backend_table_details(self) -> None:
        """
        Generates backend table details and stores them in the _backend_tables attribute.
        """
        need_names = ['eligibility']
        need_names.extend(self._parsed_unique_identifiers.get('original_without_aliases'))
        self._backend_tables = {}
        for table in need_names:
            self._backend_tables[table] = self._generate_table_name()

    @staticmethod
    def _topological_sort(odict: OrderedDict):
        # create a graph and in-degree counter
        graph = defaultdict(set)
        in_degree = defaultdict(int)

        # populate the graph and in-degree counter
        for key, values in odict.items():
            for value in values:
                if value != '':
                    graph[value].add(key)
                    in_degree[key] += 1
                    if value not in in_degree:
                        in_degree[value] = 0

        # find all nodes with no incoming edges
        zero_in_degree_queue = deque([node for node in in_degree if in_degree[node] == 0])

        # perform topological sort
        sorted_list = []
        while zero_in_degree_queue:
            node = zero_in_degree_queue.popleft()
            sorted_list.append(node)

            # decrease the in_degree of each neighbor
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    zero_in_degree_queue.append(neighbor)

        # check if the topological sort succeeded
        if len(sorted_list) == len(in_degree):
            return sorted_list
        else:
            # find all aliases causing the circular reference
            circular_aliases = set()
            for key, values in odict.items():
                if in_degree[key] > 0:
                    circular_aliases.add(key)
                    circular_aliases.update(values)
            raise ValueError(f"Circular reference detected in tables with aliases: {sorted(circular_aliases)}")

    @call_logger()
    def _assimilate_tables(self, tables: Dict[str, List[Dict[str, Any]]]) -> Tuple[
        List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Combines the tables and user work table details into a single dictionary.

        :param tables: table values to modify/assimilate
        :type tables: Dict[str, List[Dict[str, Any]]]
        :returns: modified dictionary with tables and user work tables combined
        :rtype: Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]
        """
        work_tables = tables.get('work_tables')
        for table in work_tables:
            sql = table.get('sql')

            # if the sql is a sql filename, then read the file into the sql variable and store in table
            if sql.endswith(".sql"):
                with open(sql, 'r') as f:
                    sql = f.read()
                    table['sql'] = sql

            join_type = table.get('join_type')
            alias = table.get('alias')
            where_conditions = table.get('where_conditions')
            join_conditions = table.get('join_conditions')
            table_name = self._generate_table_name()
            table['table_name'] = table_name

            table_entry = {
                'table_name': table_name,
                'join_type': join_type,
                'alias': alias,
                'where_conditions': where_conditions,
                'join_conditions': join_conditions
            }
            tables.get('tables').append(table_entry)

        tables['tables']: OrderedDict = self._sort_tables(tables.get('tables'))
        return tables.get('tables'), tables.get('work_tables')

    @call_logger()
    def _parse_unique_identifiers(self) -> None:
        """
        Parses the unique identifiers and stores the parsed values in the _parsed_unique_identifiers attribute.
        """
        without_aliases: Set[str] = set()
        with_aliases: Set[str] = set()
        original_without_aliases: List[str] = []

        # loop through unique identifiers and parse out
        for identifier in self._unique_identifiers:
            parts = [part.strip() for part in identifier.split('.')]
            original_parts_without_aliases = [part.split(' ')[-1] for part in parts]
            original_without_aliases.append('.'.join(original_parts_without_aliases))
            for part in parts:
                alias, column = part.split('.')
                without_aliases.add(column)
                with_aliases.add(part)

        parsed_unique_identifiers = {
            "without_aliases": without_aliases,
            "with_aliases": with_aliases,
            "original_without_aliases": set(original_without_aliases)
        }
        self._parsed_unique_identifiers = parsed_unique_identifiers

    @call_logger('final_result')
    def _prepare_conditions(self) -> None:
        """
        Adds the column_name to each condition and creates a reference dictionary with these names.
        These names will be used on a backend table, one column for each check in the condition dictionary.
        Column name format is {channel}_{template}_{check number}
        """
        # start the check numbers at 1
        check_num = 1
        # set column naming convention
        column_naming_convention = "{channel}_{template}_{num}"
        # prep set for column_names
        column_names: Set[str] = set()

        # iterate through each channel, template, and check in the conditions and add the column name
        for channel, templates in self.conditions.items():
            for template, checks in templates.items():
                for check in checks:
                    column_name = column_naming_convention.format(channel=channel, template=template, num=check_num)
                    check_num += 1
                    check['column_name'] = column_name
                    column_names.add(column_name)

        # create dictionary of each check with all corresponding relevant checks
        result: Dict[str, Any] = {}
        # iterate through all checks again using their newly created column names
        for column_name in column_names:
            selected_checks_list: List[str] = []

            def add_checks(channels: List[str]) -> None:
                """
                Appends all column names in the provided channel to selected_checks_list
                :param channels: a list of channels to iterate through
                :type channels: List[str]
                """
                for channel in channels:
                    for template, checks in self.conditions[channel].items():
                        for check in checks:
                            if check['column_name'] not in selected_checks_list:
                                # NOTE: since selected_checks_list was defined outside this function, this will
                                # alter that list directly
                                selected_checks_list.append(check['column_name'])

            # prep the variables to identify the specific channel and template we are working with
            selected_channel: Optional[str] = None
            selected_template: Optional[str] = None
            templates_order: List[str] = []

            # iterate through all channels and templates in conditions until we find column_name
            for channel, templates in self.conditions.items():
                # add each channel to templates_order
                templates_order.append(channel)
                # iterate through each template
                for template, checks in templates.items():
                    # as soon as the column_name is found, break the loop and save the current channel and template
                    if column_name in [check['column_name'] for check in checks]:
                        selected_channel = channel
                        selected_template = template
                        break
                # if selected_channel is not None, then break the 'for channel, templates ...' loop
                if selected_channel:
                    break

            # if the loop finishes and selected_channel is still None, then a check was not found
            if not selected_channel:
                error_message = f"{column_name} was not found in the conditions dictionary; this is an issue with the actual library; this is not a user error"
                self.logger.critical(f"{self.__class__}._prepare_conditions {error_message}")
                raise ValueError(error_message)

            # Ensure 'BA' template is included in the base waterfall
            if selected_channel == 'main' or selected_template == 'BA':
                # add all pertinent base eligibility for the selected column_name
                add_checks(['main', selected_channel])
            else:
                # add checks from the main waterfall
                add_checks(['main'])
                for channel, templates in self.conditions.items():
                    # if the channel isn't the main waterfall, then loop through the templates for the channel
                    # (since main will only ever have one template and is only base eligibility)
                    if channel != 'main':
                        for template, checks in templates.items():
                            if template == selected_template or template == 'BA':
                                for check in checks:
                                    if check['column_name'] not in selected_checks_list:
                                        selected_checks_list.append(check['column_name'])

            # remove the current column_name from the selected_checks_list
            if column_name in selected_checks_list:
                selected_checks_list.remove(column_name)

            # prep dictionaries to store templates prior and after (post) current column_name
            prior_templates: Dict[str, Any] = {}
            post_templates: Dict[str, Any] = {}

            # Build prior templates excluding 'BA'
            for template in self.conditions[selected_channel]:
                if template == selected_template:
                    break
                if template != 'BA':
                    no_output = [check['column_name'] for check in self.conditions[selected_channel][template] if
                                 not check['output']]
                    output = [check['column_name'] for check in self.conditions[selected_channel][template] if
                              check['output']]
                    prior_templates[template] = {'no_output': no_output, 'output': output}

            # Build post templates excluding 'BA'
            for template in list(self.conditions[selected_channel].keys())[
                            list(self.conditions[selected_channel].keys()).index(selected_template) + 1:]:
                if template != 'BA':
                    no_output = [check['column_name'] for check in self.conditions[selected_channel][template] if
                                 not check['output']]
                    output = [check['column_name'] for check in self.conditions[selected_channel][template] if
                              check['output']]
                    post_templates[template] = {'no_output': no_output, 'output': output}

            # for each column_name, store a dictionary with:
            # base: all prerequisite conditions for a column_name
            # prior_templates: templates that occur in the same channel as column_name BEFORE the template the column_name belongs to
            # post_templates: templates that occur in the same channel as column_name AFTER the template the column_name belongs to
            result[column_name] = {
                'base': selected_checks_list,
                'prior_templates': prior_templates,
                'post_templates': post_templates
            }

        # sort result by the keys (i.e. the check number)
        # NOTE: an IDEA will show the line below as an error "unexpected types"; ignore this warning
        result = OrderedDict(sorted(result.items(), key=lambda x: int(x[0].split('_')[-1])))

        # can't remember why I did this intermediary step; shouldn't be a technical reason
        conditions_column_mappings = result
        self._waterfall_conditions_column_mappings = conditions_column_mappings

    @property
    def conditions(self) -> OrderedDict:
        """Getter for conditions."""
        return self._conditions

    @conditions.setter
    def conditions(self, value: OrderedDict) -> None:
        """
        Setter for conditions.
        :param value: the value to set on self._conditions
        """
        self._conditions = value
        # when self._conditions (or self.conditions) is set, run self._prepare_conditions()
        self._prepare_conditions()

    @property
    def backend_tables(self) -> dict[str, str]:
        """Getter for backend_tables"""
        return self._backend_tables

    @backend_tables.setter
    def backend_tables(self, value: dict[str, str]) -> None:
        """Setter for backend_tables"""
        self._backend_tables = value

    @property
    def tables(self) -> List[Dict[str, Any]]:
        """Getter for tables."""
        return self._tables

    @tables.setter
    def tables(self, tables: Dict[str, List[Dict[str, Any]]]) -> None:
        """
        Setter for tables.
        """
        tables, work_tables = self._assimilate_tables(tables)
        self._tables: List = tables
        self._work_tables: List = work_tables

    @property
    def work_tables(self) -> List[Dict[str, Any]]:
        """Getter for work_tables."""
        return self._work_tables

    @work_tables.setter
    def work_tables(self, values: List[Dict[str, Any]]) -> None:
        """Setter for work_tables."""
        self._work_tables = values

    @property
    def unique_identifiers(self) -> List[str]:
        """Getter for unique_identifiers."""
        return self._unique_identifiers

    @unique_identifiers.setter
    def unique_identifiers(self, value: List[str]) -> None:
        """Setter for unique_identifiers."""
        self._unique_identifiers = value
        self._parse_unique_identifiers()

    @property
    @call_logger()
    def waterfall(self) -> WaterfallSQLConstructor:
        """Getter for waterfall."""
        # the first time this is called, it will be None; in this situation, set up a WaterfallSQLConstructor
        if self._WaterfallSQLConstructor is None:
            self._WaterfallSQLConstructor = WaterfallSQLConstructor(
                self.conditions,
                self._waterfall_conditions_column_mappings,
                self._backend_tables,
                self._parsed_unique_identifiers,
                self.logger
            )
        return self._WaterfallSQLConstructor

    @property
    def output_file(self) -> OutputFileSQLConstructor:
        """Getter for output_file."""
        # the first time this is called, it will be None; in this situation, set up an OutputFileSQLConstructor
        if self._OutputFileSQLConstructor is None:
            self._OutputFileSQLConstructor = OutputFileSQLConstructor(
                self._output_queries,
                self.conditions,
                self._backend_tables.get('eligibility'),
                self.logger
            )
        return self._OutputFileSQLConstructor

    @property
    def eligible(self) -> EligibilitySQLConstructor:
        """Getter for eligible."""
        # the first time this is called, it will be None; in this situation, set up an EligibilitySQLConstructor
        if self._EligibilitySQLConstructor is None:
            self._EligibilitySQLConstructor = EligibilitySQLConstructor(
                self.conditions,
                self.tables,
                self.work_tables,
                self._backend_tables.get('eligibility'),
                self._parsed_unique_identifiers,
                self.logger
            )
        return self._EligibilitySQLConstructor