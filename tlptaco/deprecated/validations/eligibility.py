from collections import OrderedDict
from tlptaco.validations.exceptions import ValueWarning


class EligibleValidator:
    _validators = {}

    @staticmethod
    def validate_conditions(conditions):
        """
        Validates the structure of the `_conditions` attribute.
        If output is missing AND it isn't the last item in the template (where output should = True), then it sets it to False automatically

        The `_conditions` attribute must adhere to the following structure:
        {
            'main': {
                'BA': [
                    {'sql': 'some_sql', 'output': False, 'description': 'some description here'},
                ],
                ...
            },
            'some_channel': {
                'BA': [{'sql': 'some_sql', 'output': False, 'description': 'some description here'}, ...],
                'segment1': [{'sql': 'some_sql', 'output': False, 'description': 'some description here'}, ...]
            }
        }

        Raises:
            ValueError: If the structure is not valid.
        """
        if not isinstance(conditions, dict):
            raise ValueError("Conditions must be a dictionary.")
        if 'main' not in conditions:
            raise ValueError("Conditions must contain a 'main' key.")

        for key, subdict in conditions.items():
            if not isinstance(subdict, dict):
                raise ValueError(f"Value for key '{key}' must be a dictionary.")

            if key == 'main':
                if list(subdict.keys()) != ['BA']:
                    raise ValueError("'main' can only have 'BA' as its key.")
                for subkey, sublist in subdict.items():
                    if subkey == 'BA':
                        for item in sublist:
                            if not isinstance(item, dict):
                                raise ValueError(f"Each item in the list for key 'main' -> 'BA' must be a dictionary.")
                            if item.get('output') is True:
                                raise ValueError("Items under 'main' -> 'BA' cannot have 'output: True'")

                            required_keys = {'sql', 'description'}
                            optional_keys_with_default_arg = {'output': False}

                            if not required_keys.issubset(item.keys()):
                                raise ValueError(
                                    f"Each dictionary in the list for key 'main' -> 'BA' must contain keys: {required_keys}")

                            # if any of the optional keys are not present in the dictionary, then add in the default argument
                            for optional_key, optional_value in optional_keys_with_default_arg.items():
                                if optional_key not in item:
                                    item[optional_key] = optional_value
            else:
                for subkey, sublist in subdict.items():
                    output_true_count = 0
                    if not isinstance(sublist, list):
                        raise ValueError(f"Value for key '{key}' -> '{subkey}' must be a list.")
                    for item in sublist:
                        if not isinstance(item, dict):
                            raise ValueError(
                                f"Each item in the list for key '{key}' -> '{subkey}' must be a dictionary.")

                        if item.get('output') is True:
                            output_true_count += 1

                        required_keys = {'sql', 'description'}
                        optional_keys_with_default_arg = {'output': False}
                        if not required_keys.issubset(item.keys()):
                            raise ValueError(
                                f"Each dictionary in the list for key '{key}' -> '{subkey}' must contain keys: {required_keys}")

                        # if any of the optional keys are not present in the dictionary, then add in the default argument
                        for optional_key, optional_value in optional_keys_with_default_arg.items():
                            if optional_key not in item:
                                item[optional_key] = optional_value

                    if subkey != 'BA':
                        if output_true_count > 1:
                            raise ValueError(
                                f"Only one item in the list for any subkey under '{key}' can have 'output: True'.")
                        if output_true_count == 1 and not sublist[-1].get('output'):
                            raise ValueError(
                                f"The last item in the list for key '{key}' -> '{subkey}' must have 'output: True'.")
                    elif subkey == 'BA':
                        if output_true_count > 0:
                            raise ValueError(f"BA template CANNOT have output = True")

    @staticmethod
    def validate_tables(tables):
        """
        Validates the structure of the '_tables' attribute.

        The '_tables' attribute must adhere to the following structure:
        {
            'tables': [
                {'table_name': 'schema_name.table_name', 'join_type': 'valid join type', 'alias': 'alias', 'where_conditions': 'sql code', 'join_conditions': 'sql code'},
                ...
            ],
            'work_tables': [
                {'sql': 'sql code', 'join_type': 'valid join type', 'alias': 'alias', 'where_conditions': 'sql code', 'join_conditions': 'sql code'}
            ]
        }

        Raises:
            ValueError: if the structure is not valid.
        """
        valid_keys = {'tables', 'work_tables'}
        if not isinstance(tables, dict):
            raise ValueError("Tables must be a dictionary.")
        if not valid_keys.issuperset(tables.keys()):
            raise ValueError(f"Tables must contain only the following keys: {valid_keys}")

        from_count = 0
        for key, sublist in tables.items():
            if not isinstance(sublist, list):
                raise ValueError(f"Value for key '{key}' must be a list.")
            for i, item in enumerate(sublist):
                if not isinstance(item, dict):
                    raise ValueError(f"Each item in the list for key '{key}' must be a dictionary.")

                if key == 'tables':
                    if i == 0 and item.get('join_type') == 'FROM':
                        from_count += 1
                        required_keys = {'table_name', 'join_type', 'alias'}
                        optional_keys_with_default_args = {'where_condition': "", 'join_conditions': ""}
                    elif item.get('join_type') != 'FROM':
                        required_keys = {'table_name', 'join_type', 'alias', 'join_conditions'}
                        optional_keys_with_default_args = {'where_condition': ""}

                elif key == 'work_tables':
                    required_keys = {'sql', 'join_type', 'alias', 'join_conditions'}
                    optional_keys_with_default_args = {'collect_stats': None, 'unique_index': None,
                                                       'where_conditions': ""}

                if not required_keys.issubset(item.keys()):
                    raise ValueError(f"Each dictionary in the list for key '{key}' must contain keys: {required_keys}")

                # if an optional key does not exist in the dictionary, then add it with the default argument
                for optional_key, optional_value in optional_keys_with_default_args.items():
                    if optional_key not in item:
                        item[optional_key] = optional_value

        if from_count != 1:
            raise ValueError(
                "There must be exactly one 'FROM' join type between the first item in the 'tables' list and any item "
                "in the 'work_tables' list; any FROM in 'tables' must be the first in the list")

    @staticmethod
    def validate_unique_identifiers(unique_identifiers):
        """
        Validates the structure of the '_unique_identifiers' attribute.
        The '_unique_identifiers' attribute must be a list of strings.

        Raises:
            ValueError: If the structure is not valid.
        """
        if not isinstance(unique_identifiers, list):
            raise ValueError("Unique identifiers must be a list.")
        if not all(isinstance(item, str) for item in unique_identifiers):
            raise ValueError("All items in unique identifiers must be strings.")

    @staticmethod
    def validate_campaign_planner(value):
        if value is None:
            raise ValueWarning(
                f"'campaign_planner' is 'None'; attempting to build a Waterfall from this instance of Eligible will fail")
        elif not isinstance(value, str):
            raise ValueWarning(
                f"'campaign_planner' is type {type(value)}, instead of a string; attempting to build a Waterfall from this instance of Eligible will fail")
        elif value == '':
            raise ValueWarning(
                f"'campaign_planner' is blank; attempting to build a Waterfall from this instance of Eligible will fail")

    @staticmethod
    def validate_lead(value):
        if value is None:
            raise ValueWarning(
                f"'lead' is 'None'; it must be a string; attempting to build a waterfall from this instance of Eligible will fail")
        elif not isinstance(value, str):
            raise ValueWarning(
                f"'lead' is type {type(value)}, instead of a string; attempting to build a waterfall from this instance of Eligible will fail")
        elif value == '':
            raise ValueWarning(
                f"'lead' is blank; attempting to build a Waterfall from this instance of Eligible will fail")

    @staticmethod
    def validate_username(value):
        if value is None:
            raise ValueError(f"'username' cannot be 'None'; it must be a string")
        elif not isinstance(value, str):
            raise ValueError(f"'username' must be a string, not {type(value)}")
        elif value == '':
            raise ValueError(f"'username' cannot be blank")

    @staticmethod
    def validate_offer_code(value):
        if value is None:
            raise ValueWarning(
                f"'offer_code' is 'None'; attempting to build a Waterfall from this instance of Eligible will fail")
        elif not isinstance(value, str):
            raise ValueWarning(
                f"'offer_code' is type {type(value)}, not string; attempting to build a waterfall from this instance of Eligible will fail")
        elif value == '':
            raise ValueWarning(
                f"'offer_code' is blank; attempting to build a waterfall from this instance of Eligible will fail")

    def __setattr__(self, name, value):
        try:
            if name in self._validators:
                self._validators[name](value)
                if hasattr(self, 'logger') and self.logger:
                    self.logger.info(f"{self.__class__}.{name} validated")
            super().__setattr__(name, value)
        except ValueWarning as e:
            if hasattr(self, 'logger') and self.logger:
                self.logger.warning(f'WARNING {self.__class__}.{name}: {e}')
        except Exception as e:
            if hasattr(self, 'logger') and self.logger:
                self.logger.error(f'{self.__class__}.{name} unable to validate: {e}')
            raise e