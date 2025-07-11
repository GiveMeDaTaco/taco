from tlptaco.logging.logging import CustomLogger
from tlptaco.connections.teradata import TeradataHandler
from tlptaco.construct_sql.construct_sql import SQLConstructor
from tlptaco.validations.exceptions import ValueWarning


class BaseValidator:
    """
    Contains some validations that are commonly used by multiple classes
    """
    _validators = {}

    @staticmethod
    def validate_logger(logger):
        """
        Ensures the logger is type `tlptaco.logging.logging.CustomLogger`

        :param logger: logger instance to be tested
        :raises ValueError: if logger is not type `tlptaco.logging.logging.CustomLogger`
        """
        if not isinstance(logger, CustomLogger):
            raise ValueError(f"logger provided must be type `tlptaco.logging.logging.CustomLogger`, not {type(logger)}")

    @staticmethod
    def validate_teradata_connection(teradata_connection):
        """
        Ensures the connection method is type `tlptaco.connections.teradata.TeradataHandler`

        :param teradata_connection: connection instance to be tested
        :raises ValueError: If connection is not `tlptaco.connections.teradata.TeradataHandler`
        """
        if not isinstance(teradata_connection, TeradataHandler):
            raise ValueError(
                f"teradata_connection provided must be type `tlptaco.connections.teradata.TeradataHandler`, not {type(teradata_connection)}")

    @staticmethod
    def validate_sqlconstructor(sqlconstructor):
        """
        Ensures that the SQL Constructor being used is type `tlptaco.construct_sql.construct_sql.SQLConstructor`

        :param sqlconstructor: constructor to validate
        :raises ValueError: if constructor is not `tlptaco.construct_sql.construct_sql.SQLConstructor`
        """
        if not isinstance(sqlconstructor, SQLConstructor):
            raise ValueError(
                f"sqlconstructor provided must be type `from tlptaco.construct_sql.construct_sql.SQLConstructor`, not {type(sqlconstructor)}")

    def __setattr__(self, name, value):
        """
        Redefines the __setattr to make the validations work; the end user DOES NOT CALL THIS DIRECTLY; this is backend Python

        :param name: Name of the variable being passed
        :param value: The value that will be stored in `name`
        """
        try:
            if name in self._validators:
                self._validators[name](value)
                if hasattr(self, 'logger') and self.logger:
                    self.logger.info(f'{self.__class__}.{name} validated')
            super().__setattr__(name, value)
        except UserWarning as e:
            if hasattr(self, 'logger') and self.logger:
                self.logger.warning(f'WARNING {self.__class__}.{name}: {e}')
        except Exception as e:
            if hasattr(self, 'logger') and self.logger:
                self.logger.error(f'{self.__class__}.{name} unable to validate: {e}')
            raise e