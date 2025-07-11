from tlptaco.logging.logging import CustomLogger, call_logger

class TrackSQL:
    """
    Tracks the tables created by the processes and handles clean up when they are finished
    This is a backend module that can be used separately, but is intended to be used by the connection h
    andlers in tlptaco.connections
    """
    def __init__(self, handler, logger: CustomLogger):
        """
        :param handler: the instance of TeradataHandler that created this instance of TrackSQL
        :param logger: the instance of CustomLogger that is used for logging during the process
        """
        self.handler = handler
        self.tracked_tables = []
        self.logger = logger

    def track_table(self, table_name: str) -> None:
        """
        Saves the name of a SQL table; the purpose is to save user_work tables that are created through
        tlptaco.connections so that they can be dropped later

        :param table_name: the schema_name.table_name that is being tracked
        :return: None
        """
        if table_name not in self.tracked_tables:
            self.tracked_tables.append(table_name)

    @call_logger()
    def clean_up(self) -> None:
        """
        Drops all the tables that are being tracked

        :returns: None
        """
        for table in self.tracked_tables:
            try:
                query = f"DROP TABLE {table}"
                self.handler.execute_query(query)
                self.logger.info(f"Table {table} dropped successfully.")
            except Exception as e:
                self.logger.warning(f"Failed to drop table {table}: {e}")