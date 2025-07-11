"""
Simple runner to orchestrate multiple SQL executions.
"""
from typing import List
from tlptaco.db.connection import DBConnection

class DBRunner:
    # TODO add timings to logger
    def __init__(self, cfg, logger):
        db = cfg
        self.conn = DBConnection(db.host, db.user, db.password, db.logmech)
        self.logger = logger

    def run(self, sql: str):
        self.logger.info(f"Executing SQL")
        return self.conn.execute(sql)

    def run_many(self, sql_list: List[str]):
        results = []
        for s in sql_list:
            results.append(self.run(s))
        return results

    def to_df(self, sql: str):
        self.logger.info("Fetching data to DataFrame")
        return self.conn.to_df(sql)

    def fastload(self, df, **kwargs):
        self.logger.info("Fastloading DataFrame")
        return self.conn.fastload(df, **kwargs)

    def cleanup(self):
        self.logger.info("Cleaning up DB connection")
        self.conn.disconnect()