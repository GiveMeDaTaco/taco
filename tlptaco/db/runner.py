"""
Simple runner to orchestrate multiple SQL executions.
"""
import time
from typing import List
from tlptaco.db.connection import DBConnection

class DBRunner:
    # TODO add timings to logger
    def __init__(self, cfg, logger):
        db = cfg
        self.conn = DBConnection(db.host, db.user, db.password, db.logmech)
        self.logger = logger

    def run(self, sql: str):
        """
        Execute a SQL statement, log the SQL text and timing, and return the cursor.
        """
        start = time.time()
        self.logger.info("Executing SQL:")
        self.logger.info(sql)
        cur = self.conn.execute(sql)
        duration = time.time() - start
        self.logger.info(f"SQL execution finished in {duration:.2f}s")
        return cur

    def run_many(self, sql_list: List[str]):
        results = []
        for s in sql_list:
            results.append(self.run(s))
        return results

    def to_df(self, sql: str):
        """
        Execute a SQL query and return a pandas DataFrame, logging SQL text, timing, and shape.
        """
        start = time.time()
        self.logger.info("Fetching data to DataFrame:")
        self.logger.info(sql)
        df = self.conn.to_df(sql)
        duration = time.time() - start
        try:
            rows, cols = df.shape
            self.logger.info(f"Fetched DataFrame with {rows} rows and {cols} columns in {duration:.2f}s")
        except Exception:
            self.logger.info(f"Fetched DataFrame in {duration:.2f}s")
        return df

    def fastload(self, df, **kwargs):
        self.logger.info("Fastloading DataFrame")
        return self.conn.fastload(df, **kwargs)

    def cleanup(self):
        self.logger.info("Cleaning up DB connection")
        self.conn.disconnect()