"""
Wrap Teradata (and other) connections for SQL execution and data transfer.
"""
import teradatasql
import pandas as pd
from typing import Any

class DBConnection:
    # TODO add some logging outputs OR extend Daniel's connection
    def __init__(self, host: str, user: str, password: str, logmech: str = "KRB5"):
        self.host = host
        self.user = user
        self.password = password
        self.logmech = logmech
        self.conn = None

    def connect(self):
        # Establish a direct teradatasql (DB-API) connection
        conn_kwargs = {
            'host': self.host,
            'user': self.user,
            'password': self.password,
        }
        # logmech is not passed; teradatasql uses default authentication mechanism
        # TODO add KRB5 authentication mechanism
        self.conn = teradatasql.connect(**conn_kwargs)

    def disconnect(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None

    def execute(self, sql: str) -> Any:
        if self.conn is None:
            self.connect()
        cur = self.conn.cursor()
        cur.execute(sql)
        # Commit DDL/DML to the database
        try:
            self.conn.commit()
        except Exception:
            # Some drivers auto-commit or may not support explicit commit
            pass
        return cur

    def to_df(self, sql: str):
        if self.conn is None:
            self.connect()
        # Use pandas to read SQL via DB-API connection
        return pd.read_sql(sql, self.conn)

    def fastload(self, df, **kwargs):
        raise NotImplementedError("fastload is not supported with teradatasql driver")