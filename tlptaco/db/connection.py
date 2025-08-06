"""
Wrap Teradata (and other) connections for SQL execution and data transfer.
"""
import teradatasql
import pandas as pd
import warnings
from typing import Any

class DBConnection:
    """Lightweight Teradata connection wrapper used by :class:`DBRunner`.

    The class sticks to the DB-API surface exposed by the official
    ``teradatasql`` driver so that higher-level code can rely on *standard*
    ``.cursor().execute()`` semantics.

    Example
    -------
    >>> conn = DBConnection(host='rchtera', user='me', password='pw')
    >>> cur = conn.execute('SELECT 1')
    >>> rows = cur.fetchall()
    >>> conn.disconnect()
    """
    # TODO add some logging outputs OR extend Daniel's connection
    def __init__(self, host: str, user: str, password: str, logmech: str = "KRB5"):
        self.host = host
        self.user = user
        self.password = password
        self.logmech = logmech
        self.conn = None

    def connect(self):
        # Establish a direct teradatasql (DB-API) connection
        # Build connection arguments
        conn_kwargs = {
            'host': self.host,
            'user': self.user,
            'password': self.password,
        }
        # Include logmech if explicitly set (None = omit)
        if self.logmech is not None:
            conn_kwargs['logmech'] = self.logmech
        # Establish a direct teradatasql (DB-API) connection
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
        # Suppress pandas warning about non-SQLAlchemy DBAPI2 connections
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=UserWarning,
                message=r"pandas only supports SQLAlchemy connectable.*"
            )
            df = pd.read_sql(sql, self.conn)
        return df

    def fastload(self, df, **kwargs):
        raise NotImplementedError("fastload is not supported with teradatasql driver")
