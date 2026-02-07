import os
import pandas as pd
import pymysql

from pymysql.cursors import DictCursor


def get_conn():
    cfg = {
        'host': os.getenv('MARIA_HOST', 'localhost'),
        'port': int(os.getenv('MARIA_PORT', 3306)),
        'user': os.getenv('MARIA_USER', 'root'),
        'password': os.getenv('MARIA_PASSWORD', ''),
        'db': os.getenv('MARIA_DB', ''),
        'charset': 'utf8mb4',
        'cursorclass': DictCursor,
    }
    return pymysql.connect(**cfg)


def run_query(query, params=None, tables_priority=None):
    """Run a parameterized query against MariaDB.

    - `query` can contain a `{table_path}` placeholder which will be replaced by
      each candidate table name in `tables_priority` in order.
    - `params` should be a list/tuple of values for `%s` placeholders in the query.
    - Returns (DataFrame, used_table_name) or (empty DataFrame, None).
    """
    tables_priority = tables_priority or [None]
    last_error = None
    for table in tables_priority:
        q = query.format(table_path=table) if table else query
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(q, params or [])
                    rows = cur.fetchall()
                    if rows:
                        return pd.DataFrame(rows), table
        except Exception as exc:
            last_error = exc
            continue
    if last_error:
        raise last_error
    return pd.DataFrame(), None
