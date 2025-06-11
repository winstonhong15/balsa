import collections
import contextlib
import json
from typing import Optional

import duckdb

Result = collections.namedtuple(
    'Result',
    ['result', 'has_timeout', 'latency', 'server_ip'],
)

dsn = '~/data/duckdb/imdb/imdb.db'

@contextlib.contextmanager
def Cursor(dsn=dsn):
    """Get a cursor to local duckdb database."""
    # TODO: create the cursor once per worker node.
    conn = duckdb.connect(dsn)
    try:
        with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


def Execute(sql: str, use_optimizer=False, timeout_ms: Optional[int]=None, cursor: Optional['duckdb.DuckDBPyConnection']=None):
    if cursor is None:
        with Cursor() as cursor:
            return Execute(sql, use_optimizer, timeout_ms=timeout_ms, cursor=cursor)
    if use_optimizer:
        cursor.sql('PRAGMA enable_optimizer')
    else:
        cursor.sql('PRAGMA disable_optimizer')
    # for getting query latency
    cursor.sql("PRAGMA enable_profiling=json")
    cursor.sql("PRAGMA profile_output='output.json'")

    if timeout_ms is not None:
        raise NotImplementedError
    else:
        res = cursor.sql(sql).fetchall()
    latency = __get_executation_time()
    return Result(res, False, latency, '')
    
def __get_executation_time(path='output.json'):
    latency = -1
    with open(path) as file:
        json_raw_content = file.read()
        json_content = json.loads(json_raw_content)
        latency = json_content['latency']
    return latency

if __name__ == '__main__':
    sql = 'SELECT count(1);'
    print(Execute(sql))