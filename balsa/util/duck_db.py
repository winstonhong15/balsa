import collections
import contextlib
import json
from typing import Optional

import duckdb

Result = collections.namedtuple(
    'Result',
    ['result', 'has_timeout'],
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


def Execute(sql: str, disable_optimizer = True, timeout_ms: Optional[int]=None, cursor: Optional['duckdb.DuckDBPyConnection']=None):
    if cursor is None:
        with Cursor() as cursor:
            return Execute(sql, timeout_ms=timeout_ms, cursor=cursor)
    if disable_optimizer:
        cursor.execute('PRAGMA disable_optimizer')
    else:
        cursor.execute('PRAGMA enable_optimizer')
    # for getting query latency
    cursor.execute("PRAGMA enable_profiling=json")
    cursor.execute("PRAGMA profile_output='output.json'")
    if timeout_ms is not None:
        raise NotImplementedError
    else:
        res = cursor.execute(sql).fetchall()
    __get_executation_time()
    return Result(res, False)
    
def __get_executation_time():
    with open('output.json') as file:
        json_raw_content = file.read()
        json_content = json.loads(json_raw_content)
        print(json_content['latency'])

if __name__ == '__main__':
    sql = 'SELECT count(1);'
    print(Execute(sql))