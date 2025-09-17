import collections
import concurrent.futures
import contextlib
import json
import os
from pathlib import Path
from typing import Optional

import duckdb

Result = collections.namedtuple(
    'Result',
    ['result', 'has_timeout', 'latency', 'server_ip'],
)

dsn = os.path.join(str(Path.home()), 'duckdb', 'imdb.db')

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


def Execute(sql: str, hinted_plan: Optional[str]=None, use_optimizer=False, timeout_ms: Optional[int]=None, cursor: Optional['duckdb.DuckDBPyConnection']=None):
    if cursor is None:
        with Cursor() as cursor:
            return Execute(sql, hinted_plan, use_optimizer, timeout_ms=timeout_ms, cursor=cursor)

    if timeout_ms is not None:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(Execute, sql, hinted_plan, use_optimizer, timeout_ms=None, cursor=cursor)
            try:
                result = future.result(timeout=timeout_ms / 1000)
                return result
            except concurrent.futures.TimeoutError:
                cursor.interrupt()
                return Result(None, True, -1, '')
    else:
        sql_to_run = sql
        if hinted_plan:
            sql_to_run = hinted_plan
        # print('Execute duckdb query:', sql_to_run)
        if use_optimizer:
            cursor.execute('PRAGMA enable_optimizer')
        else:
            cursor.execute("SET disabled_optimizers = 'join_order,build_side_probe_side'")
        # for getting query latency
        cursor.execute("PRAGMA enable_profiling=json")
        cursor.execute("PRAGMA profile_output='output.json'")
        cursor.execute("SET memory_limit = '32GB'")
        try:
            res = cursor.sql(sql_to_run).fetchall()
        except Exception as e:
            print(e)
            return Result(None, True, -1, '')
    # latency reported by duckdb is in seconds
    latency = __get_executation_time() * 1000
    return Result(res, False, latency, '')
    
def __get_executation_time(path='output.json'):
    latency = -1
    with open(path) as file:
        json_raw_content = file.read()
        json_content = json.loads(json_raw_content)
        latency = json_content['latency']
    return latency

if __name__ == '__main__':
    query = 'SELECT count(1);'
    print(Execute(query))

    timeout_query = "select count(*) from range(1000000000000)" # Long running query
    print(Execute(timeout_query, timeout_ms=100))
