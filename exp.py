import os

from balsa.envs.envs import ParseSqlToNode
from balsa.util import plans_lib
import balsa.util.duck_db as duckdb
import pg_executor.pg_executor as pg

def check_query(path: str, name: str):
    original_sql_str = ''

    with open(path, 'r') as f:
        original_sql_str = f.read()

    node = ParseSqlToNode(path)
    # print(node.hint_str())
    sql = node.generate_duckdb_sql()
    # print('SQL for DuckDB: {}'.format(sql))

    duck_db_res = duckdb.Execute(sql, use_optimizer=True).result
    with pg.Cursor() as cursor:
        pg_res = pg.Execute(sql, cursor=cursor).result
        pg_original_res = pg.Execute(original_sql_str, cursor=cursor).result
        if duck_db_res != pg_res:
            print(f'Query: {name} result not the same. Duck DB res: {duck_db_res} PG res: {pg_res} PG original res: {pg_original_res}')
        elif pg_res != pg_original_res:
            print(f'Query: {name} result may be different. PG res: {pg_res} PG original res: {pg_original_res}')
        else:
            print(f'Query: {name} matched')

if __name__ == '__main__':
    QUERY_ROOT_DIR = os.path.join('queries', 'join-order-benchmark')
    # check_query(os.path.join(QUERY_ROOT_DIR, '9c.sql'), '9c.sql')
    queries = os.listdir(QUERY_ROOT_DIR)
    for filename in sorted(queries):
        if filename.endswith('.sql'):
            path = os.path.join(QUERY_ROOT_DIR, filename)
            check_query(path, filename)
