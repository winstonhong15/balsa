from balsa.envs.envs import ParseSqlToNode
from balsa.util import plans_lib
import balsa.util.duck_db as duckdb
import pg_executor.pg_executor as pg


if __name__ == '__main__':
    path = 'queries/0a.sql'
    original_sql_str = ''

    with open(path, 'r') as f:
        original_sql_str = f.read()

    node = ParseSqlToNode(path)
    sql = node.rewrite_sql()
    print('SQL for DuckDB: {}'.format(sql))

    print('DuckDB result:', duckdb.Execute(sql, True).result)
    with pg.Cursor() as cursor:
        print('Postgre result:', pg.Execute(original_sql_str, cursor=cursor).result)
