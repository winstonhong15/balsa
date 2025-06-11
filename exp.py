from balsa.envs.envs import ParseSqlToNode
from balsa.util import plans_lib
from balsa.util.duck_db import Execute

def ParsePostgresPlanJsonNew(json_dict):
    """Takes JSON dict, parses into a Node."""
    curr = json_dict['Plan']

    def _parse_pg(json_dict, indent=0):
        op = json_dict['Node Type']
        cost = json_dict['Total Cost']

        # Record relevant info.
        curr_node = plans_lib.Node(op)
        curr_node.cost = cost
        # Only available if 'analyze' is set (actual execution).
        curr_node.actual_time_ms = json_dict.get('Actual Total Time')
        if 'Relation Name' in json_dict:
            curr_node.table_name = json_dict['Relation Name']
            curr_node.table_alias = json_dict['Alias']

        if op == 'Aggregate':
            op = json_dict['Partial Mode'] + op
            if 'Partial' not in json_dict['Partial Mode']:
                # Record the SELECT <select_exprs> at the topmost Aggregate.
                # E.g., ['min(mi.info)', 'min(miidx.info)', 'min(t.title)'].
                curr_node.info['select_exprs'] = json_dict['Output']

        # Unary predicate on a table.
        if 'Filter' in json_dict:
            assert 'Scan' in op, json_dict
            assert 'Relation Name' in json_dict, json_dict
            curr_node.info['filter'] = json_dict['Filter']

        if 'Scan' in op:
            curr_node.info['select_exprs'] = json_dict['Output']

        # Recurse.
        if 'Plans' in json_dict:
            for n in json_dict['Plans']:
                curr_node.children.append(
                    _parse_pg(n, indent=indent + 2))

        # Special case.
        if op == 'Bitmap Heap Scan':
            for c in curr_node.children:
                if c.node_type == 'Bitmap Index Scan':
                    # 'Bitmap Index Scan' doesn't have the field 'Relation Name'.
                    c.table_name = curr_node.table_name
                    c.table_alias = curr_node.table_alias

        return curr_node

    return _parse_pg(curr)


def to_sql(node: plans_lib.Node):
    children = []
    for child in node.children:
        res = to_sql(child)
        if res != '':
            children.append(res)
    sql = ''
    if node.IsScan():
        select_str = ', '.join(node.info['select_exprs']) if node.info.get('select_exprs') else '*'
        # filter_str = node.info.get('filter')
        # print(filter_str)
        from_str = node.get_table_id()
        sql = 'SELECT {} FROM {}'.format(select_str, from_str)
    elif node.IsJoin():
        print('JOIN')
    elif node.IsFullAggregate():
        select_str = ', '.join(node.info['select_exprs']) if node.info.get('select_exprs') else '*'
        sql = 'SELECT {} FROM '.format(select_str)
    print(children)
    if (len(children) == 1) and sql:
        sql += '({})'.format(children[0])
    return sql


if __name__ == '__main__':
    path = 'queries/0a.sql'

    node = ParseSqlToNode(path, parser=ParsePostgresPlanJsonNew)

    # sql = node_to_sql(node)
    # print(sql)

    print(node.to_str(with_cost=False, verbose=True))

    sql = to_sql(node)
    print(sql)
    print(Execute(sql, True))

    # print(node.GetFilters())
    # print(node.info['parsed_join_conds'])
    # print(node.to_sql(node.info['parsed_join_conds'], with_filters=True, with_select_exprs=True))
