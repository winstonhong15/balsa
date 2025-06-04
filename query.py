import pprint

import numpy as np
import ray

from balsa.util import plans_lib, postgres

@ray.remote
def ExecuteSql(query_name,
               sql_str,
               hint_str,
               hinted_plan,
               query_node,
               predicted_latency,
               curr_timeout_ms=None,
               found_plans=None,
               predicted_costs=None,
               silent=False,
               is_test=False,
               use_local_execution=True,
               plan_physical=True,
               repeat=1,
               engine='postgres'):
    """Executes a query.

    Returns:
      If use_local_execution:
        A (pg_executor, dbmsx_executor).Result.
      Else:
        A ray.ObjectRef of the above.
    """
    # Unused args.
    del query_name, hinted_plan, query_node, predicted_latency, found_plans,\
        predicted_costs, silent, is_test, plan_physical

    assert engine in ('postgres', 'dbmsx', 'duckdb'), engine
    if engine == 'postgres':
        return postgres.ExplainAnalyzeSql(sql_str,
                                          comment=hint_str,
                                          verbose=False,
                                          geqo_off=True,
                                          timeout_ms=curr_timeout_ms,
                                          remote=not use_local_execution)
    else:
        return DbmsxExecuteSql(sql_str,
                               comment=hint_str,
                               timeout_ms=curr_timeout_ms,
                               remote=not use_local_execution,
                               repeat=repeat)


def AddCommentToSql(sql_str, comment, engine):
    """Adds a comment (hint string) to a SQL string."""
    fns = {
        'postgres': PostgresAddCommentToSql,
        'dbmsx': DbmsxAddCommentToSql,
    }
    return fns[engine](sql_str, comment)


def PostgresAddCommentToSql(sql_str, comment=None):
    """Postgres: <comment> <SELECT ...>."""
    return comment + '\n' + sql_str


def DbmsxAddCommentToSql(sql_str, comment=None):
    raise NotImplementedError


def DbmsxExecuteSql(sql_str,
                    comment=None,
                    timeout_ms=None,
                    remote=True,
                    repeat=1):
    raise NotImplementedError


def DbmsxNodeToHintStr(node, with_physical_hints=False):
    """Converts a plans_lib.Node plan into Dbmsx-compatible hint string."""
    raise NotImplementedError


def HintStr(node, with_physical_hints, engine):
    if engine == 'postgres':
        return node.hint_str(with_physical_hints=with_physical_hints)
    assert engine == 'dbmsx', engine
    return DbmsxNodeToHintStr(node, with_physical_hints=with_physical_hints)


def ParseExecutionResult(result_tup,
                         query_name,
                         sql_str,
                         hint_str,
                         hinted_plan,
                         query_node,
                         predicted_latency,
                         curr_timeout_ms=None,
                         found_plans=None,
                         predicted_costs=None,
                         silent=False,
                         is_test=False,
                         use_local_execution=True,
                         plan_physical=True,
                         repeat=None,
                         engine='postgres'):
    del repeat  # Unused.
    messages = []
    result = result_tup.result
    has_timeout = result_tup.has_timeout
    server_ip = result_tup.server_ip
    if has_timeout:
        assert not result, result
    if engine == 'dbmsx':
        real_cost = -1 if has_timeout else result_tup.latency
    else:
        if has_timeout:
            real_cost = -1
        else:
            json_dict = result[0][0][0]
            real_cost = json_dict['Execution Time']
    if hint_str is not None:
        # Check that the hint has been respected.  No need to check if running
        # baseline.
        do_hint_check = True
        if engine == 'dbmsx':
            raise NotImplementedError
        else:
            if not has_timeout:
                executed_node = postgres.ParsePostgresPlanJson(json_dict)
            else:
                # Timeout has occurred & 'result' is empty.  Fallback to
                # checking against local Postgres.
                print('Timeout occurred; checking the hint against local PG.')
                executed_node, _ = postgres.SqlToPlanNode(sql_str,
                                                          comment=hint_str,
                                                          verbose=False)
            executed_node = plans_lib.FilterScansOrJoins(executed_node)
            executed_hint_str = executed_node.hint_str(
                with_physical_hints=plan_physical)
        if do_hint_check and hint_str != executed_hint_str:
            print('initial\n', hint_str)
            print('after\n', executed_hint_str)
            msg = 'Hint not respected for {}; server_ip={}'.format(
                query_name, server_ip)
            try:
                assert False, msg
            except Exception as e:
                print(e, flush=True)
                import ipdb
                ipdb.set_trace()

    if not silent:
        messages.append('{}Running {}: hinted plan\n{}'.format(
            '[Test set] ' if is_test else '', query_name, hinted_plan))
        messages.append('filters')
        messages.append(pprint.pformat(query_node.info['all_filters']))
        messages.append('')
        messages.append('q{},{:.1f},{}'.format(query_node.info['query_name'],
                                               real_cost, hint_str))
        messages.append(
            '{} Execution time: {:.1f} (predicted {:.1f}) curr_timeout_ms={}'.
            format(query_name, real_cost, predicted_latency, curr_timeout_ms))

    if hint_str is None or silent:
        # Running baseline: don't print debug messages below.
        return result_tup, real_cost, server_ip, '\n'.join(messages)

    messages.append('Expert plan: latency, predicted, hint')
    expert_hint_str = query_node.hint_str()
    expert_hint_str_physical = query_node.hint_str(with_physical_hints=True)
    messages.append('  {:.1f} (predicted {:.1f})  {}'.format(
        query_node.cost, query_node.info['curr_predicted_latency'],
        expert_hint_str))
    if found_plans:
        if predicted_costs is None:
            predicted_costs = [None] * len(found_plans)
        messages.append('SIM-predicted costs, predicted latency, plan: ')
        min_p_latency = np.min([p_latency for p_latency, _ in found_plans])
        for p_cost, found in zip(predicted_costs, found_plans):
            p_latency, found_plan = found
            found_hint_str = found_plan.hint_str()
            found_hint_str_physical = HintStr(found_plan,
                                              with_physical_hints=True,
                                              engine=engine)
            extras = [
                'cheapest' if p_latency == min_p_latency else '',
                '[expert plan]'
                if found_hint_str_physical == expert_hint_str_physical else '',
                '[picked]' if found_hint_str_physical == hint_str else ''
            ]
            extras = ' '.join(filter(lambda s: s, extras)).strip()
            if extras:
                extras = '<-- {}'.format(extras)
            if p_cost:
                messages.append('  {:.1f}  {:.1f}  {}  {}'.format(
                    p_cost, p_latency, found_hint_str, extras))
            else:
                messages.append('          {:.1f}  {}  {}'.format(
                    p_latency, found_hint_str, extras))
    messages.append('-' * 80)
    return result_tup, real_cost, server_ip, '\n'.join(messages)
