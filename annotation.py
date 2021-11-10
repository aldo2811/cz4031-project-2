import logging
import os
import typing
from pprint import pprint, pformat

import psycopg2
from dotenv import load_dotenv
from mo_sql_parsing import parse

from util import NodeCoverage


def import_config():
    load_dotenv()
    db_name = os.getenv("DB_NAME")
    db_uname = os.getenv("DB_UNAME")
    db_pass = os.getenv("DB_PASS")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    return db_name, db_uname, db_pass, db_host, db_port


def open_db(db_name, db_uname, db_pass, db_host, db_port):
    conn = psycopg2.connect(database=db_name, user=db_uname, password=db_pass, host=db_host, port=db_port)
    return conn


def get_query_execution_plan(cursor, sql_query):
    cursor.execute(f"EXPLAIN  (VERBOSE TRUE, COSTS FALSE, FORMAT JSON) {sql_query}")
    return cursor.fetchone()


def transverse_plan(plan):
    nc = NodeCoverage()
    logging.debug(f"now in {plan['Node Type']}")
    if plan['Node Type'] == 'Nested Loop':
        assert len(plan['Plans']) == 2, "Length of Plans is more than two."
        if 'Join Filter' in plan:
            nc.inc_p()
            yield {
                'Type': 'Join',
                'Subtype': plan['Node Type'],
                'Filter': plan['Join Filter'],
            }
        else:  # else can try heuristic to recover join condition IF both children are scan
            nc.inc_p()
            yield {
                'Type': 'Join',
                'Subtype': plan['Node Type'],
                'Filter': '',  # can also not include
                'Possible LHS': plan['Plans'][0]['Output'],
                'Possible RHS': plan['Plans'][1]['Output'],
            }
        yield from transverse_plan(plan['Plans'][0])
        yield from transverse_plan(plan['Plans'][1])
    elif plan['Node Type'] == 'Hash Join':
        nc.inc_p()
        yield {
            'Type': 'Join',
            'Subtype': plan['Node Type'],
            'Filter': plan['Hash Cond'],
        }
        assert len(plan['Plans']) == 2, "Length of Plans is more than two."
        yield from transverse_plan(plan['Plans'][0])
        yield from transverse_plan(plan['Plans'][1])
    elif plan['Node Type'] == 'Merge Join':
        nc.inc_p()
        yield {
            'Type': 'Join',
            'Subtype': plan['Node Type'],
            'Filter': plan['Merge Cond'],
        }

        for p in plan['Plans']:
            yield from transverse_plan(p)
    elif plan['Node Type'] == 'Seq Scan':
        nc.inc_p()
        yield {
            'Type': 'Scan',
            'Subtype': plan['Node Type'],
            'Name': plan['Relation Name'],
            'Alias': plan['Alias'],
            'Filter': plan.get('Filter', ''),
        }
    elif plan['Node Type'] in ['Index Scan', 'Index Only Scan']:
        def _f():
            if 'Index Cond' in plan:
                yield plan['Index Cond']
            if 'Filter' in plan:
                yield plan['Filter']

        nc.inc_p()
        yield {
            'Type': 'Scan',
            'Subtype': plan['Node Type'],
            'Name': plan['Relation Name'],
            'Alias': plan['Alias'],
            'Filter': ' AND '.join(_f()),
            # 'Filter': ' AND'.join([plan.get('Index Cond', None), plan.get('Filter', None)]),
        }

    elif plan['Node Type'] == 'Bitmap Index Scan':
        nc.inc_p()
        yield {
            'Type': 'Scan',
            'Subtype': plan['Node Type'],
            'Name': plan['Index Name'],
            'Alias': '',
            'Filter': plan.get('Index Cond', ''),
        }
    elif plan['Node Type'] == 'Bitmap Heap Scan':
        nc.inc_p()
        yield {
            'Type': 'Scan',
            'Subtype': plan['Node Type'],
            'Name': plan['Relation Name'],
            'Alias': plan['Alias'],
            'Filter': plan.get('Filter', ''),
        }
        for p in plan['Plans']:
            yield from transverse_plan(p)
    elif plan['Node Type'] == 'Hash':
        nc.inc_t()
        assert len(plan['Plans']) == 1, "Length of Plans of Type Hash is more than one."
        yield from transverse_plan(plan['Plans'][0])
    elif plan['Node Type'] == 'Unique':
        nc.inc_t()
        assert len(plan['Plans']) == 1, "Length of Plans of Type Unique is more than one."
        yield from transverse_plan(plan['Plans'][0])
    elif plan['Node Type'] == 'Materialize':
        nc.inc_t()
        assert len(plan['Plans']) == 1, "Length of Plans of Type Materialize is more than one."
        yield from transverse_plan(plan['Plans'][0])
    else:
        nc.inc_t()
        logging.warning(f"WARNING: Unimplemented Node Type {plan['Node Type']}")
        for p in plan['Plans']:
            yield from transverse_plan(p)


def format_ann(result: dict, use_alias=False):
    if result['Type'] == 'Join':
        return f"{result['Subtype']} on {result['Filter']}"
    elif result['Type'] == 'Scan':
        return f"Filtered on {result['Subtype']} of {result['Name']}"


def parse_expr_node(query: dict, result: dict) -> bool:
    # logging.info(f'query={query}, result={result}')
    """
    :param query:
    :param result:
    :return:
    """
    if 'ann' in query.keys():
        return False
    comp_ops = {
        'gt': (' > ', ' < '),
        'lt': (' < ', ' > '),
        'eq': (' = ', ' = '),
        'neq': (' <> ', ' <> '),
        'gte': (' >= ', ' <= '),
        'lte': (' <= ', ' >= '),
        'like': (' ~~ ', ' ~~ '),  # TODO: (((part.p_type)::text ~~ '%%BRASS'::text)
        'not_like': (' !~~ ', ' !~~ '),
    }
    op = list(query.keys())[0]
    if op == 'and' or op == 'or':
        res = False
        for subq in query[op]:
            if type(subq) is dict:
                # TODO: If we return early, some condition in query might not be tagged.
                #       If we return late, we will visit unneeded node, try to improve.
                res |= parse_expr_node(subq, result)
            else:
                raise NotImplementedError(f'{subq}')
        return res
    elif op in comp_ops:
        """
        ((lineitem.l_shipdate >= '1994-01-01'::date) 
        (lineitem.l_shipdate < '1995-01-01 00:00:00'::timestamp without time zone)

        {'gte': ['lineitem.l_shipdate', {'literal': '1994-01-01'}]}
        {'lt': ['lineitem.l_shipdate', {'add': [{'date': {'literal': '1994-01-01'}}, {'interval': [1, 'year']}]}]}
        """
        arr = []
        for subq in query[op]:
            if type(subq) is str:
                arr.append(subq)
            elif type(subq) in [int, float]:
                # TODO: handle numeric
                arr.append(str(subq))
            elif type(subq) is dict:
                if 'literal' in subq:
                    arr.append(f"'{subq['literal']}'")
                elif 'date' in subq:
                    arr.append(f"'{subq['date']['literal']}'")
                    pass
                elif len(subq.keys() & {'sub', 'add'}) > 0:
                    arr.append('$')
                    pass  # TODO: should recurse to calculator
                else:
                    arr.append('$')
                    find_query_node(subq, result)
                    pass  # TODO: should be recursing to find_query_node
            else:
                raise NotImplementedError(f'{subq}')
        exp = (comp_ops[op][0].join(arr), comp_ops[op][1].join(reversed(arr)))
        if any(x in result['Filter'] for x in exp):
            query['ann'] = format_ann(result)
            return True
        else:
            return False
    elif op == 'between':
        """
        {'between': ['lineitem.l_discount', {'sub': [0.06, 0.01]}, {'add': [0.06, 0.01]}]}
        (lineitem.l_discount >= 0.05) AND (lineitem.l_discount <= 0.07)
        """
        # TODO: WRONG
        return parse_expr_node({
            'and': [{'lt': [query[op][1], query[op][0]]},
                    {'lt': [query[op][0], query[op][2]]}]
        }, result)
        pass
    elif op == 'exists':
        find_query_node(query[op], result)
        return True
    elif op == 'not':
        return parse_expr_node(query[op], result)
    elif op in ['in', 'nin']:
        if type(query[op][1]) is dict:
            if 'literal' in query[op][1]:
                # If with literal array
                # LHS = ANY('{13,31,23,29,30,18,17}'::text[])
                pass
            else:
                # If with subquery, become equijoin
                find_query_node(query[op][1], result)
        elif type(query[op][1]) is list:
            assert type(query[op][1][0]) in [str, int, float]
            # LHS = ANY('{49,14,23,45,19,3,36,9}'::integer[])
        return False
    else:
        raise NotImplementedError(f'{op}')


def find_query_node(query: dict, result: dict):
    # logging.info(f'query={query}, result={result}')
    nc = NodeCoverage()
    if result['Type'] == 'Join':  # look at WHERE
        if 'where' in query:
            if result['Filter'] == '':
                # For Nested Loop without explicit Filter, we try to find the condition by matching column names
                possible_cond = []
                for cond in [f'{x} = {y}' for x in result['Possible LHS'] for y in result['Possible RHS']]:
                    result['Filter'] = cond
                    if parse_expr_node(query['where'], result):
                        possible_cond.append(cond)
            else:
                parse_expr_node(query['where'], result)
            # _find_query_node_where(query['where'], result)
        # TODO: return when annotated already
        if type(query['from']) is dict and type(query['from']['value']) is dict:
            find_query_node(query['from']['value'], result)
        if type(query['from']) is list:
            for v in query['from']:
                if type(v) is dict and type(v['value']) is dict:
                    find_query_node(v['value'], result)
    elif result['Type'] == 'Scan':  # look at FROM
        # TODO: BUG multiple from statement are assigned the same scan statement
        # goto from
        if type(query['from']) is str:
            if query['from'] == result['Name'] and query['from'] == result['Alias']:
                nc.inc_q()
                query['from'] = {
                    'value': query['from'],
                    'ann': f"{result['Subtype']} {result['Name']}"
                }
        elif type(query['from']) is dict:
            if type(query['from']['value']) is dict:
                find_query_node(query['from']['value'], result)
            elif type(query['from']['value']) is str and query['from']['value'] == result['Name'] and query['from'].get(
                    'name', '') == result['Alias']:
                nc.inc_q()
                query['from']['ann'] = f"{result['Subtype']} {result['Name']} as {result['Alias']}"
        elif type(query['from']) is list:
            for i, rel in enumerate(query['from']):
                if type(rel) is str:
                    if rel == result['Name'] and rel == result['Alias']:
                        nc.inc_q()
                        query['from'][i] = {
                            'value': rel,
                            'ann': f"{result['Subtype']} {result['Name']}"
                        }
                        break
                else:
                    if type(rel['value']) is dict:
                        find_query_node(rel['value'], result)
                        continue
                    assert type(rel['value']) is str
                    if rel['value'] == result['Name'] and rel.get('name', '') == result['Alias']:
                        nc.inc_q()
                        rel['ann'] = f"{result['Subtype']} {result['Name']} as {result['Alias']}"
                        break
        # if filter exist, goto where
        if result['Filter'] != '':
            if 'where' in query:
                parse_expr_node(query['where'], result)


def transverse_query(query: dict, plan: dict):
    # TODO: have to first check whether ann already exist or not, act accordingly
    for result in transverse_plan(plan):  # iterate over node in root
        find_query_node(query, result)


def init_conn(db_name=None):
    if db_name is None:
        db_name, db_uname, db_pass, db_host, db_port = import_config()
    else:
        _, db_uname, db_pass, db_host, db_port = import_config()
    conn = open_db(db_name, db_uname, db_pass, db_host, db_port)
    return conn


def process(conn, query):
    """
    process given query, returned formatted query with its annotation
    :param conn:
    :param query:
    :return: formatted_query, annotation
    """
    cur = conn.cursor()
    plan = get_query_execution_plan(cur, query)
    parsed_query = parse(query)
    transverse_query(parsed_query, plan)
    # TODO: convert parsed_query to line-break separated query and annotation
    return "QUERY", "ANN"


def preprocess_query_string(query):
    return ' '.join([word.lower() if word[0] != '"' and word[0] != "'" else word for word in query.split()])


def collect_relation_list(query_tree, rel_list):
    if type(query_tree['from']) is str:
        rel_list.append(query_tree['from'])
    elif type(query_tree['from']) is dict:
        if type(query_tree['from']['value']) is str:
            rel_list.append(query_tree['from']['value'])
        elif type(query_tree['from']['value']) is dict:
            collect_relation_list(query_tree['from']['value'], rel_list)
        else:
            raise NotImplementedError(f"{query_tree['from']['value']}")
    elif type(query_tree['from']) is list:
        for rel in query_tree['from']:
            if type(rel) is str:
                rel_list.append(rel)
            elif type(rel) is dict:
                if type(rel['value']) is str:
                    rel_list.append(rel['value'])
                elif type(rel['value']) is dict:
                    collect_relation_list(rel['value'], rel_list)
                else:
                    raise NotImplementedError(f"{rel['value']}")


def rename_column_to_full_name(query_tree: typing.Union[dict, list], column_relation_dict: dict):
    if type(query_tree) is dict:
        for key, val in query_tree.items():
            if key in ['literal', 'interval']:
                continue
            if type(val) is str:
                if '.' not in val and val in column_relation_dict and len(column_relation_dict[val]) == 1:
                    query_tree[key] = f'{column_relation_dict[val][0]}.{val}'
            elif type(val) not in [int, float]:
                rename_column_to_full_name(val, column_relation_dict)
    elif type(query_tree) is list:
        for i, v in enumerate(query_tree):
            if type(v) is str:
                if '.' not in v and v in column_relation_dict and len(column_relation_dict[v]) == 1:
                    query_tree[i] = f'{column_relation_dict[v][0]}.{v}'
            elif type(v) not in [int, float]:
                rename_column_to_full_name(v, column_relation_dict)
    else:
        raise NotImplementedError(f"{query_tree}")


def preprocess_query_tree(cur, query_tree):
    rel_list = []
    column_relation_dict = {}
    collect_relation_list(query_tree, rel_list)
    logging.debug(f'rel_list={rel_list}')
    # Collect column info
    for rel in rel_list:
        cur.execute(f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{rel}'")
        res = cur.fetchall()
        # pprint(res)
        for col in res:
            if col in column_relation_dict:
                column_relation_dict[col[0]].append(rel)
            else:
                column_relation_dict[col[0]] = [rel]
    logging.debug(f'column_relation_dict={column_relation_dict}')
    # For every column, if no dot, try to find in dict, if multiple relation raise exception, else rename
    rename_column_to_full_name(query_tree, column_relation_dict)


def main():
    nc = NodeCoverage()
    logging.basicConfig(filename='log/debug.log', filemode='w', level=logging.DEBUG)
    db_name, db_uname, db_pass, db_host, db_port = import_config()
    conn = open_db(db_name, db_uname, db_pass, db_host, db_port)
    cur = conn.cursor()

    queries = [
        # Test cases
        "SELECT * FROM nation, region WHERE nation.n_regionkey = region.r_regionkey and nation.n_regionkey = 0;",
        "SELECT * FROM nation, region WHERE nation.n_regionkey < region.r_regionkey and nation.n_regionkey = 0;",
        "SELECT * FROM nation;",
        'select N_NATIONKey, "n_regionkey" from NATion;',
        'select N_NATIONKey from NATion;',
        "SELECT * FROM nation as n1, nation as n2 WHERE n1.n_regionkey = n2.n_regionkey;",
        "SELECT * FROM nation as n1, nation as n2 WHERE n1.n_regionkey < n2.n_regionkey;",
        "SELECT * FROM nation as n1, nation as n2 WHERE n1.n_regionkey <> n2.n_regionkey;",
        "SELECT * FROM nation as n WHERE 0 < n.n_regionkey  and n.n_regionkey < 3;",
        "SELECT * FROM nation as n WHERE 0 < n.n_nationkey  and n.n_nationkey < 30;",
        "SELECT n.n_nationkey FROM nation as n WHERE 0 < n.n_nationkey  and n.n_nationkey < 30;",
        "SELECT * FROM customer as c, (SELECT * FROM nation as n where n.n_nationkey > 7 and n.n_nationkey < 15) as n, region as r WHERE n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",
        "SELECT * FROM customer as c, nation as n, region as r WHERE n.n_nationkey > 7 and n.n_nationkey < 15 and  n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",
        "SELECT * FROM customer as c, (SELECT * FROM nation as n where n.n_regionkey=0) as n, region as r WHERE n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",
        "SELECT * FROM customer as c, (SELECT * FROM nation as n where n.n_regionkey<5) as n, region as r WHERE n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",
        "SELECT  DISTINCT c.c_custkey FROM customer as c, (SELECT * FROM nation as n where n.n_regionkey=0) as n, region as r WHERE n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",

        # http://www.qdpma.com/tpch/TPCH100_Query_plans.html
        """SELECT L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY) AS SUM_QTY,
 SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS SUM_DISC_PRICE,
 SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)) AS SUM_CHARGE, AVG(L_QUANTITY) AS AVG_QTY,
 AVG(L_EXTENDEDPRICE) AS AVG_PRICE, AVG(L_DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER
FROM LINEITEM
WHERE L_SHIPDATE <= date '1998-12-01' + interval '-90 day'
GROUP BY L_RETURNFLAG, L_LINESTATUS
ORDER BY L_RETURNFLAG,L_LINESTATUS""",
        """SELECT S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT
FROM PART, SUPPLIER, PARTSUPP, NATION, REGION
WHERE P_PARTKEY = PS_PARTKEY AND S_SUPPKEY = PS_SUPPKEY AND P_SIZE = 15 AND
P_TYPE LIKE '%%BRASS' AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY AND
R_NAME = 'EUROPE' AND
PS_SUPPLYCOST = (SELECT MIN(PS_SUPPLYCOST) FROM PARTSUPP, SUPPLIER, NATION, REGION
 WHERE P_PARTKEY = PS_PARTKEY AND S_SUPPKEY = PS_SUPPKEY
 AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY AND R_NAME = 'EUROPE')
ORDER BY S_ACCTBAL DESC, N_NAME, S_NAME, P_PARTKEY
LIMIT 100;""",
        # Test cases too hard to do
        # "SELECT * FROM nation as n1, (SELECT * FROM nation as n1) as n2 WHERE n1.n_regionkey = n2.n_regionkey;",
        # "SELECT * FROM nation as n1, (SELECT n1.n_regionkey FROM nation as n1) as n2 WHERE n1.n_regionkey = n2.n_regionkey;",
    ]

    for query in queries:
        print("==========================")
        query = preprocess_query_string(query)  # assume all queries are case insensitive
        logging.debug(query)
        plan = get_query_execution_plan(cur, query)
        parsed_query = parse(query)
        try:
            preprocess_query_tree(cur, parsed_query)
            transverse_query(parsed_query, plan[0][0]['Plan'])
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.debug(pformat(query))
            logging.debug(pformat(parsed_query))
            logging.debug(pformat(plan))
            raise e
        else:
            pprint(parsed_query, sort_dicts=False)
            pprint(plan, sort_dicts=False)
        print()

    print(nc)
    cur.close()


if __name__ == '__main__':
    main()

# SELECT *
# FROM nation, 					  -> Seq Scan, Filter n_regionkey
#      region  					  -> Index Scan on n_regionkey = 0
# WHERE nation.n_regionkey = region.r_regionkey     -> Nested Loop
# AND
#       nation.n_regionkey = 0			  -> SeqScan, Filter n_regionkey = 0
