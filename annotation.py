import logging
import os
from pprint import pprint

import psycopg2
from dotenv import load_dotenv
from mo_sql_parsing import parse

from util import NodeCoverage


def import_config():
    load_dotenv()
    db_name = os.getenv("DB_NAME")
    db_uname = os.getenv("DB_UNAME")
    db_pass = os.getenv("DB_PASS")
    return db_name, db_uname, db_pass


def open_db(db_name, db_uname, db_pass):
    conn = psycopg2.connect(database=db_name, user=db_uname, password=db_pass)
    return conn


def get_query_execution_plan(cursor, sql_query):
    cursor.execute(f"EXPLAIN  (VERBOSE TRUE, COSTS FALSE, FORMAT JSON) {sql_query}")
    return cursor.fetchone()


def convert_query_cond_to_plan_like_cond(query_cond):
    """
    convert condition from parsed query to be like plan condition
    :param query_cond:
    :return:
    """
    comp_ops = {
        'gt': (' > ', ' < '),
        'lt': (' < ', ' > '),
        'eq': (' = ', ' = '),
        'neq': (' <> ', ' <> '),
        'gte': (' >= ', ' <= '),
        'lte': (' <= ', ' >= '),
        'like': (' LIKE ', ' LIKE '),
    }
    for comp_op, comp_op_rep in comp_ops.items():
        if comp_op in query_cond:
            assert len(query_cond[comp_op]) == 2
            return \
                comp_op_rep[0].join(map(str, query_cond[comp_op])), \
                comp_op_rep[1].join(map(str, reversed(query_cond[comp_op])))


def compare_condition(query_cond, plan_cond) -> bool:
    """Compares between condition from a parsed query and condition from a plan.
    This is needed because the format between the two is very different.
    It returns true if the query condition is a subset of the plan condition.
    Example of query_cond:
    'where': {'and': [{'gt': ['n.n_nationkey', 7]},
                       {'lt': ['n.n_nationkey', 15]},
                       {'eq': ['n.n_regionkey', 'r.r_regionkey']},
                       {'eq': ['c.c_nationkey', 'n.n_nationkey']}]}
    Example of plan_cond:
    - '(c.c_nationkey = n.n_nationkey)'
    - '((n.n_nationkey > 7) AND (n.n_nationkey < 15))'
    """
    logging.debug(query_cond)
    logging.debug(plan_cond)
    return any(x in plan_cond for x in convert_query_cond_to_plan_like_cond(query_cond))


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
        else:   # else can try heuristic to recover join condition IF both children are scan
            nc.inc_p()
            yield {
                'Type': 'Join',
                'Subtype': plan['Node Type'],
                'Filter': '',                                   # can also not include
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
    elif plan['Node Type'] == 'Seq Scan':
        nc.inc_p()
        yield {
            'Type': 'Scan',
            'Subtype': plan['Node Type'],
            'Name': plan['Relation Name'],
            'Alias': plan['Alias'],
            'Filter': plan.get('Filter', ''),
        }
    elif plan['Node Type'] == 'Index Scan' or plan['Node Type'] == 'Index Only Scan':
        nc.inc_p()
        yield {
            'Type': 'Scan',
            'Subtype': plan['Node Type'],
            'Name': plan['Relation Name'],
            'Alias': plan['Alias'],
            'Filter': plan.get('Index Cond', ''),
        }
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
        logging.warning(f"WARNING: Unimplemented Node Type{plan['Node Type']}")
        for p in plan['Plans']:
            yield from transverse_plan(p)


def transverse_query(query: dict, plan: dict):
    conj_ops = {'and', 'or'}
    nc = NodeCoverage()
    # TODO: have to first check whether ann already exist or not, act accordingly
    for result in transverse_plan(plan):  # iterate over node in root
        if result['Type'] == 'Join':  # look at WHERE
            # TODO: does not cover NOT
            assert len(query['where'].keys() - {'ann'}) == 1, "dict where len > 1"  # TODO: TEMP FIX
            if result['Filter'] == '':
                possible_cond = []
                for cond in [f'{x} = {y}' for x in result['Possible LHS'] for y in result['Possible RHS']]:
                    if conj_op := conj_ops.intersection(query['where'].keys()):
                        for sub_cond in query['where'][conj_op.pop()]:
                            if compare_condition(sub_cond, cond):
                                nc.inc_q()
                                sub_cond['ann'] = f"{result['Subtype']} on {cond}"
                                possible_cond.append(sub_cond)
                    else:
                        if compare_condition(query['where'], result['Filter']):
                            nc.inc_q()
                            query['where']['ann'] = f"{result['Subtype']} on {cond}"
                            possible_cond.append(query['where'])
                assert len(possible_cond) <= 1, "MORE THAN ONE POSSIBLE CONDITION"
            else:
                if conj_op := conj_ops.intersection(query['where'].keys()):
                    for sub_cond in query['where'][conj_op.pop()]:
                        if compare_condition(sub_cond, result['Filter']):
                            nc.inc_q()
                            sub_cond['ann'] = f"{result['Subtype']} on {result['Filter']}"
                            break
                else:
                    if compare_condition(query['where'], result['Filter']):
                        nc.inc_q()
                        query['where']['ann'] = f"{result['Subtype']} on {result['Filter']}"
        elif result['Type'] == 'Scan':  # look at FROM
            # goto from
            if type(query['from']) is str:
                if query['from'] == result['Name'] and query['from'] == result['Alias']:
                    nc.inc_q()
                    query['from'] = {
                        'value': query['from'],
                        'ann': f"{result['Subtype']} {result['Name']}"
                    }
            elif type(query['from']) is dict:
                if query['from']['value'] == result['Name'] and query['from'].get('name', '') == result['Alias']:
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
                            transverse_query(rel['value'], plan)
                            continue
                        assert type(rel['value']) is str
                        if rel['value'] == result['Name'] and rel.get('name', '') == result['Alias']:
                            nc.inc_q()
                            rel['ann'] = f"{result['Subtype']} {result['Name']} as {result['Alias']}"
                            break
            # if filter exist, goto where
            if result['Filter'] != '':
                assert len(query['where'].keys() - {'ann'}) == 1, "dict where len > 1"  # TODO: TEMP FIX
                if conj_op := conj_ops.intersection(query['where'].keys()):
                    for sub_cond in query['where'][conj_op.pop()]:
                        if compare_condition(sub_cond, result['Filter']):
                            nc.inc_q()
                            sub_cond['ann'] = f"{result['Subtype']} {result['Name']} Filter on {result['Filter']}"
                else:
                    if compare_condition(query['where'], result['Filter']):
                        nc.inc_q()
                        query['where']['ann'] = f"{result['Subtype']} {result['Name']} Filter on {result['Filter']}"


def init_conn(db_name=None):
    if db_name is None:
        db_name, db_uname, db_pass = import_config()
    else:
        _, db_uname, db_pass = import_config()
    conn = open_db(db_name, db_uname, db_pass)
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


def preprocess_query(query):
    return ' '.join([word.lower() if word[0] != '"' and word[0] != "'" else word for word in query.split()])


def main():
    nc = NodeCoverage()
    logging.basicConfig(filename='log/debug.log', filemode='w', level=logging.DEBUG)
    db_name, db_uname, db_pass = import_config()
    conn = open_db(db_name, db_uname, db_pass)
    cur = conn.cursor()

    queries = [
        # Test cases
        "SELECT * FROM nation, region WHERE nation.n_regionkey = region.r_regionkey and nation.n_regionkey = 0;",
        "SELECT * FROM nation, region WHERE nation.n_regionkey < region.r_regionkey and nation.n_regionkey = 0;",
        "SELECT * FROM nation;",
        'select N_NATIONKey, "n_regionkey" from NATion;',
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
        query = preprocess_query(query)   # assume all queries are case insensitive
        logging.debug(query)
        plan = get_query_execution_plan(cur, query)
        parsed_query = parse(query)
        transverse_query(parsed_query, plan[0][0]['Plan'])
        pprint(parsed_query, sort_dicts=False)
        pprint(plan, sort_dicts=False)
        print()

    print(nc)


if __name__ == '__main__':
    main()

# SELECT *
# FROM nation, 					  -> Seq Scan, Filter n_regionkey
#      region  					  -> Index Scan on n_regionkey = 0
# WHERE nation.n_regionkey = region.r_regionkey     -> Nested Loop
# AND
#       nation.n_regionkey = 0			  -> SeqScan, Filter n_regionkey = 0
