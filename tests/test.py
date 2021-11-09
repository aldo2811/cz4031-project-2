import pytest
from annotation import *


@pytest.mark.parametrize("query", [
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
    "SELECT * FROM nation WHERE n_nationkey = (SELECT max(n_nationkey) FROM nation);",
    "SELECT * FROM supplier WHERE s_nationkey IN (SELECT n_nationkey FROM nation WHERE n_regionkey = 3);",
    "SELECT n.n_nationkey FROM nation as n WHERE 0 < n.n_nationkey  and n.n_nationkey < 30;",
    "SELECT * FROM customer as c, (SELECT * FROM nation as n where n.n_nationkey > 7 and n.n_nationkey < 15) as n, region as r WHERE n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",
    "SELECT * FROM customer as c, nation as n, region as r WHERE n.n_nationkey > 7 and n.n_nationkey < 15 and  n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",
    "SELECT * FROM customer as c, (SELECT * FROM nation as n where n.n_regionkey=0) as n, region as r WHERE n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",
    "SELECT * FROM customer as c, (SELECT * FROM nation as n where n.n_regionkey<5) as n, region as r WHERE n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",
    "SELECT  DISTINCT c.c_custkey FROM customer as c, (SELECT * FROM nation as n where n.n_regionkey=0) as n, region as r WHERE n.n_regionkey = r.r_regionkey  and c.c_nationkey = n.n_nationkey;",
    "SELECT * FROM customer, (SELECT * FROM nation, region WHERE n_regionkey = r_regionkey) as nr WHERE c_nationkey = n_regionkey;",
    "SELECT * FROM (SELECT * FROM nation, region WHERE n_regionkey = r_regionkey) as nr;",
    "SELECT * FROM (SELECT * FROM nation, region WHERE nation.n_regionkey = region.r_regionkey) as nr;",
    "SELECT * FROM customer, nation, region WHERE n_nationkey > 7 and n_nationkey < 15 and  n_regionkey = r_regionkey  and c_nationkey = n_nationkey;",

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
    """SELECT L_ORDERKEY, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS REVENUE, O_ORDERDATE, O_SHIPPRIORITY
FROM CUSTOMER, ORDERS, LINEITEM
WHERE C_MKTSEGMENT = 'BUILDING' AND C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND
O_ORDERDATE < '1995-03-15' AND L_SHIPDATE > '1995-03-15'
GROUP BY L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
ORDER BY REVENUE DESC, O_ORDERDATE LIMIT 10""",
    """SELECT O_ORDERPRIORITY, COUNT(*) AS ORDER_COUNT FROM ORDERS
WHERE O_ORDERDATE < (date '1993-07-01' + interval '3 day') AND O_ORDERDATE >= date '1993-07-01' 
AND EXISTS (SELECT * FROM LINEITEM WHERE L_ORDERKEY = O_ORDERKEY AND L_COMMITDATE < L_RECEIPTDATE)
GROUP BY O_ORDERPRIORITY
ORDER BY O_ORDERPRIORITY;""",
    # Test cases too hard to do
    # "SELECT * FROM nation as n1, (SELECT * FROM nation as n1) as n2 WHERE n1.n_regionkey = n2.n_regionkey;",
    # "SELECT * FROM nation as n1, (SELECT n1.n_regionkey FROM nation as n1) as n2 WHERE n1.n_regionkey = n2.n_regionkey;",
    ])
def test_query(query):
    db_name, db_uname, db_pass, db_host, db_port = import_config()
    conn = open_db(db_name, db_uname, db_pass, db_host, db_port)
    cur = conn.cursor()

    query = preprocess_query_string(query)  # assume all queries are case insensitive
    logging.debug(query)
    plan = get_query_execution_plan(cur, query)
    parsed_query = parse(query)
    try:
        preprocess_query_tree(cur, parsed_query)
        transverse_query(parsed_query, plan[0][0]['Plan'])
    except Exception as e:
        logging.error(e, exc_info=True)
        logging.debug(query)
        logging.debug(parsed_query)
        logging.debug(plan)
        raise e
    else:
        pprint(parsed_query, sort_dicts=False)
        pprint(plan, sort_dicts=False)