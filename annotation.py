import logging
import os
from pprint import pprint, pformat

import psycopg2
from dotenv import load_dotenv
from mo_sql_parsing import parse, format

from preprocessing import preprocess_query_string, preprocess_query_tree


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
    conn.set_session(readonly=True, autocommit=True)
    return conn


def get_query_execution_plan(cursor, sql_query):
    cursor.execute(f"EXPLAIN  (VERBOSE TRUE, COSTS FALSE, FORMAT JSON) {sql_query}")
    return cursor.fetchone()


def transverse_plan(plan):
    logging.debug(f"now in {plan['Node Type']}")
    if plan['Node Type'] == 'Nested Loop':
        assert len(plan['Plans']) == 2, "Length of Plans is more than two."
        if 'Join Filter' in plan:
            yield {
                'Type': 'Join',
                'Subtype': plan['Node Type'],
                'Filter': plan['Join Filter'],
            }
        else:  # else can try heuristic to recover join condition IF both children are scan
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
        yield {
            'Type': 'Join',
            'Subtype': plan['Node Type'],
            'Filter': plan['Hash Cond'],
        }
        assert len(plan['Plans']) == 2, "Length of Plans is more than two."
        yield from transverse_plan(plan['Plans'][0])
        yield from transverse_plan(plan['Plans'][1])
    elif plan['Node Type'] == 'Merge Join':
        yield {
            'Type': 'Join',
            'Subtype': plan['Node Type'],
            'Filter': plan['Merge Cond'],
        }

        for p in plan['Plans']:
            yield from transverse_plan(p)
    elif plan['Node Type'] == 'Seq Scan':
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

        yield {
            'Type': 'Scan',
            'Subtype': plan['Node Type'],
            'Name': plan['Relation Name'],
            'Alias': plan['Alias'],
            'Filter': ' AND '.join(_f()),
            # 'Filter': ' AND'.join([plan.get('Index Cond', None), plan.get('Filter', None)]),
        }

    elif plan['Node Type'] == 'Bitmap Index Scan':
        yield {
            'Type': 'Scan',
            'Subtype': plan['Node Type'],
            'Name': plan['Index Name'],
            'Alias': '',
            'Filter': plan.get('Index Cond', ''),
        }
    elif plan['Node Type'] == 'Bitmap Heap Scan':
        yield {
            'Type': 'Scan',
            'Subtype': plan['Node Type'],
            'Name': plan['Relation Name'],
            'Alias': plan['Alias'],
            'Filter': plan.get('Filter', ''),
        }
        for p in plan['Plans']:
            yield from transverse_plan(p)
    else:
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
        'like': (' ~~ ', ' ~~ '),
        'not_like': (' !~~ ', ' !~~ '),
    }
    op = list(query.keys())[0]
    if op == 'and' or op == 'or':
        res = False
        for subq in query[op]:
            if type(subq) is dict:
                res |= parse_expr_node(subq, result)
            else:
                raise NotImplementedError(f'{subq}')
        if res:
            query['expand'] = True
        return res
    elif op in comp_ops:
        """
        ((lineitem.l_shipdate >= '1994-01-01'::date) 
        (lineitem.l_shipdate < '1995-01-01 00:00:00'::timestamp without time zone)

        {'gte': ['lineitem.l_shipdate', {'literal': '1994-01-01'}]}
        {'lt': ['lineitem.l_shipdate', {'add': [{'date': {'literal': '1994-01-01'}}, {'interval': [1, 'year']}]}]}
        """
        arr = []
        annotated = False
        for subq in query[op]:
            if type(subq) is str:
                arr.append(subq)
            elif type(subq) in [int, float]:
                arr.append(str(subq))
            elif type(subq) is dict:
                if 'literal' in subq:
                    arr.append(f"'{subq['literal']}'")
                elif 'date' in subq:
                    arr.append(f"'{subq['date']['literal']}'")
                elif len(subq.keys() & {'sub', 'add'}) > 0:
                    arr.append('$')
                else:
                    arr.append('$')
                    if find_query_node(subq, result):
                        query['expand'] = True
                        annotated = True
            else:
                raise NotImplementedError(f'{subq}')
        exp = (comp_ops[op][0].join(arr), comp_ops[op][1].join(reversed(arr)))
        if any(x in result['Filter'] for x in exp):
            query['ann'] = format_ann(result)
            return True
        else:
            return annotated
    elif op == 'between':
        """
        {'between': ['lineitem.l_discount', {'sub': [0.06, 0.01]}, {'add': [0.06, 0.01]}]}
        (lineitem.l_discount >= 0.05) AND (lineitem.l_discount <= 0.07)
        """
        return False
    elif op == 'exists':
        if find_query_node(query[op], result):
            query['expand'] = True
            return True
        return False
    elif op == 'not':
        if parse_expr_node(query[op], result):
            query['expand'] = True
            return True
        return False
    elif op in ['in', 'nin']:
        if type(query[op][1]) is dict:
            if 'literal' in query[op][1]:
                # If with literal array
                # LHS = ANY('{13,31,23,29,30,18,17}'::text[])
                pass
            else:
                # If with subquery, become equijoin
                if find_query_node(query[op][1], result):
                    query['expand'] = True
                    return True
        elif type(query[op][1]) is list:
            assert type(query[op][1][0]) in [str, int, float]
            # LHS = ANY('{49,14,23,45,19,3,36,9}'::integer[])
        return False
    else:
        raise NotImplementedError(f'{op}')


def find_query_node(query: dict, result: dict) -> bool:
    # logging.info(f'query={query}, result={result}')
    if result['Type'] == 'Join':  # look at WHERE
        if 'where' in query:
            if result['Filter'] == '':
                # For Nested Loop without explicit Filter, we try to find the condition by matching column names
                possible_cond = []
                for cond in [f'{x} = {y}' for x in result['Possible LHS'] for y in result['Possible RHS']]:
                    result['Filter'] = cond
                    if parse_expr_node(query['where'], result):
                        possible_cond.append(cond)
                assert len(possible_cond) <= 1, "MORE THAN ONE POSSIBLE CONDITION"
                if len(possible_cond) == 1:
                    return True
            else:
                if parse_expr_node(query['where'], result):
                    return True
        if type(query['from']) is dict and type(query['from']['value']) is dict:
            if find_query_node(query['from']['value'], result):
                return True
        if type(query['from']) is list:
            for v in query['from']:
                if type(v) is dict and type(v['value']) is dict:
                    if find_query_node(v['value'], result):
                        return True
    elif result['Type'] == 'Scan':  # look at FROM
        # goto from
        annotated = False
        if type(query['from']) is str:
            if query['from'] == result['Name'] and query['from'] == result['Alias']:
                query['from'] = {
                    'value': query['from'],
                    'ann': f"{result['Subtype']} {result['Name']}"
                }
                annotated = True
        elif type(query['from']) is dict:
            if type(query['from']['value']) is dict:
                if find_query_node(query['from']['value'], result):
                    query['from']['expand'] = True
                    annotated = True
            elif type(query['from']['value']) is str and query['from']['value'] == result['Name'] and query['from'].get(
                    'name', '') == result['Alias']:
                annotated = True
                query['from']['ann'] = f"{result['Subtype']} {result['Name']} as {result['Alias']}"
        elif type(query['from']) is list:
            for i, rel in enumerate(query['from']):
                if type(rel) is str:
                    if rel == result['Name'] and rel == result['Alias']:
                        query['from'][i] = {
                            'value': rel,
                            'ann': f"{result['Subtype']} {result['Name']}"
                        }
                        annotated = True
                        break
                else:
                    if type(rel['value']) is dict:
                        if find_query_node(rel['value'], result):
                            rel['expand'] = True
                            annotated = True
                        continue
                    assert type(rel['value']) is str
                    if rel['value'] == result['Name'] and rel.get('name', '') == result['Alias']:
                        rel['ann'] = f"{result['Subtype']} {result['Name']} as {result['Alias']}"
                        annotated = True
                        break
        # if filter exist, goto where
        if result['Filter'] != '' and 'where' in query:
            parse_expr_node(query['where'], result)
        return annotated
    return False

def transverse_query(query: dict, plan: dict):
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
    query = preprocess_query_string(query)
    plan = get_query_execution_plan(cur, query)
    parsed_query = parse(query)
    preprocess_query_tree(cur, parsed_query)
    transverse_query(parsed_query, plan[0][0]['Plan'])
    result = []
    reparse_query(result, parsed_query)
    return [q['statement'] for q in result], [q['annotation'] for q in result]


def reparse_without_expand(statement_dict):
    temp = []
    annotation = get_annotation(statement_dict)
    if 'ann' in statement_dict.keys():
        del statement_dict['ann']
    statement = format(statement_dict)
    temp.append(format_query(statement, annotation))
    return temp


def format_keyword_special(statement_dict):
    formatted = format(statement_dict)
    return formatted.split('""', 1)[1].strip()


def get_annotation(statement_dict):
    return '' if 'ann' not in statement_dict.keys() else statement_dict['ann']


def get_name(statement_dict):
    return None if 'name' not in statement_dict.keys() else statement_dict['name']


def find_keyword_operation(statement_dict: dict):
    keyword_ops = {'sum', 'count', 'avg', 'coalesce', 'min', 'max'}
    for op in keyword_ops:
        if op in statement_dict.keys():
            return op
    return None


def find_arithmetic_operation(statement_dict: dict):
    symbol_ops = {'mul', 'sub', 'add', 'div', 'mod'}
    for op in symbol_ops:
        if op in statement_dict.keys():
            return op
    return None


def find_conjunction_operation(statement_dict: dict):
    conj_ops = {'and', 'or'}
    for op in conj_ops:
        if op in statement_dict.keys():
            return op
    return None


def find_comparison_operation(statement_dict: dict):
    comp_ops = {
        'gt',
        'lt',
        'eq',
        'neq',
        'gte',
        'lte',
        'like',
        'not_like',
        'in',
        'nin'
    }
    for op in comp_ops:
        if op in statement_dict.keys():
            return op
    return None


def find_datetime_operation(statement_dict: dict):
    datetime_ops = {'date', 'interval', 'time', 'timestamp'}
    for op in datetime_ops:
        if op in statement_dict.keys():
            return op
    return None


def reparse_literal(value: any):
    if type(value) is str:
        return "'" + value + "'"
    elif type(value) is list:
        res = '('
        for i, v in enumerate(value):
            res += "'" + v + "'"
            if i < len(value) - 1:
                res += ', '
            else:
                res += ')'
        return res
    else:
        raise NotImplementedError(f"literal type - {value}")


def reparse_arithmetic_operation(statement_dict: dict, symbol_op: str):
    temp = []

    if 'expand' not in statement_dict.keys():
        temp.extend(reparse_without_expand(statement_dict))
        return temp

    symbol_ops = {'mul': '*', 'sub': '-', 'add': '+', 'div': '/', 'mod': '%'}
    operands = statement_dict[symbol_op]

    if type(operands) is list:
        statement = ''
        for i, operand in enumerate(operands):
            if isinstance(operand, (int, float, str)):
                statement += str(operand)
            elif type(operand) is dict:
                arithmetic_op = find_arithmetic_operation(operand)
                datetime_op = find_datetime_operation(operand)
                if arithmetic_op is not None:
                    subquery = reparse_arithmetic_operation(operand, arithmetic_op)
                    if len(subquery) > 1:
                        statement += '('
                        temp.append(format_query(statement))
                        temp.extend(subquery)
                        temp.append(format_query(')'))
                    else:
                        statement += '(' + subquery[0]['statement'] + ')'
                        temp.append(format_query(statement, subquery[0]['annotation']))

                    statement = ''
                elif datetime_op is not None:
                    subquery = reparse_datetime_operation(operand, datetime_op)
                    statement += '('
                    if len(subquery) == 1:
                        statement += subquery[0]['statement']
                        statement += ')'
                        temp.append(format_query(statement))
                    else:
                        temp.append(format_query(statement))
                        temp.extend(subquery)
                        temp.append(format_query(')'))
                    statement = ''
                else:
                    subquery = reparse_other_operations(operand)
                    statement += '(' + subquery + ')'
                    temp.append(format_query(statement))
                    statement = ''

            if i < len(operands) - 1:
                if statement != '':
                    statement += ' '
                statement += symbol_ops[symbol_op] + ' '

        if statement != '':
            temp.append(format_query(statement))

    return temp


def reparse_keyword_operation(statement_dict: dict, op: str, comma: bool = False):
    temp = []

    if 'expand' not in statement_dict.keys():
        temp.extend(reparse_without_expand(statement_dict))
        return temp

    operand = statement_dict[op]

    if type(operand) is str:
        statement = op.upper() + ' (' + operand + ')'
        if comma:
            statement += ','
        temp.append(format_query(statement))
    elif type(operand) is dict:
        arithmetic_op = find_arithmetic_operation(operand)
        temp.append(format_query(op.upper() + ' ('))

        if arithmetic_op is not None:
            subquery = reparse_arithmetic_operation(operand, arithmetic_op)
            temp.extend(subquery)
        else:
            subquery = reparse_other_operations(operand)
            temp.append(format_query(subquery))

        end_statement = ')'
        if comma:
            end_statement += ','
        temp.append(format_query(end_statement))

    return temp


def reparse_conjunction_operation(statement_dict: dict, conj_op: str):
    temp = []

    if 'expand' not in statement_dict.keys():
        temp.extend(reparse_without_expand(statement_dict))
        return temp

    operands = statement_dict[conj_op]
    assert type(operands) is list

    for i, operand in enumerate(operands):
        arithmetic_op = find_arithmetic_operation(operand)
        conjunction_op = find_conjunction_operation(operand)
        comparison_op = find_comparison_operation(operand)

        if arithmetic_op is not None:
            temp.append(format_query('('))
            subquery = reparse_arithmetic_operation(operand, arithmetic_op)
            temp.extend(subquery)
            temp.append(format_query(')'))
        elif conjunction_op is not None:
            temp.append(format_query('('))
            subquery = reparse_conjunction_operation(operand, conjunction_op)
            temp.extend(subquery)
            temp.append(format_query(')'))
        elif comparison_op is not None:
            subquery = reparse_comparison_operation(operand, comparison_op)
            if len(subquery) > 1:
                temp.append(format_query('('))
                temp.extend(subquery)
                temp.append(format_query(')'))
            else:
                temp.extend(subquery)
        elif 'exists' in operand.keys():
            temp.append(format_query('('))
            subquery = reparse_exists_keyword(operand)
            temp.extend(subquery)
            temp.append(format_query(')'))
        elif 'not' in operand.keys():
            temp.append(format_query('('))
            subquery = reparse_not_operation(operand)
            temp.extend(subquery)
            temp.append(format_query(')'))
        else:
            temp.append(format_query('('))
            subquery = reparse_other_operations(operand)
            temp.append(format_query(subquery))
            temp.append(format_query(')'))

        if i < len(operands) - 1:
            temp.append(format_query(conj_op.upper()))

    return temp


def reparse_not_operation(statement_dict: dict):
    temp = []

    if 'expand' not in statement_dict.keys():
        temp.extend(reparse_without_expand(statement_dict))
        return temp

    operand = statement_dict['not']

    arithmetic_op = find_arithmetic_operation(operand)
    conjunction_op = find_conjunction_operation(operand)
    comparison_op = find_comparison_operation(operand)

    temp.append(format_query('NOT ('))
    if arithmetic_op is not None:
        subquery = reparse_arithmetic_operation(operand, arithmetic_op)
        temp.extend(subquery)
    elif conjunction_op is not None:
        subquery = reparse_conjunction_operation(operand, conjunction_op)
        temp.extend(subquery)
    elif comparison_op is not None:
        subquery = reparse_comparison_operation(operand, comparison_op)
        temp.extend(subquery)
    elif 'exists' in operand.keys():
        subquery = reparse_exists_keyword(operand)
        temp.extend(subquery)
    else:
        subquery = reparse_other_operations(operand)
        temp.append(format_query(subquery))

    temp.append(format_query(')'))
    return temp


def reparse_comparison_operation(statement_dict: dict, comp_op: str):
    temp = []

    if 'expand' not in statement_dict.keys():
        temp.extend(reparse_without_expand(statement_dict))
        return temp

    comp_ops = {
        'gt': '>',
        'lt': '<',
        'eq': '=',
        'neq': '<>',
        'gte': '>=',
        'lte': '<=',
        'like': 'LIKE',
        'not_like': 'NOT LIKE',
        'in': 'IN',
        'nin': 'NOT IN'
    }

    annotation = get_annotation(statement_dict)
    operands = statement_dict[comp_op]

    # size of list must be 2
    if type(operands) is list:
        statement = ''
        for i, operand in enumerate(operands):
            if isinstance(operand, (int, float, str)):
                statement += str(operand)
            elif type(operand) is dict:
                arithmetic_op = find_arithmetic_operation(operand)
                datetime_op = find_datetime_operation(operand)

                if 'literal' in operand.keys():
                    statement += reparse_literal(operand['literal'])

                else:
                    statement += '('
                    if i > 0:
                        temp.append(format_query(statement, annotation))
                    else:
                        temp.append(format_query(statement))
                    statement = ''

                    if 'select' in operand.keys():
                        reparse_query(temp, operand)
                    elif arithmetic_op is not None:
                        subquery = reparse_arithmetic_operation(operand, arithmetic_op)
                        temp.extend(subquery)
                    elif datetime_op is not None:
                        subquery = reparse_datetime_operation(operand, datetime_op)
                        temp.extend(subquery)
                    else:
                        subquery = reparse_other_operations(operand)
                        temp.append(format_query(subquery))

                    temp.append(format_query(')'))
            elif type(operand) is list:
                if all(isinstance(x, int) for x in operand):
                    statement += '(' + ', '.join([str(o) for o in operand]) + ')'

            if i < len(operands) - 1:
                if statement != '':
                    statement += ' '
                statement += comp_ops[comp_op] + ' '

        if statement != '':
            temp.append(format_query(statement, annotation))

    return temp


def reparse_datetime_operation(statement_dict: dict, datetime_op: str):
    temp = []

    if 'expand' not in statement_dict.keys():
        temp.extend(reparse_without_expand(statement_dict))
        return temp

    operand = statement_dict[datetime_op]

    statement = datetime_op.upper() + " "
    if type(operand) is list:
        statement += "'" + ' '.join([str(o) for o in operand]) + "'"
    elif type(operand) is dict:
        if 'literal' in operand.keys():
            statement += reparse_literal(operand['literal'])

    temp.append(format_query(statement))
    return temp


def reparse_other_operations(statement_dict: dict):
    if 'expand' in statement_dict.keys():
        raise NotImplementedError(f"operation - {statement_dict}")
    else:
        return format(statement_dict)


def reparse_exists_keyword(statement_dict: dict):
    temp = []

    if 'expand' not in statement_dict.keys():
        temp.extend(reparse_without_expand(statement_dict))
        return temp

    operand = statement_dict['exists']

    if type(operand) is dict:
        temp.append(format_query('EXISTS ('))
        reparse_query(temp, operand)
        temp.append(format_query(')'))

    return temp


def reparse_between_keyword(statement_dict: dict):
    temp = []

    if 'expand' not in statement_dict.keys():
        temp.extend(reparse_without_expand(statement_dict))
        return temp

    operand = statement_dict['between']
    temp.append(format_query(operand[0] + ' BETWEEN '))

    for i, op in range(1, len(operand)):
        if isinstance(op, (int, float, str)):
            temp.append(format_query(op))
        elif type(op) is dict:
            arithmetic_op = find_arithmetic_operation(op)

            if arithmetic_op is not None:
                temp.append(format_query('('))
                subquery = reparse_arithmetic_operation(operand, arithmetic_op)
                temp.extend(subquery)
                temp.append(format_query(')'))
            elif 'literal' in op:
                temp.append(format_query(reparse_literal(op['literal'])))

        if i == 1:
            temp.append(format_query('AND'))

    return temp


def format_query(statement: str, annotation: str = ''):
    return {'statement': statement, 'annotation': annotation}


def reparse_from_keyword(formatted_query: list, identifier: any, last_identifier: bool = True):
    temp = []

    if type(identifier) is dict:
        name = get_name(identifier)
        annotation = get_annotation(identifier)
        end_statement = ''

        if type(identifier['value']) is str:
            end_statement = identifier['value']
        elif type(identifier['value']) is dict:
            temp.append(format_query('('))
            reparse_query(temp, identifier['value'])
            end_statement = ')'

        if name is not None:
            end_statement += ' AS ' + name

        if not last_identifier:
            end_statement += ','
        temp.append(format_query(end_statement, annotation))
    elif type(identifier) is str:
        statement = identifier

        if not last_identifier:
            statement += ','

        temp.append(format_query(statement))
    elif type(identifier) is list:
        for i, single_identifier in enumerate(identifier):
            reparse_from_keyword(temp, single_identifier, i == len(identifier) - 1)

    formatted_query.extend(temp)


def reparse_where_keyword(formatted_query: list, identifier: any):
    temp = []
    assert type(identifier) is dict

    conjunction_op = find_conjunction_operation(identifier)
    comparison_op = find_comparison_operation(identifier)

    if conjunction_op is not None:
        subquery = reparse_conjunction_operation(identifier, conjunction_op)
        temp.extend(subquery)
    elif comparison_op is not None:
        subquery = reparse_comparison_operation(identifier, comparison_op)
        temp.extend(subquery)
    elif 'exists' in identifier.keys():
        subquery = reparse_exists_keyword(identifier)
        temp.extend(subquery)
    elif 'not' in identifier.keys():
        subquery = reparse_not_operation(identifier)
        temp.extend(subquery)
    else:
        subquery = reparse_other_operations(identifier)
        temp.append(format_query(subquery))

    formatted_query.extend(temp)


def reparse_keyword_without_annotation(formatted_query: list, identifier: any):
    formatted_query.append(format_query(format_keyword_special(identifier)))


def reparse_query(formatted_query: list, statement_dict: dict):
    temp = []

    for keyword, identifier in statement_dict.items():
        if keyword.startswith('select'):
            appended_identifier = {keyword: identifier}
            if 'distinct_on' in statement_dict.keys():
                appended_identifier['distinct_on'] = statement_dict['distinct_on']
            temp.append(format_query(format(appended_identifier)))
        elif keyword == 'from':
            temp.append(format_query('FROM'))
            reparse_from_keyword(temp, identifier)
        elif keyword == 'where':
            temp.append(format_query('WHERE'))
            reparse_where_keyword(temp, identifier)
        elif keyword == 'having':
            temp.append(format_query('HAVING'))
            reparse_where_keyword(temp, identifier)
        elif keyword == 'groupby':
            appended_identifier = {'groupby': identifier, 'from': ''}
            reparse_keyword_without_annotation(temp, appended_identifier)
        elif keyword == 'orderby':
            appended_identifier = {'orderby': identifier, 'from': ''}
            reparse_keyword_without_annotation(temp, appended_identifier)
        elif keyword == 'limit':
            appended_identifier = {'limit': identifier, 'from': ''}
            reparse_keyword_without_annotation(temp, appended_identifier)

    formatted_query.extend(temp)


def annotate_query(parsed_query: dict):
    formatted_query = []
    reparse_query(formatted_query, parsed_query)
    return formatted_query


def main():
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
            result = []
            reparse_query(result, parsed_query)
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.debug(pformat(query))
            logging.debug(pformat(parsed_query))
            logging.debug(pformat(plan))
            raise e
        else:
            pprint(parsed_query, sort_dicts=False)
            pprint(result)
        print()

    cur.close()


if __name__ == '__main__':
    main()

# SELECT *
# FROM nation, 					  -> Seq Scan, Filter n_regionkey
#      region  					  -> Index Scan on n_regionkey = 0
# WHERE nation.n_regionkey = region.r_regionkey     -> Nested Loop
# AND
#       nation.n_regionkey = 0			  -> SeqScan, Filter n_regionkey = 0
