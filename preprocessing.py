import logging
import typing


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