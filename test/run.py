#!/usr/bin/env python

import time
import argparse
import psycopg2


def execute(query, connect=None, dsn=None):
    if connect:
        connect, is_close_connect = connect, False
    else:
        connect, is_close_connect = psycopg2.connect(dsn), True
    cursor = connect.cursor()
    try:
        cursor.execute(query)
    finally:
        while connect.notices:
            print connect.notices.pop(0),
    try:
        res = cursor.fetchall()
    except:
        res = None
    cursor.close()
    connect.commit()
    if is_close_connect:
        connect.close()
    return res


def run(queries,
        dsn=None,
        cycles=1,
        query_print_format=None,
        cycle_print_format=None,
        **kw):
    for cycle in xrange(1, cycles + 1):
        cycle_start_time = time.time()
        checks = []
        for n, query, expect_result in ((n, q['query'], q.get('result'))
                                        for n, q in enumerate(queries)):
            query_start_time = time.time()
            result = execute(query, dsn=dsn)
            if expect_result:
                check = check_result(result, expect_result)
            else:
                check = True
            checks.append(check)
            if query_print_format:
                print query_print_format.format(
                    query=query,
                    n=n,
                    cycle=cycle,
                    result=result,
                    check='T' if check else 'F',
                    expect_result=expect_result,
                    time=time.time() - query_start_time)
        if cycle_print_format:
            print cycle_print_format.format(
                cycle=cycle,
                check_stat='{}, {}'.format(
                    'ok: {}'.format(checks.count(True)),
                    'fail: {}'.format(checks.count(False))),
                time=time.time() - cycle_start_time)


def check_result(result, original):
    return result == original


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument('--dsn',
                            default='dbname=proxy',
                            help='DSN')
    arg_parser.add_argument('--cycles',
                            type=int,
                            default=1,
                            help='number of cycles')
    arg_parser.add_argument('--connect-per-execute',
                            type=bool,
                            default=False,
                            help='make unique connect for every execute')
    arg_parser.add_argument('--query-print-format',
                            type=str,
                            default='{query}',
                            help='query execution output format')
    arg_parser.add_argument('--cycle-print-format',
                            type=str,
                            default='cycle {cycle:>2}: {check_stat}s',
                            help='query execution output format')
    arg_parser.add_argument('test',
                            type=str,
                            help='file with test queries')
    args = arg_parser.parse_args()

    queries = eval(open(args.__dict__.pop('test')).read())
    run(queries, **args.__dict__)


'''
dropdb proxy &&
dropdb node0 &&
dropdb node1 &&
createdb proxy &&
createdb node0 &&
createdb node1 &&
psql -f test/init_proxy.sql proxy &&
psql -f test/init_node.sql node0 &&
psql -f test/init_node.sql node1 &&
python test/run.py test/test.py --query-print-format='{check}: {query}' --cycle-print-format='{cycle}: {check_stat}' --cycles=100

'''