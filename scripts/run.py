#!/usr/bin/env python

from __future__ import print_function

import os
import time
import argparse
import psycopg2
import traceback

from jinja2 import Template


def execute(query, connect=None, dsn=None, is_autocommit=False):
    if connect:
        connect, is_close_connect = connect, False
    else:
        connect, is_close_connect = psycopg2.connect(dsn), True
        connect.autocommit = is_autocommit
    cursor = connect.cursor()
    try:
        cursor.execute(query)
    except:
        map(print, traceback.format_exc().splitlines())
    finally:
        while connect.notices:
            print(connect.notices.pop(0))
    try:
        res = cursor.fetchall()
    except:
        res = None
    cursor.close()
    connect.commit()
    if is_close_connect:
        connect.close()
    return res


def get_queries(filename):
    text = ''.join(l for l in open(filename) if not l.startswith('--'))
    return (q for q in text.split('\n\n') if q)


def ddl_execute(path, sql=None, script=None, **kw):
    if sql:
        execute(sql['sql'], dsn=sql['dsn'], is_autocommit=True)
    if script:
        for q in get_queries(os.path.join(path, script.get('script'))):
            execute(Template(q).render(script.get('params', {})),
                dsn=script['dsn'],
                is_autocommit=True)


def ddl(path, proxy, nodes, sql_field, script_field, **kw):
    ddl_execute(path, sql=proxy.get(sql_field), script=proxy.get(script_field))
    for node in nodes:
        ddl_execute(path,
                    sql=node.get(sql_field),
                    script=node.get(script_field))

def test(path,
         test,
         proxy,
         cycles=1,
         query_print_format=None,
         cycle_print_format=None,
         is_single_connect=True,
         **kw):
    dsn = test.get('dsn') or proxy.get('init', {}).get('dsn')
    script_name = os.path.join(path, test['script'])
    queries = eval(open(script_name).read())
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
                print(
                    query_print_format.format(
                        query=query,
                        n=n,
                        cycle=cycle,
                        result=result,
                        check='T' if check else 'F',
                        expect_result=expect_result,
                        time=time.time() - query_start_time))
        if cycle_print_format:
            print(
                cycle_print_format.format(
                    cycle=cycle,
                    check_stat='{}, {}'.format(
                        'ok: {}'.format(checks.count(True)),
                        'fail: {}'.format(checks.count(False))),
                    time=time.time() - cycle_start_time))


def check_result(result, original):
    return result == original



def run(is_init=False, is_test=False, is_final=False, **kw):
    if is_init:
        ddl(sql_field='init_sql', script_field='init', **kw)
    if is_test:
        test(**kw)
    if is_final:
        ddl(sql_field='final_sql', script_field='final', **kw)


def main():
    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument('config',
                            help='test configuration file')
    arg_parser.add_argument('--cycles',
                            type=int,
                            default=1,
                            help='cycles count')
    arg_parser.add_argument('--query-print-format',
                            type=str)
    arg_parser.add_argument('--cycle-print-format',
                            type=str)
    arg_parser.add_argument('--single-connect',
                            dest='is_single_connect',
                            action='store_true',
                            help='run all test in single connect')
    arg_parser.add_argument('--no-single-connect',
                            dest='is_single_connect',
                            action='store_false',
                            help='run all test in single connect')
    arg_parser.set_defaults(is_single_connect=True)
    arg_parser.add_argument('--init',
                            dest='is_init',
                            action='store_true',
                            help='run init section')
    arg_parser.add_argument('--no-init',
                            dest='is_init',
                            action='store_false',
                            help='don\'t run init section')
    arg_parser.set_defaults(is_init=True)
    arg_parser.add_argument('--test',
                            dest='is_test',
                            action='store_true',
                            help='run test section')
    arg_parser.add_argument('--no-test',
                            dest='is_test',
                            action='store_false',
                            help='don\'t run test section')
    arg_parser.set_defaults(is_test=True)
    arg_parser.add_argument('--final',
                            dest='is_final',
                            action='store_true',
                            help='run final section')
    arg_parser.add_argument('--no-final',
                            dest='is_final',
                            action='store_false',
                            help='don\'t run final section')
    arg_parser.set_defaults(is_final=True)
    args = arg_parser.parse_args()

    config_name = args.__dict__.pop('config')
    path = os.path.dirname(os.path.abspath(config_name))
    config = eval(open(config_name).read())
    config.update(path=path,
                  **{k: v
                     for k, v in args.__dict__.iteritems()
                     if v is not None})
    run(**config)

if __name__ == '__main__':
    main()
