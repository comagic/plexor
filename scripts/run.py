#!/usr/bin/env python

from __future__ import print_function

import os
import time
import argparse
import psycopg2
import traceback

from jinja2 import Template
from psycopg2.extras import RealDictCursor


def execute(query, connect=None, dsn=None, is_autocommit=False):
    if connect:
        connect, is_close_connect = connect, False
    else:
        connect, is_close_connect = psycopg2.connect(dsn), True
        connect.autocommit = is_autocommit
    cursor = connect.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(query)
    except psycopg2.ProgrammingError as err:
        return None, err.pgerror.strip(), False
    except:
        print('execute failed: {}'.format(query))
        map(print, traceback.format_exc().splitlines())
        return None, None, False
    finally:
        while connect.notices:
            print(connect.notices.pop(0), end='')
    try:
        res = cursor.fetchall()
    except:
        res = None
    cursor.close()
    try:
        connect.commit()
    except  psycopg2.IntegrityError as err:
        return None, err.pgerror.strip(), False
    if is_close_connect:
        connect.close()
    return res, None, True


def get_queries(filename):
    text = ''.join(l for l in open(filename) if not l.startswith('--'))
    return (q for q in text.split('\n\n') if q)


def ddl_execute(path, dsn, sql=None, script=None, params=None):
    if sql:
        for query in (q for q in sql.split(';') if q):
            execute(query, dsn=dsn, is_autocommit=True)
    if script:
        for q in get_queries(os.path.join(path, script)):
            execute(Template(q).render(params or {}),
                dsn=dsn,
                is_autocommit=True
            )


def ddl(path, queries):
    for q in queries:
        ddl_execute(path, **q)


def get_pg_backend_pid(connect):
    result, _, _ = execute('select pg_backend_pid()', connect)
    return result[0]['pg_backend_pid']


def cycle_test(
    connect,
    dsn,
    cycle,
    n,
    pre,
    query,
    expect_result,
    expect_pgerror,
    query_print_format
):
    if pre:
        execute(pre, connect=connect, dsn=dsn)
    query_start_time = time.time()

    result, pgerror, is_ok = execute(query, connect=connect, dsn=dsn)
    if not is_ok:
        if expect_pgerror:
            if expect_pgerror == pgerror:
                check = True
            else:
                check = False
            execute('rollback;', connect=connect)
    elif expect_result is not None:
        check = check_result(result, expect_result)
    else:
        check = True
    if query_print_format:
        print(
            query_print_format.format(
                query=query,
                n=n,
                cycle=cycle,
                result=result,
                pgerror=pgerror,
                check='T' if check else 'F',
                expect_result=expect_result,
                expect_pgerror=expect_pgerror,
                time=time.time() - query_start_time
            )
        )
    return check


def test(
    dsn,
    tests,
    config_name,
    key=None,
    cycles=1,
    query_print_format=None,
    cycle_print_format=None,
    total_print_format=None,
    is_single_connect=None,
    gdb_macros=None,
    run_gdb_after=None,
    **kw
):
    connect = psycopg2.connect(dsn) if is_single_connect else None
    backend_pid = get_pg_backend_pid(connect) if connect else None
    run_gdb_after = run_gdb_after or [1, cycles]
    test_start_time = time.time()
    for cycle in xrange(1, cycles + 1):
        cycle_start_time = time.time()
        checks = [
            cycle_test(
                connect,
                dsn,
                cycle,
                n,
                q.get('pre'),
                q['query'],
                q.get('result'),
                q.get('pgerror'),
                query_print_format,
            )
            for n, q in enumerate(tests)
            if not key or key in q['query']
        ]
        if gdb_macros and cycle in run_gdb_after:
            open(
                '{}_{}'.format(os.path.basename(config_name), cycle),
                'w'
            ).write(
                os.popen(
                    'gdb -p {} < {}'.format(backend_pid, gdb_macros)
                ).read()
            )

        if cycle_print_format:
            print(
                cycle_print_format.format(
                    cycle=cycle,
                    check_stat='{}, {}'.format(
                        'ok: {}'.format(checks.count(True)),
                        'fail: {}'.format(checks.count(False))),
                    time=time.time() - cycle_start_time
                )
            )
    if total_print_format:
        print(
            total_print_format.format(
                time=time.time() - test_start_time
            )
        )


def check_result(result, original):
    return result == original


def run(
    path,
    is_init=False,
    is_test=False,
    is_final=False,
    init=None,
    final=None,
    **kw
):
    if is_init:
        ddl(path, init or [])
    if is_test:
        test(**kw)
    if is_final:
        ddl(path, final or [])


def main():
    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument('config',
                            help='test configuration file')
    arg_parser.add_argument('--cycles',
                            type=int,
                            default=1,
                            help='cycles count')
    arg_parser.add_argument('--key', '-k',
                            type=str,
                            default=None)
    arg_parser.add_argument('--query-print-format',
                            type=str)
    arg_parser.add_argument('--cycle-print-format',
                            type=str)
    arg_parser.add_argument('--total-print-format',
                            type=str)
    arg_parser.add_argument('--gdb-macros',
                            type=str)
    arg_parser.add_argument('--run-gdb-after',
                            type=int,
                            nargs='+',
                            default=[],
                            help='run gdb after N cycle')
    arg_parser.add_argument('--single-connect',
                            dest='is_single_connect',
                            action='store_true',
                            help='run all test in single connect')
    arg_parser.add_argument('--no-single-connect',
                            dest='is_single_connect',
                            action='store_false',
                            help='run all test in single connect')
    arg_parser.set_defaults(is_single_connect=None)
    arg_parser.add_argument('--init',
                            dest='is_init',
                            action='store_true',
                            help='run init section')
    arg_parser.add_argument('--no-init',
                            dest='is_init',
                            action='store_false',
                            help='don\'t run init section')
    arg_parser.set_defaults(is_init=None)
    arg_parser.add_argument('--test',
                            dest='is_test',
                            action='store_true',
                            help='run test section')
    arg_parser.add_argument('--no-test',
                            dest='is_test',
                            action='store_false',
                            help='don\'t run test section')
    arg_parser.set_defaults(is_test=None)
    arg_parser.add_argument('--final',
                            dest='is_final',
                            action='store_true',
                            help='run final section')
    arg_parser.add_argument('--no-final',
                            dest='is_final',
                            action='store_false',
                            help='don\'t run final section')
    arg_parser.set_defaults(is_final=None)


    args = arg_parser.parse_args()
    config_name = args.__dict__.pop('config')
    path = os.path.dirname(os.path.abspath(config_name))
    config = eval(open(config_name).read())
    config.update(
        path=path,
        config_name=config_name,
        **{
            k: v
            for k, v in args.__dict__.iteritems()
            if v is not None
        }
    )
    run(**config)

if __name__ == '__main__':
    main()
