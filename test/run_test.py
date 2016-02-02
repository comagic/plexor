#!/usr/bin/env python

import psycopg2


DSN = 'dbname=comagic host=127.0.0.1 port=5432'

connect = psycopg2.connect(DSN)


def execute(query):
    # connect = psycopg2.connect(DSN)
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
    return res


queries = {
    'select * from get_node(1)': '''[(0,)]''',
    'select * from test_integer(1)': '''[(1,)]''',
    'select * from test_integer_array(1)': '''[([1, 2],)]''',
    'select * from test_enum(1)': '''[('idle',)]''',
    'select * from test_agg_enum(1)': '''[(1, '{idle,start,done}')]''',
    'select * from test_enum_array(1)': '''[('{idle,start}',)]''',
    'select * from test_complex(1)': '''[('{"(\\\\"{\\\\"\\\\"(1,yes)\\\\"\\\\",\\\\"\\\\"(2,no)\\\\"\\\\"}\\\\",\\\\"{\\\\"\\\\"id\\\\"\\\\": 42}\\\\",\\\\"{idle,start}\\\\")","(\\\\"{\\\\"\\\\"(1,yes)\\\\"\\\\",\\\\"\\\\"(2,no)\\\\"\\\\"}\\\\",\\\\"{\\\\"\\\\"id\\\\"\\\\": 42}\\\\",\\\\"{idle,start}\\\\")"}',)]''',
    'select * from test_composite(1)': '''[(1, ' yes')]''',
    'select * from test_typed_record(1)': '''[(1, 'yes', 'dev')]''',
    'select * from test_untyped_record(1) as (id integer, name text)': '''[(1, 'yes')]''',
    'select * from test_set_of_record(1)': '''[(1, 'customer_1')]''',
    'select test_retset(1)': '''[(1,)]'''
}

queries = [


    "select * from test_run_on_0_node(0);",
    "select * from test_run_test_integer(0, 42);",
    "select * from test_run_test_integer_on_0_node(0, 42);",
    "select * from test_run_test_integer_on_anode(0, 42);",
    "select * from test_overload_function(0);",
    "select * from test_overload_function(0, 42);",
    "select * from test_overload_function(0, 'test message');",
    "select * from test_return_null(0);",
    "select * from test_return_null_in_setof(0);",
    # "select * from test_return_null_in_typed_record(0, out id integer, out name text);",
    "select * from test_set_person(0, 1, 'ivanov');",
    "select * from test_integer(0);",
    "select * from test_integer_array(0);",
    "select * from test_enum(0);",
    "select * from test_agg_enum(0);",
    "select * from test_enum_array(0);",
    "select * from test_complex(0);",
    "select * from test_composite(0);",
    # "select * from test_typed_record(0, out id integer, out name text, out dep text);",
    "select * from test_untyped_record(0) as (id integer, name text);",
    "select * from test_set_of_record(0);",
    "select * from test_retset(0);",


    'select * from test_return_null(0);',
    'select * from test_return_null_in_setof(0);',
    'select * from test_return_null_in_typed_record(0);',
    'select * from test_integer(0);',
    'select * from test_integer_array(0);',
    'select * from test_enum(0);',
    'select * from test_agg_enum(0);',
    'select * from test_enum_array(0);',
    'select * from test_complex(0);',
    'select * from test_composite(0);',
    'select * from test_typed_record(0);',
    'select * from test_untyped_record(0) as (id integer, name text);',
    'select * from test_set_of_record(0);',
    'select * from test_retset(0);',

# '''
#     select * from test_set_person(0, 1, 'one');
#     select * from test_set_person(0, 10, '10');
#     select * from test_set_person(1, 2, 'two');
#     select * from test_set_person(1, 20, '20');
#     savepoint s1;
#     select * from test_set_person(1, 2, '!two');
#     rollback to savepoint s1;
#     select * from test_set_person(1, 2, '!two');
#     rollback to savepoint s1;
#     release savepoint s1;
#     savepoint s2;
#     select * from test_set_person(0, 10, '100');
#     select * from test_set_person(1, 20, '200');
#     release savepoint s2;
# ''',
# '''
#     select * from test_set_person(0, 1, 'one');
#     select * from test_set_person(0, 10, '10');
#     select * from test_set_person(1, 2, 'two');
#     select * from test_set_person(1, 20, '20');
#     savepoint s1;
#     select * from test_set_person(1, 2, '!!two');
#     rollback to savepoint s1;
#     select * from test_set_person(1, 2, '!!two');
#     rollback to savepoint s1;
#     release savepoint s1;
#     savepoint s2;
#     select * from test_set_person(0, 10, '1000');
#     select * from test_set_person(1, 20, '2000');
#     release savepoint s2;
# rollback;
# ''',
]
#import time
#time.sleep(1)

def ttest(n=1000):
    for i in xrange(n):
        for q in queries:
            # print q,
            # print execute(q)
            print '{:*^100}'.format(' '.join([str(i), q]))
            execute(q)

#time.sleep(5)
# print '\n'.join(
#     "%s %s %s" % (q, r, e)
#     for q, r, e in ((k, v, str(execute(k))) for k, v in queries.items())
#     if r != e)

ttest(1)






















