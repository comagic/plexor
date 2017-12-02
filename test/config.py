{
    'init': [
        {
            'sql': 'create database proxy',
            'dsn': 'dbname=postgres',
        },
        {
            'script': 'init_proxy.sql',
            'dsn': 'dbname=proxy',
        },
        {
            'sql': 'create database node0',
            'dsn': 'dbname=postgres',
        },
        {
            'script': 'init_node.sql',
            'params': {'node': 0},
            'dsn': 'dbname=node0',
        },
        {
            'sql': 'create database node1',
            'dsn': 'dbname=postgres',
        },
        {
            'script': 'init_node.sql',
            'params': {'node': 1},
            'dsn': 'dbname=node1',
        },
        {
            'sql': 'create database node2',
            'dsn': 'dbname=postgres',
        },
        {
            'script': 'init_node.sql',
            'params': {'node': 2},
            'dsn': 'dbname=node2',
        },
    ],
    'final': [
        {
            'sql': 'drop database proxy',
            'dsn': 'dbname=postgres',
        },
        {
            'sql': 'drop database node0',
            'dsn': 'dbname=postgres',
        },
        {
            'sql': 'drop database node1',
            'dsn': 'dbname=postgres',
        },
        {
            'sql': 'drop database node2',
            'dsn': 'dbname=postgres',
        },
    ],
    'query_print_format': '{n:>2}. {query}',
    'cycle_print_format': 'cycle {cycle:>2}: {check_stat}s',
    'is_single_connect': True,
    'is_init': False,
    'is_test': True,
    'is_final': False,
    'dsn': 'dbname=proxy',
    'tests': [
        {
            'query': 'select * from get_node(1)',
            'result': [{'get_node': 1}]
        },
        {
            'query': 'select * from get_node_number(1)',
            'result': [{'get_node_number': 1}]
        },
        {
            'query': 'select * from get_node0_number()',
            'result': [{'get_node0_number': 0}]
        },
        {
            'query': 'select * from return_integer_value(0, 42)',
            'result': [{'return_integer_value': 42}]
        },
        {
            'query': 'select * from return_integer_array(1, 42)',
            'result': [{'return_integer_array': [1, 42]}]
        },
        {
            'query': "select * from overload_function(1);",
            'result': [{'overload_function': 1}]
        },
        {
            'query': "select * from overload_function(0, 42);",
            'result': [{'overload_function': 42}]

        },
        {
            'query': "select * from overload_function(0, 'test message');",
            'result': [{'overload_function': 'test message'}]
        },
        {
            'query': "select * from get_null(0);",
            'result': [{'get_null': None}]
        },
        {
            'query': "select * from get_null_in_setof(0);",
            'result': [{'get_null_in_setof': 1},
                       {'get_null_in_setof': 2},
                       {'get_null_in_setof': None},
                       {'get_null_in_setof': 4}]
        },
        {
            'query': "select * from get_null_in_typed_record(0);",
            'result': [{'id': None, 'name': 'yes'},
                       {'id': 1, 'name': None}]
        },
        {
            'query': 'select * from get_idle_enum(1)',
            'result': [{'get_idle_enum': 'idle'}]
        },
        {
            'query': 'select * from get_agg_enum(1)',
            'result': [{'states': '{idle,start,done}', 'id': 1},
                       {'states': '{idle,start,done}', 'id': 2},
                       {'states': '{idle,start,done}', 'id': 3},
                       {'states': '{idle,start,done}', 'id': 4},
                       {'states': '{idle,start,done}', 'id': 5}]
        },
        {
            'query': 'select * from get_enum_array(1)',
            'result': [{'get_enum_array': '{idle,start}'}]
        },
        {
            'query': 'select * from unnest(get_complex(1))',
            'result': [{'states': '{idle,start}',
                        'data': {u'id': 42},
                        'id_names': '{"(1,yes)","(2,no)"}'},
                       {'states': '{idle,start}',
                        'data': {u'id': 42},
                        'id_names': '{"(1,yes)","(2,no)"}'}]
        },
        {
            'query': 'select * from get_composite(1)',
            'result': [{'id': 1, 'name': ' yes'}]
        },
        {
            'query': 'select * from get_typed_record(1)',
            'result': [{'id': 1, 'name': 'yes', 'dep': 'dev'}]
        },
        {
            'query': 'select * from get_untyped_record(1) '
                     'as (id integer, name text)',
            'result': [{'id': 1, 'name': 'yes'}]
        },
        {
            'query': 'select * from get_set_of_record(1)',
            'result': [{'id': 1, 'name': 'customer_1'},
                       {'id': 2, 'name': 'customer_2'},
                       {'id': 3, 'name': 'customer_3'},
                       {'id': 4, 'name': 'customer_4'},
                       {'id': 5, 'name': 'customer_5'}]
        },
        {
            'query': 'select * from get_retset(1)',
            'result': [{'get_retset': 1},
                       {'get_retset': 2},
                       {'get_retset': 3},
                       {'get_retset': 4},
                       {'get_retset': 5}]
        },
        {
            'pre': '''
                      select * from clear_person(0);
                      select * from clear_person(1);
                      select * from clear_person(2);
                   ''',
            'query': '''
                        select * from set_person(0, 1, 'one');
                        select * from set_person(0, 10, '10');
                        select * from set_person(1, 2, 'two');
                        select * from set_person(1, 20, '20');
                        savepoint s1;
                        select * from set_person(1, 2, '!two');
                        rollback to savepoint s1;
                        select * from set_person(1, 2, '!two');
                        rollback to savepoint s1;
                        release savepoint s1;
                        savepoint s2;
                        select * from set_person(0, 10, '100');
                        select * from set_person(1, 20, '200');
                        release savepoint s2;
                        select * from get_person_name(0, 1);
                     ''',
            'result': [{'get_person_name': 'one'}]
        },
        {
            'pre': '''
                      select * from clear_person(0);
                      select * from clear_person(1);
                      select * from clear_person(2);
                   ''',
            'query': '''
                        select * from set_person(0, 1, 'one');
                        select * from set_person(0, 10, '10');
                        select * from set_person(1, 2, 'two');
                        select * from set_person(1, 20, '20');
                        savepoint s1;
                        select * from set_person(1, 2, '!!two');
                        rollback to savepoint s1;
                        select * from set_person(1, 2, '!!two');
                        rollback to savepoint s1;
                        release savepoint s1;
                        savepoint s2;
                        select * from set_person(0, 10, '1000');
                        select * from set_person(1, 20, '2000');
                        release savepoint s2;
                        rollback;
                        select * from get_persons(0);
                     ''',
            'result': []
        },
        {
            'query': 'select * from two_args_hash_function(null)',
            'result': [{'two_args_hash_function': 1}]
        },
        {
            'query': 'select diferred_error()',
            'pgerror':
            '\n'.join(
                (
                    'ERROR:  Remote error: duplicate key value violates '
                    'unique constraint "uni_id"',
                    'DETAIL:  Remote detail: Key (id)=(1) already exists.'
                )
            )
        },

    ]
}
