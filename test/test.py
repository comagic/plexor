[
    {'query': 'select * from get_node(1)',
     'result': [(1,)]
    },
    {'query': 'select * from test_integer(1)',
     'result': [(1,)]
    },
    {'query': 'select * from test_integer_array(1)',
     'result': [([1, 2],)]
    },
    {'query': 'select * from test_enum(1)',
     'result': [('idle',)]
    },
    {'query': 'select * from test_agg_enum(1)',
     'result': [(1, '{idle,start,done}'),
                (2, '{idle,start,done}'),
                (3, '{idle,start,done}'),
                (4, '{idle,start,done}'),
                (5, '{idle,start,done}')]
    },
    {'query': 'select * from test_enum_array(1)',
     'result': [('{idle,start}',)]
    },
    {'query': 'select * from test_complex(1)',
     'result': [('{"(\\"{\\"\\"(1,yes)\\"\\",\\"\\"(2,no)\\"\\"}\\",\\"'
                 '{\\"\\"id\\"\\": 42}\\",\\"{idle,start}\\")","(\\"'
                 '{\\"\\"(1,yes)\\"\\",\\"\\"(2,no)\\"\\"}\\",\\"'
                 '{\\"\\"id\\"\\": 42}\\",\\"{idle,start}\\")"}',)
               ]
    },
    {'query': 'select * from test_composite(1)',
     'result': [(1, ' yes')]
    },
    {'query': 'select * from test_typed_record(1)',
     'result': [(1, 'yes', 'dev')]
    },
    {'query': 'select * from test_untyped_record(1) '
              'as (id integer, name text)',
     'result': [(1, 'yes')]
    },
    {'query': 'select * from test_set_of_record(1)',
     'result': [(1, 'customer_1'),
                (2, 'customer_2'),
                (3, 'customer_3'),
                (4, 'customer_4'),
                (5, 'customer_5')]
    },
    {'query': 'select test_retset(1)',
     'result': [(1,), (2,), (3,), (4,), (5,)]
    },
    {'query': "select * from test_run_on_0_node(0);"},
    {'query': "select * from test_run_test_integer(0, 42);"},
    {'query': "select * from test_run_test_integer_on_0_node(0, 42);"},
    {'query': "select * from test_run_test_integer_on_anode(0, 42);"},
    {'query': "select * from test_overload_function(0);"},
    {'query': "select * from test_overload_function(0, 42);"},
    {'query': "select * from test_overload_function(0, 'test message');"},
    {'query': "select * from test_return_null(0);"},
    {'query': "select * from test_return_null_in_setof(0);"},
    {'query': "select * from test_set_person(0, 1, 'ivanov');"},
    {'query': "select * from test_integer(0);"},
    {'query': "select * from test_integer_array(0);"},
    {'query': "select * from test_enum(0);"},
    {'query': "select * from test_agg_enum(0);"},
    {'query': "select * from test_enum_array(0);"},
    {'query': "select * from test_complex(0);"},
    {'query': "select * from test_composite(0);"},
    {'query': "select * from test_untyped_record(0) "
              "as (id integer, name text);"},
    {'query': "select * from test_set_of_record(0);"},
    {'query': "select * from test_retset(0);"},
    {'query': "select * from test_return_null(0);"},
    {'query': "select * from test_return_null_in_setof(0);"},
    {'query': "select * from test_return_null_in_typed_record(0);"},
    {'query': "select * from test_integer(0);"},
    {'query': "select * from test_integer_array(0);"},
    {'query': "select * from test_enum(0);"},
    {'query': "select * from test_agg_enum(0);"},
    {'query': "select * from test_enum_array(0);"},
    {'query': "select * from test_complex(0);"},
    {'query': "select * from test_composite(0);"},
    {'query': "select * from test_typed_record(0);"},
    {'query': "select * from test_untyped_record(0) "
              "as (id integer, name text);"},
    {'query': "select * from test_set_of_record(0);"},
    {'query': "select * from test_retset(0);"},
    {'query': '''
        select * from test_set_person(0, 1, 'one');
        select * from test_set_person(0, 10, '10');
        select * from test_set_person(1, 2, 'two');
        select * from test_set_person(1, 20, '20');
        savepoint s1;
        select * from test_set_person(1, 2, '!two');
        rollback to savepoint s1;
        select * from test_set_person(1, 2, '!two');
        rollback to savepoint s1;
        release savepoint s1;
        savepoint s2;
        select * from test_set_person(0, 10, '100');
        select * from test_set_person(1, 20, '200');
        release savepoint s2;
     '''},
     {'query': '''
        select * from test_set_person(0, 1, 'one');
        select * from test_set_person(0, 10, '10');
        select * from test_set_person(1, 2, 'two');
        select * from test_set_person(1, 20, '20');
        savepoint s1;
        select * from test_set_person(1, 2, '!!two');
        rollback to savepoint s1;
        select * from test_set_person(1, 2, '!!two');
        rollback to savepoint s1;
        release savepoint s1;
        savepoint s2;
        select * from test_set_person(0, 10, '1000');
        select * from test_set_person(1, 20, '2000');
        release savepoint s2;
        rollback;
     '''},
]
