{
    'proxy': {
        'init_sql': {
            'sql': 'create database proxy',
            'dsn': 'dbname=postgres',
        },
        'init': {
            'script': 'init_proxy.sql',
            'dsn': 'dbname=proxy',
        },
        'final_sql': {
            'sql': 'drop database proxy',
            'dsn': 'dbname=postgres',
        },
    },
    'nodes': [
        {
            'init_sql': {
                'sql': 'create database node0',
                'dsn': 'dbname=postgres',
            },
            'init': {
                'dsn': 'dbname=node0',
                'script': 'init_node.sql',
                'params': {},
            },
            'final_sql': {
                'sql': 'drop database node0',
                'dsn': 'dbname=postgres',
            },
        },
        {
            'init_sql': {
                'sql': 'create database node1',
                'dsn': 'dbname=postgres',
            },
            'init': {
                'dsn': 'dbname=node1',
                'script': 'init_node.sql',
                'params': {},
            },
            'final_sql': {
                'sql': 'drop database node1',
                'dsn': 'dbname=postgres',
            },
        },
        {
            'init_sql': {
                'sql': 'create database node2',
                'dsn': 'dbname=postgres',
            },
            'init': {
                'dsn': 'dbname=node2',
                'script': 'init_node.sql',
                'params': {},
            },
            'final_sql': {
                'sql': 'drop database node2',
                'dsn': 'dbname=postgres',
            },
        },
    ],
    'test': {
        'script': 'test.py',
        # no dsn = proxy dsn
    },
    'query_print_format': '{query}',
    'cycle_print_format': 'cycle {cycle:>2}: {check_stat}s',
    'is_single_connect': True,
    'is_init': True,
    'is_test': True,
    'is_final': True,
}
