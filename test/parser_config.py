{
    'init': [
        {
            'sql': 'create database parser_tests',
            'dsn': 'dbname=postgres',
        },
        {
            'sql': 'create extension if not exists plexor with schema pg_catalog',
            'dsn': 'dbname=parser_tests',
        },
    ],
    'final': [
        {
            'sql': 'drop database parser_tests',
            'dsn': 'dbname=postgres',
        },
    ],
    'query_print_format': '{query}\n{pgerror}',
    'cycle_print_format': 'cycle {cycle:>2}: {check_stat}s',
    'is_single_connect': True,
    'is_init': False,
    'is_test': True,
    'is_final': False,
    'dsn': 'dbname=parser_tests',
    'tests': [
        {
            'query':
            '\n'.join(
                (
                    'create or replace function cluster_keyword_error(anode_id integer)',
                    'returns text',
                    'language plexor as $$',
                    'luster proxy;',
                    'run on get_node(anode_id);',
                    '$$;',
                )
            ),
            'pgerror':
            (
                "ERROR:  Plexor function public.cluster_keyword_error(): "
                "'cluster' keyword not found"
            )

        },
        {
            'query':
            '\n'.join(
                (
                    'create or replace function syntax_error(anode_id integer)',
                    'returns text',
                    '    language plexor',
                    '    as $$',
                    '    cluster |proxy',
                    '$$;',
                )
            ),
            'pgerror':
            (
                "ERROR:  Plexor function public.syntax_error(): unexpected symbol '|'"
            )
        },
        {
            'query':
            '\n'.join(
                (
                    'create or replace function two_arg_hash_function(anode_id integer)',
                    'returns text',
                    '    language plexor',
                    '    as $$',
                    '    cluster proxy;',
                    '    run on get_node(anode_id, 0);'
                    '$$;',
                )
            ),
            'pgerror':
            (
                "ERROR:  Plexor function public.syntax_error(): unexpected symbol '|'"
            )
        },
    ]
}
