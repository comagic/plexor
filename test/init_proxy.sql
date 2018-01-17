-- create extension if not exists plexor with schema public;
-- create extension if not exists plpgsql with schema pg_catalog;
create extension if not exists plexor with schema pg_catalog;

create server proxy foreign data wrapper plexor options (
    node_0 'dbname=node0 host=127.0.0.1 port=5432',
    node_1 'dbname=node1 host=127.0.0.1 port=5432',
    node_2 'dbname=node2 host=127.0.0.1 port=5432',
    isolation_level 'read committed'
);

create user mapping
   for public
   server proxy
  options (user 'postgres',password '');

create type id_name as (
  id integer,
  name text
);

create type state as enum (
    'idle',
    'start',
    'done'
);

create type complex as (
  id_names id_name[],
  data json,
  states state[]
);

create table if not exists person (id integer primary key, name text);

create or replace function get_person_name(anode_id integer, aid integer)
returns text
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function get_persons(anode_id integer)
returns table(id integer, name text)
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function set_person(anode_id integer, aid integer, aname text)
returns void
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function clear_person(anode_id integer)
returns void
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function get_node(anode_id integer)
returns integer
    language sql
    as $$
  select anode_id;
$$;

create or replace function get_node(anode_id integer, node integer)
returns integer
    language plpgsql
    as $$
begin
  return coalesce(anode_id, node);
end;
$$;

create or replace function get_node_number(anode_id integer)
returns integer
    language plexor
    as $$
  cluster proxy;
  run get_node_number() on get_node(anode_id);
$$;

create or replace function get_node0_number()
returns integer
    language plexor
    as $$
  cluster proxy;
  run get_node_number() on 0;
$$;

create or replace function return_integer_value(anode_id integer, value integer)
returns integer
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function return_integer_array(anode_id integer, value integer)
returns integer[]
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function overload_function(anode_id integer)
returns integer
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function overload_function(anode_id integer, avalue integer)
returns integer
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function overload_function(anode_id integer, avalue text)
returns text
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function get_null(anode_id integer)
returns integer
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function get_null_in_setof(anode_id integer)
returns setof integer
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function get_null_in_typed_record(anode_id integer, out id integer, out name text)
returns setof record
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function get_idle_enum(anode_id integer)
returns state
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function get_agg_enum(anode_id integer)
returns table(id integer, states state[])
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;


create or replace function get_enum_array(anode_id integer)
returns state[]
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function get_complex(anode_id integer)
returns complex[]
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function get_composite(anode_id integer)
returns id_name
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function get_typed_record(anode_id integer, out id integer, out name text, out dep text)
returns record
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function get_untyped_record(anode_id integer)
returns record
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function get_set_of_record(anode_id integer)
returns table(id integer, name text)
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function get_retset(anode_id integer)
returns setof integer
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id);
$$;

create or replace function two_args_hash_function(anode_id integer)
returns setof integer
    language plexor
    as $$
  cluster proxy;
  run on get_node(anode_id, 1);
$$;

create or replace
function test_all_coalesce_on_records(
  out a text,
  out b text
) returns record as $$
  cluster proxy;
  run on all coalesce;
$$ language plexor;

create or replace
function diferred_error() returns void as $$
  cluster proxy;
  run on 0;
$$ language plexor;

create or replace 
function get_jsonb(anode_id integer)
returns jsonb as $$
  cluster proxy;
  run on get_node(anode_id);
$$ language plexor;
