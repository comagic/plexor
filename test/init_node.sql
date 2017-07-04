-- create extension if not exists plpgsql with schema pg_catalog;
create extension if not exists plpythonu with schema pg_catalog;

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

create or replace function test_run_on_0_node(aid integer)
returns integer
    language plpgsql
    as $$
begin
    return aid;
end;
$$;

create or replace function test_overload_function(anode_id integer)
returns integer
    language plpgsql
    as $$
begin
    return anode_id;
end;
$$;

create or replace function test_overload_function(anode_id integer, avalue integer)
returns integer
    language plpgsql
    as $$
begin
    return avalue;
end;
$$;

create or replace function test_overload_function(anode_id integer, avalue text)
returns text
    language plpgsql
    as $$
begin
    return avalue;
end;
$$;

create function test_return_null(anode_id integer)
returns integer
    language plpgsql
    as $$
begin
    return null;
end;
$$;

create or replace function test_return_null_in_setof(anode_id integer)
returns setof integer
    language plpgsql
    as $$
begin
    return next 1;
    return next 2;
    return next null;
    return next 4;
end;
$$;

create or replace function test_return_null_in_typed_record(anode_id integer, out id integer, out name text)
returns setof record
    language plpythonu
    as $$
return [{'id': None, 'name': 'yes'},
        {'id': 1, 'name': None}]
$$;

create or replace function test_set_person(anode_id integer, aid integer, aname text)
returns void
    language plpgsql
    as $$
begin
    if aname is null then
      delete from person where id = aid;
    elsif exists (select * from person where id = aid) then
        update person set name = aname where id = aid;
    else
        insert into person (id, name) values (aid, aname);
    end if;
end;
$$;

create function test_integer(anode_id integer)
returns integer
    language plpgsql
    as $$
begin
    return anode_id;
end;
$$;

create function test_all_coalesce()
returns integer
    language plpgsql
    as $$
begin
    return null;
end;
$$;

create function test_integer_array(anode_id integer)
returns integer[]
    language plpgsql
    as $$
begin
    return '{1,2}'::integer[];
end;
$$;

create function test_enum(anode_id integer)
returns state
    language plpgsql
    as $$
begin
    return 'idle'::state;
end;
$$;

create function test_agg_enum(anode_id integer)
returns table(id integer, states state[])
    language plpgsql
    as $$
begin
    return query
      select i, enum_range(null::state)
        from generate_series(1, 5) as i;
end;
$$;

create function test_enum_array(anode_id integer)
returns state[]
    language plpgsql
    as $$
begin
    return '{idle, start}'::state[];
end;
$$;

create function test_complex(anode_id integer)
returns complex[]
    language plpgsql
    as $$
declare
    res complex[] := '{}';
    t complex;
begin
    t.id_names := '{"(1,yes)", "(2,no)"}';
    t.data := '{"id": 42}';
    t.states := '{idle,start}';
    res := res || t;
    res := res || t;
    return res;
end;
$$;

create function test_composite(anode_id integer)
returns id_name
    language plpgsql
    as $$
begin
    return '(1, yes)'::id_name;
end;
$$;

create function test_typed_record(anode_id integer, out id integer, out name text, out dep text)
returns record
    language plpythonu
    as $$
return {'id': 1, 'name': 'yes', 'dep': 'dev'}
$$;

create function test_untyped_record(anode_id integer)
returns record
    language plpythonu
    as $$
return {'id': 1, 'name': 'yes'}
$$;

create function test_set_of_record(anode_id integer)
returns table(id integer, name text)
    language plpgsql
    as $$
begin
    return query
      select i, format('customer_%s', i)
        from generate_series(1, 5) as i;
end;
$$;

create function test_retset(anode_id integer)
returns setof integer
    language plpgsql
    as $$
begin
    return query
      select i from generate_series(1, 5) as i;
end;
$$;
