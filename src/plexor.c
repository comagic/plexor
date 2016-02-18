#include "plexor.h"

#ifndef PG_MODULE_MAGIC
#error Plexor requires 9.4
#else
PG_MODULE_MAGIC;
#endif

static bool initialized = false;


PG_FUNCTION_INFO_V1(plexor_call_handler);
PG_FUNCTION_INFO_V1(plexor_validator);


static void
plx_startup_init(void)
{
    if (initialized)
        return;

    plx_cluster_cache_init();
    plx_conn_cache_init();
    plx_fn_cache_init();
    plx_result_cache_init();
    execute_init();

    initialized = true;
}

static int
get_nnode(PlxFn *plx_fn, FunctionCallInfo fcinfo)
{
    PlxQuery   *plx_q = plx_fn->hash_query;
    int         err;
    SPIPlanPtr  plan;
    Oid         types[FUNC_MAX_ARGS];
    Datum       values[FUNC_MAX_ARGS];
    char        arg_nulls[FUNC_MAX_ARGS];
    Datum       val;
    bool        isnull;
    int         i;

    if ((err = SPI_connect()) != SPI_OK_CONNECT)
        plx_error(plx_fn, "SPI_connect: %s", SPI_result_code_string(err));

    for (i = 0; i < plx_q->nargs; i++)
    {
        int idx = plx_q->plx_fn_arg_indexes[i];

        types[i] = plx_fn->arg_types[idx]->oid;
        values[i] = PG_GETARG_DATUM(idx);
    }
    plan = SPI_prepare(plx_q->sql->data, plx_q->nargs, types);
    err = SPI_execute_plan(plan, values, arg_nulls, true, 0);
    if (err != SPI_OK_SELECT)
        plx_error(plx_fn,
                  "query '%s' failed: %s",
                  plx_q->sql->data,
                  SPI_result_code_string(err));
    val = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
    err = SPI_finish();
    if (err != SPI_OK_FINISH)
        plx_error(plx_fn, "SPI_finish: %s", SPI_result_code_string(err));

    return DatumGetInt32(val);
}

static void
execute(FunctionCallInfo fcinfo)
{
    PlxCluster *plx_cluster = NULL;
    PlxConn    *plx_conn    = NULL;
    PlxFn      *plx_fn      = NULL;

    plx_startup_init();
    plx_fn = get_plx_fn(fcinfo);
    plx_cluster = get_plx_cluster(plx_fn->cluster_name);
    if (plx_fn->run_on == RUN_ON_HASH)
        plx_conn = get_plx_conn(plx_cluster, get_nnode(plx_fn, fcinfo));
    else if (plx_fn->run_on == RUN_ON_NNODE)
        plx_conn = get_plx_conn(plx_cluster, plx_fn->nnode);
    else if (plx_fn->run_on == RUN_ON_ANODE)
        plx_conn = get_plx_conn(plx_cluster, PG_GETARG_DATUM(plx_fn->anode));
    else
        plx_error(plx_fn, "failed to run on %d", plx_fn->run_on);
    remote_execute(plx_conn, plx_fn, fcinfo);
}

Datum
plexor_call_handler(PG_FUNCTION_ARGS)
{
    if (CALLED_AS_TRIGGER(fcinfo))
        elog(ERROR, "plexor function can't be used as triggers");

    if (fcinfo->flinfo->fn_retset)
    {
        if (SRF_IS_FIRSTCALL())
            execute(fcinfo);
        return get_next_row(fcinfo);
    }
    else
    {
        execute(fcinfo);
        return get_single_result(fcinfo);
    }
}

Datum
plexor_validator(PG_FUNCTION_ARGS)
{
    Oid        oid        = PG_GETARG_OID(0);
    PlxFn     *plx_fn     = NULL;
    HeapTuple  proc_tuple = NULL;

    if (!CheckFunctionValidatorAccess(fcinfo->flinfo->fn_oid, oid))
        PG_RETURN_VOID();

    proc_tuple = SearchSysCache(PROCOID, ObjectIdGetDatum(oid), 0, 0, 0);
    if (!HeapTupleIsValid(proc_tuple))
        elog(ERROR, "cache lookup failed for function %u", oid);

    plx_startup_init();
    plx_fn = compile_plx_fn(NULL, proc_tuple, true);
    delete_plx_fn(plx_fn, false);

    ReleaseSysCache(proc_tuple);
    PG_RETURN_VOID();
}
