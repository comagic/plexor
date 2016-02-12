#include "plexor.h"


typedef struct
{
    /* Key value. Must be at the start */
    FunctionCallInfo fcinfo;
    /* Pointer to function data */
    PlxResult       *plx_result;
}   PlxResultCacheEntry;

/* Permanent memory area for result info structures */
static MemoryContext plx_result_mctx;

/* Result cache*/
static HTAB *plx_result_cache = NULL;

void
plx_result_cache_init(void)
{
    HASHCTL       ctl;
    int           flags;
    MemoryContext old_ctx;

    /* don't allow multiple initializations */
    if (plx_result_cache)
        return;

    plx_result_mctx = AllocSetContextCreate(TopMemoryContext,
                                      "Plexor results context",
                                      ALLOCSET_SMALL_MINSIZE,
                                      ALLOCSET_SMALL_INITSIZE,
                                      ALLOCSET_SMALL_MAXSIZE);
    MemSet(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(FunctionCallInfo);
    ctl.entrysize = sizeof(PlxResultCacheEntry);
    ctl.hash = tag_hash;
    ctl.hcxt = plx_result_mctx;
    flags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT;

    old_ctx = MemoryContextSwitchTo(plx_result_mctx);
    plx_result_cache = hash_create("Plexor results cache", MAX_RESULTS_PER_EXPR, &ctl, flags);
    MemoryContextSwitchTo(old_ctx);
}

static PlxResult*
get_plx_result_or_due(FunctionCallInfo fcinfo)
{
    PlxResultCacheEntry *hentry;

    hentry = hash_search(plx_result_cache, &fcinfo, HASH_FIND, NULL);
    if (!hentry)
    {
        PlxFn *plx_fn = get_plx_fn(fcinfo);
        if (plx_fn)
            plx_error(plx_fn, "plx_result not found");
        else
            elog(ERROR, "plx_result not found");
    }
    return hentry->plx_result;
}

void
plx_result_insert_cache(FunctionCallInfo fcinfo, PlxFn *plx_fn, PGresult *pg_result)
{
    PlxResultCacheEntry *hentry;
    bool                 found;
    PlxResult           *plx_result;

    plx_result            = MemoryContextAllocZero(plx_result_mctx, sizeof(PlxResult));
    plx_result->plx_fn    = plx_fn;
    plx_result->pg_result = pg_result;

    hentry = hash_search(plx_result_cache, &fcinfo, HASH_ENTER, &found);
    if (found)
        elog(ERROR, "%s (fcinfo = %p) already has plx_result in cache", plx_fn->name, fcinfo);
    hentry->plx_result = plx_result;
}

static void
plx_result_cache_delete(FunctionCallInfo fcinfo)
{
    hash_search(plx_result_cache, &fcinfo, HASH_REMOVE, NULL);
}

static void
setFixedStringInfo(StringInfo str, void *data, int len)
{
    str->data = data;
    str->maxlen = len;
    str->len = len;
    str->cursor = 0;
}

static Datum
get_row(FunctionCallInfo fcinfo, PlxResult *plx_result, int nrow)
{
    StringInfoData  buf;
    PlxFn          *plx_fn = plx_result->plx_fn;
    PGresult       *pg_result = plx_result->pg_result;
    Datum           ret;

    if (PQgetisnull(pg_result, nrow, 0))
    {
        fcinfo->isnull = true;
        return (Datum)NULL;
    }

    setFixedStringInfo(&buf,
                       PQgetvalue(pg_result, nrow, 0),
                       PQgetlength(pg_result, nrow, 0));

    PG_TRY();
    {
        if (plx_fn->is_binary)
            ret = ReceiveFunctionCall(&plx_fn->ret_type->receive_fn,
                                      &buf,
                                      plx_fn->ret_type->receive_io_params,
                                      plx_fn->ret_type_mod);
        else
            ret = InputFunctionCall(&plx_fn->ret_type->input_fn,
                                    buf.data,
                                    plx_fn->ret_type->receive_io_params,
                                    plx_fn->ret_type_mod);
    }
    PG_CATCH();
    {
        plx_result_cache_delete(fcinfo);
        PQclear(pg_result);
        pfree(plx_result);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return ret;
}

Datum
get_single_result(FunctionCallInfo fcinfo)
{
    PlxResult *plx_result;
    Datum      ret;

    plx_result = get_plx_result_or_due(fcinfo);
    /* code below will be never executed if pg_result not find */
    ret = get_row(fcinfo, plx_result, 0);
    plx_result_cache_delete(fcinfo);
    PQclear(plx_result->pg_result);
    pfree(plx_result);
    return ret;
}

Datum
get_next_row(FunctionCallInfo fcinfo)
{
    PlxResult       *plx_result;
    FuncCallContext *funcctx;
    int              call_cntr;

    plx_result = get_plx_result_or_due(fcinfo);
    /* code below will be never executed if pg_result not find */
    if (SRF_IS_FIRSTCALL())
    {
        funcctx = SRF_FIRSTCALL_INIT();
        funcctx->max_calls = PQntuples(plx_result->pg_result);
    }
    funcctx = SRF_PERCALL_SETUP();
    call_cntr = funcctx->call_cntr;
    if (call_cntr < funcctx->max_calls)
        SRF_RETURN_NEXT(funcctx, get_row(fcinfo, plx_result, call_cntr));
    else
    {
        plx_result_cache_delete(fcinfo);
        PQclear(plx_result->pg_result);
        pfree(plx_result);
        SRF_RETURN_DONE(funcctx);
    }
}
