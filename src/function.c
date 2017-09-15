/*
 * Copyright (c) 2015, Dima Beloborodov, Andrey Chernyakov, (CoMagic, UIS)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *    This product includes software developed by the <organization>.
 * 4. Neither the name of the <organization> nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL AUTHOR BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "plexor.h"


/* Structure to keep plx_fn in HTAB's context. */
typedef struct
{
    /* Key value. Must be at the start */
    Oid    oid;
    /* Pointer to function data */
    PlxFn *plx_fn;
} PlxFnHashEntry;

/* Permanent memory area for function info structures */
static MemoryContext plx_fn_mctx;

/* Function cache */
static HTAB *plx_fn_cache = NULL;


void
plx_error_with_errcode(PlxFn *plx_fn, int err_code, const char *fmt, ...)
{
    char     msg[1024];
    va_list  ap;
    char    *fn_name;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    fn_name = pstrdup(plx_fn->name);
    delete_plx_fn(plx_fn, plx_fn_lookup_cache(plx_fn->oid) ? true : false);

    ereport(ERROR, (errcode(err_code),
                    errmsg("Plexor function %s(): %s", fn_name, msg)));
}

/* Initialize plexor function cache */
void
plx_fn_cache_init(void)
{
    HASHCTL     ctl;
    int         flags;
    int         max_funcs = 128; // fixme
    MemoryContext old_ctx;

    /* don't allow multiple initializations */
    if (plx_fn_cache)
        return;

    plx_fn_mctx = AllocSetContextCreate(TopMemoryContext,
                                          "Plexor functions context",
                                          ALLOCSET_SMALL_MINSIZE,
                                          ALLOCSET_SMALL_INITSIZE,
                                          ALLOCSET_SMALL_MAXSIZE);
    MemSet(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(PlxFnHashEntry);
    ctl.hash = oid_hash;
    ctl.hcxt = plx_fn_mctx;
    flags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT;

    old_ctx = MemoryContextSwitchTo(plx_fn_mctx);
    plx_fn_cache = hash_create("Plexor functions cache", max_funcs, &ctl, flags);
    MemoryContextSwitchTo(old_ctx);
}

/* Search for function in cache */
PlxFn*
plx_fn_lookup_cache(Oid fn_oid)
{
    PlxFnHashEntry *hentry;

    hentry = hash_search(plx_fn_cache, &fn_oid, HASH_FIND, NULL);
    if (hentry)
        return hentry->plx_fn;
    return NULL;
}

/* Insert function into cache */
static void
plx_fn_insert_cache(PlxFn *plx_fn)
{
    PlxFnHashEntry *hentry;
    bool            found;

    hentry = hash_search(plx_fn_cache, &plx_fn->oid, HASH_ENTER, &found);
    if (found)
        elog(ERROR, "plexor function '%s' is already in cache", plx_fn->name);
    hentry->plx_fn = plx_fn;
}


/* Delete function from cache */
static void
plx_fn_cache_delete(Oid fn_oid)
{
    hash_search(plx_fn_cache, &fn_oid, HASH_REMOVE, NULL);
}

/*
 * Return the index of a argument, raise error if not found
 */
int
plx_fn_get_arg_index(PlxFn *plx_fn, const char *name)
{
    int i;

    if (name[0] == '$')
    {
        /* $# parameter reference */
        i = atoi(name + 1) - 1;
        if (i >= 0 && i < plx_fn->nargs)
            return i;
    }
    else if (plx_fn->arg_names)
    {
        /* named parameter, try to find in argument names */
        for (i = 0; i < plx_fn->nargs; i++)
        {
            if (!plx_fn->arg_names[i])
                continue;
            if (pg_strcasecmp(name, plx_fn->arg_names[i]) == 0)
                return i;
        }
    }
    plx_error(plx_fn, "no '%s' among function args", name);
    return -1;
}

void
fill_plx_fn_cluster_name(PlxFn* plx_fn, const char *cluster_name)
{
    plx_fn->cluster_name = mctx_strcpy(plx_fn->mctx, cluster_name);
}

static bool
is_fn_returns_dynamic_record(HeapTuple proc_tuple)
{
    Form_pg_proc proc_struct;
    proc_struct = (Form_pg_proc) GETSTRUCT(proc_tuple);
    if (proc_struct->prorettype == RECORDOID
        && (heap_attisnull(proc_tuple, Anum_pg_proc_proargmodes)
            || heap_attisnull(proc_tuple, Anum_pg_proc_proargnames)))
        return true;
    return false;
}

static void
fill_plx_fn_name(PlxFn* plx_fn, Form_pg_proc proc_struct)
{
    Form_pg_namespace ns_struct;
    HeapTuple         ns_tuple;
    Oid               nsoid;
    StringInfo        buf;

    nsoid = proc_struct->pronamespace;
    ns_tuple = SearchSysCache(NAMESPACEOID,
                              ObjectIdGetDatum(nsoid), 0, 0, 0);
    if (!HeapTupleIsValid(ns_tuple))
        plx_error(plx_fn, "Cannot find namespace %u", nsoid);
    ns_struct = (Form_pg_namespace) GETSTRUCT(ns_tuple);

    buf = makeStringInfo();
    appendStringInfo(buf,
                     "%s.%s",
                     quote_identifier(NameStr(ns_struct->nspname)),
                     quote_identifier(NameStr(proc_struct->proname)));
    plx_fn->name = mctx_strcpy(plx_fn->mctx, buf->data);
    ReleaseSysCache(ns_tuple);
}

void
fill_plx_fn_anode(PlxFn* plx_fn, const char *anode_name)
{
    int idx      = plx_fn_get_arg_index(plx_fn, anode_name);
    Oid arg_type = plx_fn->arg_types[idx]->oid;

    if (arg_type != INT2OID && arg_type != INT4OID && arg_type != INT8OID)
        plx_error(plx_fn, "type 'anode' must be one of (int2, int4 (integer), int8)");
    plx_fn->anode = idx;
}

static void
fill_plx_fn_arg_types(PlxFn* plx_fn, HeapTuple proc_tuple)
{
    PlxType      **plx_types;
    char         **plx_names;
    Oid           *types = NULL;
    char         **names = NULL;
    char          *modes = NULL;
    int            nargs;
    MemoryContext  old_ctx;
    int            i;

    nargs = get_func_arg_info(proc_tuple, &types, &names, &modes);
    plx_fn->nargs = 0;

    old_ctx = MemoryContextSwitchTo(plx_fn->mctx);
    plx_fn->arg_types = plx_types = palloc0(sizeof(PlxType *) * nargs);
    plx_fn->arg_names = plx_names = palloc0(sizeof(char *) * nargs);
    MemoryContextSwitchTo(old_ctx);


    for (i = 0; i < nargs; i++)
    {
        char mode = modes ? modes[i] : PROARGMODE_IN;
        switch (mode)
        {
            case PROARGMODE_IN:
            case PROARGMODE_INOUT:
                plx_types[i] = new_plx_type(types[i], plx_fn->mctx);
                plx_names[i] = mctx_strcpy(plx_fn->mctx, names[i]);
                plx_fn->nargs++;
                if (plx_fn->is_binary && !OidIsValid(plx_types[i]->oid))
                    plx_fn->is_binary = 0;
                break;
            case PROARGMODE_VARIADIC:
                plx_error(plx_fn, "Plexor does not support variadic args");
                break;
            case PROARGMODE_OUT:
            case PROARGMODE_TABLE:
                break;
            default:
                plx_error(plx_fn, "Plexor: unknown value in proargmodes: %c", mode);
                break;
        }
    }
}

static void
fill_plx_fn_ret_type(PlxFn* plx_fn, FunctionCallInfo fcinfo)
{
    Oid       oid;
    TupleDesc tuple_desc;

    switch(get_call_result_type(fcinfo, &oid, &tuple_desc))
    {
        case TYPEFUNC_SCALAR:
        case TYPEFUNC_COMPOSITE:
            plx_fn->ret_type = new_plx_type(oid, plx_fn->mctx);
            break;
        default:
            return;
    }
    if (plx_fn->is_binary && !OidIsValid(&plx_fn->ret_type->oid))
        plx_fn->is_binary = 0;
    plx_fn->ret_type_mod = (oid == RECORDOID) ? tuple_desc->tdtypmod : -1;
    plx_fn->is_return_void = oid == VOIDOID;
}

static void
parse_plx_fn(PlxFn *plx_fn, HeapTuple proc_tuple)
{
    bool  isnull;
    Datum src_raw, src_detoast;

    src_raw = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_prosrc, &isnull);
    if (isnull)
        plx_error(plx_fn, "procedure source datum is null");

    src_detoast = PointerGetDatum(PG_DETOAST_DATUM_PACKED(src_raw));
    run_plexor_parser(plx_fn, VARDATA_ANY(src_detoast), VARSIZE_ANY_EXHDR(src_detoast));
}

static PlxFn *
new_plx_fn()
{
    PlxFn *plx_fn = MemoryContextAllocZero(plx_fn_mctx, sizeof(PlxFn));
    plx_fn->mctx = plx_fn_mctx;
    return plx_fn;
}

PlxFn *
compile_plx_fn(FunctionCallInfo fcinfo, HeapTuple proc_tuple, bool is_validate)
{
    Form_pg_proc  proc_struct;
    PlxFn        *plx_fn;

    proc_struct = (Form_pg_proc) GETSTRUCT(proc_tuple);
    if (proc_struct->provolatile != PROVOLATILE_VOLATILE)
        elog(ERROR, "Plexor functions must be volatile");

    plx_fn = new_plx_fn();
    plx_fn->oid = HeapTupleGetOid(proc_tuple);
    fill_plx_fn_name(plx_fn, proc_struct);
    fill_plx_fn_arg_types(plx_fn, proc_tuple);
    parse_plx_fn(plx_fn, proc_tuple);

    if (is_validate)
        return plx_fn;

    plx_fn->is_binary = 0; // fixme try to use 1 and text only if binary failed
    plx_fn->is_return_untyped_record = is_fn_returns_dynamic_record(proc_tuple);
    if (!plx_fn->run_query)
        plx_fn->run_query = create_plx_query_from_plx_fn(plx_fn);
    fill_plx_fn_ret_type(plx_fn, fcinfo);
    plx_set_stamp(&plx_fn->stamp, proc_tuple);
    return plx_fn;
}

static bool
is_plx_fn_todate(PlxFn *plx_fn, HeapTuple proc_tuple)
{
    int i;

    if (!plx_check_stamp(&(plx_fn->stamp), proc_tuple))
        return false;
    for (i = 0; i < plx_fn->nargs; i++)
        if (!is_plx_type_todate(plx_fn->arg_types[i]))
            return false;
    if (!is_plx_type_todate(plx_fn->ret_type))
        return false;
    return true;
}

void
delete_plx_fn(PlxFn *plx_fn, bool is_cache_delete)
{
    int i;

    if (plx_fn->name)
        pfree(plx_fn->name);
    if (plx_fn->cluster_name)
        pfree(plx_fn->cluster_name);
    if (plx_fn->hash_query)
        delete_plx_query(plx_fn->hash_query);
    if (plx_fn->run_query)
        delete_plx_query(plx_fn->run_query);
    if (plx_fn->arg_types)
    {
        for (i = 0; i < plx_fn->nargs; i++)
            pfree(plx_fn->arg_types[i]);
        pfree(plx_fn->arg_types);
    }
    if (plx_fn->arg_names)
    {
        for (i = 0; i < plx_fn->nargs; i++)
            pfree(plx_fn->arg_names[i]);
        pfree(plx_fn->arg_names);
    }
    if (plx_fn->ret_type)
        pfree(plx_fn->ret_type);
    if (is_cache_delete)
        plx_fn_cache_delete(plx_fn->oid);
    pfree(plx_fn);
}

PlxFn *
get_plx_fn(FunctionCallInfo fcinfo)
{
    HeapTuple  proc_tuple;
    PlxFn     *plx_fn = plx_fn_lookup_cache(fcinfo->flinfo->fn_oid);

    proc_tuple = SearchSysCache(PROCOID, ObjectIdGetDatum(fcinfo->flinfo->fn_oid), 0, 0, 0);
    if (!HeapTupleIsValid(proc_tuple))
        elog(ERROR, "cache lookup failed for function %u", fcinfo->flinfo->fn_oid);

    if (plx_fn && !is_plx_fn_todate(plx_fn, proc_tuple))
    {
        delete_plx_fn(plx_fn, true);
        plx_fn = NULL;
    }

    if (!plx_fn)
    {
        plx_fn = compile_plx_fn(fcinfo, proc_tuple, false);
        plx_fn_insert_cache(plx_fn);
    }
    ReleaseSysCache(proc_tuple);
    return plx_fn;
}
