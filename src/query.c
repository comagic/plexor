#include "plexor.h"

/*
 * Add a function argument index to the query.
 *
 * Function argument may be a parameter reference.
 */
void
append_plx_query_arg_index(PlxQuery *plx_q, PlxFn *plx_fn, const char *name)
{
    int           arg_idx = plx_fn_get_arg_index(plx_fn, name);
    MemoryContext old_ctx;

    if (arg_idx < 0)
        return;

    old_ctx = MemoryContextSwitchTo(plx_fn->mctx);
    if (!plx_q->nargs)
        plx_q->plx_fn_arg_indexes = palloc0(sizeof(int));
    else
        plx_q->plx_fn_arg_indexes = repalloc(plx_q->plx_fn_arg_indexes,
                                             sizeof(int) * (plx_q->nargs + 1));
    plx_q->plx_fn_arg_indexes[plx_q->nargs] = arg_idx;
    plx_q->nargs++;
    appendStringInfo(plx_q->sql, "$%d", plx_q->nargs);
    MemoryContextSwitchTo(old_ctx);
}

PlxQuery *
create_plx_query_from_plx_fn(PlxFn *plx_fn)
{
    PlxQuery *plx_q = new_plx_query(plx_fn->mctx);

    appendStringInfo(plx_q->sql, "%s", plx_fn->name);
    appendStringInfo(plx_q->sql, "(");
    for (int i = 1; i <= plx_fn->nargs; i++)
        appendStringInfo(plx_q->sql, "$%d%s", i, i < plx_fn->nargs ? "," : "");
    appendStringInfo(plx_q->sql, ")");

    plx_q->plx_fn_arg_indexes = MemoryContextAllocZero(plx_fn->mctx,
                                                       sizeof(int) * plx_fn->nargs);
    for (int i = 0; i < plx_fn->nargs; i++)
        plx_q->plx_fn_arg_indexes[i] = i;
    plx_q->nargs = plx_fn->nargs;

    return plx_q;
}

void
delete_plx_query(PlxQuery *plx_q)
{
    if (plx_q->sql)
    {
        pfree(plx_q->sql->data);
	pfree(plx_q->sql);
    }
    if (plx_q->plx_fn_arg_indexes)
        pfree(plx_q->plx_fn_arg_indexes);
    pfree(plx_q);
}

PlxQuery *
new_plx_query(MemoryContext mctx)
{
    PlxQuery      *plx_q;
    MemoryContext  old_ctx;

    old_ctx = MemoryContextSwitchTo(mctx);
    plx_q = palloc0(sizeof(PlxQuery));
    plx_q->sql = makeStringInfo();
    MemoryContextSwitchTo(old_ctx);
    return plx_q;
}
