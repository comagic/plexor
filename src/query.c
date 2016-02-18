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
    int       i;

    appendStringInfo(plx_q->sql, "%s", plx_fn->name);
    appendStringInfo(plx_q->sql, "(");
    for (i = 1; i <= plx_fn->nargs; i++)
        appendStringInfo(plx_q->sql, "$%d%s", i, i < plx_fn->nargs ? "," : "");
    appendStringInfo(plx_q->sql, ")");

    plx_q->plx_fn_arg_indexes = MemoryContextAllocZero(plx_fn->mctx,
                                                       sizeof(int) * plx_fn->nargs);
    for (i = 0; i < plx_fn->nargs; i++)
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
