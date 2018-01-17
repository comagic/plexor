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

PlxResult*
new_plx_result(PlxConn *plx_conn, PlxFn *plx_fn, PGresult *pg_result, MemoryContext mctx)
{
    PlxResult *plx_result;

    plx_result = MemoryContextAllocZero(mctx, sizeof(PlxResult));
    plx_result->plx_conn = plx_conn;
    plx_result->plx_fn = plx_fn;
    plx_result->pg_result = pg_result;
    return plx_result;
}

static void
setFixedStringInfo(StringInfo str, void *data, int len)
{
    str->data = data;
    str->maxlen = len;
    str->len = len;
    str->cursor = 0;
}

Datum
get_row(FunctionCallInfo fcinfo, PlxFn *plx_fn, PGresult *pg_result, int nrow)
{
    StringInfoData  buf;
    Datum           ret;

    fcinfo->isnull = !PQntuples(pg_result) || PQgetisnull(pg_result, nrow, 0);
    if (fcinfo->isnull)
        return (Datum) NULL;

    if (plx_fn->ret_type->oid == RECORDOID)
    {
        Oid       oid;
        TupleDesc tuple_desc;

        get_call_result_type(fcinfo, &oid, &tuple_desc);
        if (
            tuple_desc &&
            /* 2 means ( and ), tuple_desc->natts - 1 means ',' count */
	    /* example: natts = 3 => (,,) */
            PQgetlength(pg_result, nrow, 0) == 2 + tuple_desc->natts - 1
        )
        {
            fcinfo->isnull = 1;
            return (Datum) NULL;
        }
    }

    PG_TRY();
    {
        setFixedStringInfo(&buf,
                           PQgetvalue(pg_result, nrow, 0),
                           PQgetlength(pg_result, nrow, 0));

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
        PQclear(pg_result);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return ret;
}

Datum
get_next_row(FunctionCallInfo fcinfo)
{
    PlxResult       *plx_result;
    FuncCallContext *funcctx;
    int              call_cntr;

    funcctx = SRF_PERCALL_SETUP();
    plx_result = funcctx->user_fctx;
    call_cntr = funcctx->call_cntr;
    if (call_cntr < funcctx->max_calls)
        SRF_RETURN_NEXT(funcctx, get_row(fcinfo,
                                         plx_result->plx_fn,
                                         plx_result->pg_result,
                                         call_cntr));
    PQclear(plx_result->pg_result);
    if (plx_result->plx_fn->run_on == RUN_ON_ALL &&
        plx_result->plx_conn->nnode + 1 < plx_result->plx_conn->plx_cluster->nnodes)
    {
        PlxCluster *plx_cluster = plx_result->plx_conn->plx_cluster;
        PlxFn      *plx_fn = plx_result->plx_fn;
        PlxConn    *plx_conn = get_plx_conn(plx_cluster, plx_result->plx_conn->nnode + 1);

	funcctx->user_fctx = NULL;
        pfree(plx_result);
        remote_retset_execute(plx_conn, plx_fn, fcinfo, false);
        return get_next_row(fcinfo);
    }
    pfree(plx_result);
    SRF_RETURN_DONE(funcctx);
}
