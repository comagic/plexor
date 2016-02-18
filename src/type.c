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

bool
is_plx_type_todate(PlxType *plx_type)
{
    HeapTuple type_tuple;
    bool      ret;

    type_tuple = SearchSysCache(TYPEOID, ObjectIdGetDatum(plx_type->oid), 0, 0, 0);
    if (!HeapTupleIsValid(type_tuple))
        elog(ERROR, "cache lookup failed for type %u", plx_type->oid);

    ret = plx_check_stamp(&plx_type->stamp, type_tuple);
    ReleaseSysCache(type_tuple);
    return ret;
}

PlxType *
new_plx_type(Oid oid, MemoryContext mctx)
{
    HeapTuple     type_tuple;
    Form_pg_type  type_struct;
    PlxType      *plx_type = NULL;

    type_tuple = SearchSysCache(TYPEOID, ObjectIdGetDatum(oid), 0, 0, 0);
    if (!HeapTupleIsValid(type_tuple))
        elog(ERROR, "cache lookup failed for type %u", oid);
    type_struct = (Form_pg_type) GETSTRUCT(type_tuple);

    plx_type = MemoryContextAllocZero(mctx, sizeof(PlxType));
    plx_type->oid = HeapTupleGetOid(type_tuple);
    fmgr_info_cxt(type_struct->typsend,    &plx_type->send_fn,    mctx);
    fmgr_info_cxt(type_struct->typreceive, &plx_type->receive_fn, mctx);
    fmgr_info_cxt(type_struct->typoutput,  &plx_type->output_fn,  mctx);
    fmgr_info_cxt(type_struct->typinput,   &plx_type->input_fn,   mctx);
    plx_type->receive_io_params = getTypeIOParam(type_tuple);
    plx_set_stamp(&plx_type->stamp, type_tuple);

    ReleaseSysCache(type_tuple);
    return plx_type;
}