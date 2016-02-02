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