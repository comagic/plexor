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

#ifndef __plexor_h__
#define __plexor_h__

#include <postgres.h>
#include <commands/trigger.h>
#include <commands/defrem.h>
#include <catalog/pg_foreign_server.h>
#include <catalog/pg_foreign_data_wrapper.h>
#include <catalog/pg_user_mapping.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <access/htup_details.h>
#include <access/reloptions.h>
#include <access/hash.h>
#include <access/xact.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/typcache.h>
#include <utils/memutils.h>
#include <utils/acl.h>
#include <executor/spi.h>
#include <foreign/foreign.h>
#include <lib/stringinfo.h>
#include <sys/epoll.h>
#include <funcapi.h>
#include <libpq-fe.h>
#include <miscadmin.h>
#include <stdlib.h>
#include <time.h>

// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>


/*
longest string of {auto commit | {read committed | serializable } \
    [read write | read only] [ [ not ] deferrable] }
is "read committed read write not deferrable" (42 chars)
*/
#define MAX_ISOLATION_LEVEL_LEN 42
#define MAX_DSN_LEN 1024
#define MAX_NODES 100
#define MAX_RESULTS_PER_EXPR 128
#define MAX_CONNECTIONS 128
#define TYPED_SQL_TMPL "select %s"
#define UNTYPED_SQL_TMPL "select x from (select * from %s as (%s)) as x"

/* tuple stamp */
typedef struct TupleStamp {
    TransactionId   xmin;
    ItemPointerData tid;
} TupleStamp;

static inline void
plx_set_stamp(TupleStamp *stamp, HeapTuple tuple)
{
    stamp->xmin = HeapTupleHeaderGetXmin(tuple->t_data);
    stamp->tid = tuple->t_self;
}

static inline bool
plx_check_stamp(TupleStamp *stamp, HeapTuple tuple)
{
    return stamp->xmin == HeapTupleHeaderGetXmin(tuple->t_data)
           && ItemPointerEquals(&stamp->tid, &tuple->t_self);
}

/* Copy string using specified context */
static inline char *
mctx_strcpy(MemoryContext mctx, const char *s)
{
    int len;
    if (s == NULL)
        return NULL;
    len = strlen(s) + 1;
    return memcpy(MemoryContextAlloc(mctx, len), s, len);
}


typedef struct PlxCluster
{
    Oid             oid;                            /* foreign server OID  */
    char            name[NAMEDATALEN];              /* foreign server name */
    char           *isolation_level;
    int             connection_lifetime;
    char            nodes[MAX_NODES][MAX_DSN_LEN];  /* node DSNs           */
    int             nnodes;                         /* nodes count         */
} PlxCluster;


typedef struct PlxType
{
    Oid             oid;                     /* type OID */
    FmgrInfo        send_fn;                 /* OID of binary out convert procedure  */
    FmgrInfo        receive_fn;              /* OID of binary in  convert procedure  */
    FmgrInfo        output_fn;               /* OID of text   out convert procedure  */
    FmgrInfo        input_fn;                /* OID of text   in  convert procedure  */
    Oid             receive_io_params;       /* OID to pass to I/O convert procedure */
    TupleStamp      stamp;                   /* stamp to check type up to date       */
} PlxType;


typedef struct PlxQuery
{
    StringInfo      sql;                     /* sql that contain query                   */
    int            *plx_fn_arg_indexes;      /* indexes of plx_fn that use this PlxQuery */
    int             nargs;                   /* plx_fn_arg_indexes len                   */
} PlxQuery;


typedef enum RunOnType
{
    RUN_ON_HASH         = 1,                 /* node returned by hash function         */
    RUN_ON_NNODE        = 2,                 /* exact node number                      */
    RUN_ON_ANY          = 3,                 /* decide randomly during runtime         */
    RUN_ON_ANODE        = 4,                 /* get node number from function argument */
    RUN_ON_ALL          = 5,                 /* return all nodes (for retset)          */
    RUN_ON_ALL_COALESCE = 6,                 /* return all nodes (for single)          */
} RunOnType;

typedef struct PlxFn
{
    MemoryContext   mctx;                    /* function MemoryContext                     */
    Oid             oid;                     /* function OID                               */
    char           *name;                    /* function name                              */
    char           *cluster_name;            /* cluster name at which "run on" function
                                                will be called                             */
    RunOnType       run_on;                  /* type of method to find node to run on      */
    int             nnode;                   /* node number (RUN_ON_NNODE)                 */
    int             anode;                   /* argument index that contain node number
                                                (RUN_ON_ANODE)                             */
    PlxQuery       *hash_query;              /* query to find node to run on (RUN_ON_HASH) */
    PlxQuery       *run_query;               /* query that will be run on node             */
    PlxType       **arg_types;               /* plexor function arguments types            */
    char          **arg_names;               /* plexor function arguments names            */
    int             nargs;                   /* plexor function arguments count            */
    PlxType        *ret_type;                /* plexor function return type                */
    int             ret_type_mod;            /* tdtypmod for record or -1                  */
    bool            is_binary;               /* use binary fotmat to transfer values       */
    bool            is_return_untyped_record;/* return type is untyped record              */
    bool            is_return_void;          /* return type is untyped record              */
    TupleStamp      stamp;                   /* stamp to determinate function upadte       */
} PlxFn;

typedef struct PlxConn
{
    PlxCluster     *plx_cluster;             /* cluster date                               */
    PGconn         *pq_conn;                 /* connection to node                         */
    int             nnode;                   /* node number                                */
    char           *dsn;                     /* node dns                                   */
    int             xlevel;                  /* transaction nest level                     */
    time_t          connect_time;            /* time at which connection was opened        */
} PlxConn;

typedef struct PlxResult
{
    PlxFn          *plx_fn;                  /* plexor function the result is user for     */
    PGresult       *pg_result;               /* result from node                           */
    PlxConn        *plx_conn;                /* pointer to external connection             */
} PlxResult;

/* Structure to keep plx_conn in HTAB's context. */
typedef struct PlxConnHashEntry
{
    char            key[MAX_DSN_LEN];        /* Key value. Must be at the start */
    PlxConn        *plx_conn;                /* Pointer to connection data */
} PlxConnHashEntry;


/* cluster.c */

void        plx_cluster_cache_init(void);
PlxCluster *get_plx_cluster(char* name);
void        delete_plx_cluster(PlxCluster *plx_cluster);
bool        extract_node_num(const char *node_name, int *node_num);

/* type.c */
bool     is_plx_type_todate(PlxType *plx_type);
PlxType *new_plx_type(Oid oid, MemoryContext mctx);


/* query.c */
PlxQuery *new_plx_query(MemoryContext mctx);
void      delete_plx_query(PlxQuery *plx_q);
void      append_plx_query_arg_index(PlxQuery *plx_q, PlxFn *plx_fn, const char *name);
PlxQuery *create_plx_query_from_plx_fn(PlxFn *plx_fn);

/* function.c*/
void   plx_fn_cache_init(void);
PlxFn *compile_plx_fn(FunctionCallInfo fcinfo, HeapTuple proc_tuple, bool is_validate);
PlxFn *get_plx_fn(FunctionCallInfo fcinfo);
PlxFn *plx_fn_lookup_cache(Oid fn_oid);
void   delete_plx_fn(PlxFn *plx_fn, bool is_cache_delete);
void   fill_plx_fn_anode(PlxFn* plx_fn, const char *anode_name);
int    plx_fn_get_arg_index(PlxFn *plx_fn, const char *name);


/* result.c */
PlxResult* new_plx_result(PlxConn *plx_conn, PlxFn *plx_fn, PGresult *pg_result, MemoryContext mctx);
Datum get_row(FunctionCallInfo fcinfo, PlxFn *plx_fn, PGresult *pg_result, int nrow);
Datum get_next_row(FunctionCallInfo fcinfo);


/* connection.c */
void     plx_conn_cache_init(void);
PlxConn *get_plx_conn(PlxCluster *plx_cluster, int nnode);
void     delete_plx_conn(PlxConn *plx_conn);


/* transaction.c */
void start_transaction(PlxConn* plx_conn);


/* plexor.c */
void plx_error_with_errcode(PlxFn *plx_fn, int err_code, const char *fmt, ...)
     __attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));
#define plx_error(func,...) plx_error_with_errcode((func), ERRCODE_INTERNAL_ERROR, __VA_ARGS__)
#define plx_syntax_error(func,...) plx_error_with_errcode((func), ERRCODE_SYNTAX_ERROR , __VA_ARGS__)


/* parser.c */
void parse(PlxFn *plx_fn, const char *body, int len);

/* execute.c */
void execute_init(void);
Datum remote_single_execute(PlxConn *plx_conn, PlxFn *plx_fn, FunctionCallInfo fcinfo);
void remote_retset_execute(PlxConn *plx_conn, PlxFn *plx_fn, FunctionCallInfo fcinfo, bool is_first_call);

#endif
