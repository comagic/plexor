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

extern HTAB *plx_conn_cache;

static bool is_remote_transaction = false;
static bool is_remote_subtransaction = false;


static void
subxact_callback(SubXactEvent event,
                 SubTransactionId mySubid,
                 SubTransactionId parentSubid,
                 void *arg)
{
    char              sql[1024]; //fixme
    HASH_SEQ_STATUS   scan;
    PlxConnHashEntry *entry = NULL;
    int               curlevel;

    /* Nothing to do at subxact start, nor after commit. */
    if (!is_remote_subtransaction ||
        !(event == SUBXACT_EVENT_PRE_COMMIT_SUB ||
          event == SUBXACT_EVENT_ABORT_SUB))
        return;

    curlevel = GetCurrentTransactionNestLevel();

    if (event == SUBXACT_EVENT_PRE_COMMIT_SUB)
        snprintf(sql, sizeof(sql), "release savepoint s%d", curlevel);
    else
        snprintf(sql, sizeof(sql),
                 "rollback to savepoint s%d; release savepoint s%d",
                 curlevel, curlevel);

    hash_seq_init(&scan, plx_conn_cache);
    while ((entry = (PlxConnHashEntry *) hash_seq_search(&scan)))
    {
        PlxConn *plx_conn = entry->plx_conn;
        if (plx_conn->xlevel < curlevel)
            continue;

        if (plx_conn->xlevel > curlevel)
            ereport(ERROR,
                    (errcode(ERRCODE_RAISE_EXCEPTION),
                     errmsg("missed cleaning up remote subtransaction at level")));

        if (!PQexec(plx_conn->pq_conn, sql))
            ereport(ERROR,
                    (errcode(ERRCODE_RAISE_EXCEPTION),
                     errmsg("error on %s", sql)));
        plx_conn->xlevel--;
    }
}

static void
xact_callback(XactEvent event, void *arg)
{
    HASH_SEQ_STATUS   scan;
    PlxConnHashEntry *entry = NULL;
    char             *sql   = NULL;

    switch (event)
    {
        case XACT_EVENT_PRE_COMMIT:
            sql = "commit;";
            break;
        case XACT_EVENT_ABORT:
            sql = "rollback;";
            break;
        case XACT_EVENT_PRE_PREPARE:
            ereport(ERROR,
                    (errcode(ERRCODE_RAISE_EXCEPTION),
                     errmsg("cannot prepare a transaction that modified remote tables")));
            break;
        case XACT_EVENT_COMMIT:
        case XACT_EVENT_PREPARE:
            /* Pre-commit should have closed the open transaction */
            ereport(ERROR,
                    (errcode(ERRCODE_RAISE_EXCEPTION),
                     errmsg("missed cleaning up connection during pre-commit"))); //FIXME cur_func in NULL
            break;
        default:
            return;
    }

    /*
     * Scan all connection cache entries to find open remote transactions, and
     * close them.
     */
    hash_seq_init(&scan, plx_conn_cache);
    while ((entry = (PlxConnHashEntry *) hash_seq_search(&scan)))
    {
        PlxConn *plx_conn = entry->plx_conn;

        if (plx_conn->xlevel > 0)
        {
            PGresult *pg_result;
            pg_result = PQexec(plx_conn->pq_conn, sql);
            if (pg_result)
                PQclear(pg_result);
            plx_conn->xlevel = 0;
        }

    }
    UnregisterXactCallback(xact_callback, NULL);
    UnregisterSubXactCallback(subxact_callback, NULL);
    is_remote_transaction = false;
}

void
start_transaction(PlxConn *plx_conn)
{
    int        curlevel;
    StringInfo sql = NULL;

    if (!strcmp(plx_conn->plx_cluster->isolation_level, "auto commit"))
        return;

    curlevel = GetCurrentTransactionNestLevel();
    if (!is_remote_transaction)
    {
        RegisterXactCallback(xact_callback, NULL);
        RegisterSubXactCallback(subxact_callback, NULL);
        is_remote_transaction = true;
    }

    if (plx_conn->xlevel == 0)
    {
        sql = makeStringInfo();
        appendStringInfo(sql,
                         "start transaction isolation level %s;",
                         plx_conn->plx_cluster->isolation_level);
        plx_conn->xlevel = 1;
    }

    while (plx_conn->xlevel < curlevel)
    {
        if (!sql)
            sql = makeStringInfo();
        appendStringInfo(sql, "savepoint s%d; ", (int) ++(plx_conn->xlevel));
        is_remote_subtransaction = true;
    }
    if (sql)
        PQclear(PQexec(plx_conn->pq_conn, sql->data));
}
