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
