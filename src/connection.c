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


/* Permanent memory area for connection info structures */
static MemoryContext plx_conn_mctx;

/* Cluster cache */
HTAB *plx_conn_cache = NULL;

/* Initialize plexor connection cache */
void
plx_conn_cache_init(void)
{
    HASHCTL       ctl;
    int           flags;
    int           max_conns = MAX_CONNECTIONS;
    MemoryContext old_ctx;

    /* don't allow multiple initializations */
    if (plx_conn_cache)
        return;

    plx_conn_mctx = AllocSetContextCreate(TopMemoryContext,
                                          "Plexor connections context",
                                          ALLOCSET_SMALL_MINSIZE,
                                          ALLOCSET_SMALL_INITSIZE,
                                          ALLOCSET_SMALL_MAXSIZE);
    MemSet(&ctl, 0, sizeof(ctl));
    ctl.keysize = MAX_DSN_LEN;
    ctl.entrysize = sizeof(PlxConnHashEntry);
    ctl.hash = string_hash;
    ctl.hcxt = plx_conn_mctx;
    flags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT;

    old_ctx = MemoryContextSwitchTo(plx_conn_mctx);
    plx_conn_cache = hash_create("Plexor connections cache", max_conns, &ctl, flags);
    MemoryContextSwitchTo(old_ctx);
}

/* Search for connection in cache */
static PlxConn*
plx_conn_lookup_cache(const char *dsn)
{
    PlxConnHashEntry *hentry;

    hentry = hash_search(plx_conn_cache, dsn, HASH_FIND, NULL);
    if (hentry)
        return hentry->plx_conn;
    return NULL;
}

/* Insert connection into cache */
static void
plx_conn_insert_cache(PlxConn *plx_conn)
{
    PlxConnHashEntry *hentry;
    bool              found;

    hentry = hash_search(plx_conn_cache, plx_conn->dsn, HASH_ENTER, &found);
    if (found)
        elog(ERROR, "connection '%s' is already in cache", plx_conn->dsn);
    hentry->plx_conn = plx_conn;
}


/* Delete connection from cache */
static void
plx_conn_cache_delete(const char *dsn)
{
    hash_search(plx_conn_cache, dsn, HASH_REMOVE, NULL);
}

static UserMapping *
get_user_mapping(PlxCluster *plx_cluster)
{
    UserMapping *um = GetUserMapping(GetUserId(), plx_cluster->oid);
    AclResult    acl_result;

    /* Check permissions, user must have usage on the server. */
    acl_result = pg_foreign_server_aclcheck(um->serverid, um->userid, ACL_USAGE);
    if (acl_result != ACLCHECK_OK)
        aclcheck_error(acl_result, ACL_KIND_FOREIGN_SERVER, plx_cluster->name);

    return um;
}

static char *
get_user(PlxCluster *plx_cluster)
{
    ListCell *cell;

    foreach(cell, get_user_mapping(plx_cluster)->options)
    {
        DefElem *def = lfirst(cell);

        if (strcmp(def->defname, "user") == 0)
            return strVal(def->arg);
    }
    return GetUserNameFromId(GetUserId(), false);
}

static StringInfo
get_dsn(PlxCluster *plx_cluster, const char *dsn)
{
    bool        got_user = false;
    ListCell   *cell;
    StringInfo  buf;

    buf = makeStringInfo();
    appendStringInfo(buf, "%s", dsn);

    foreach(cell, get_user_mapping(plx_cluster)->options)
    {
        DefElem *def = lfirst(cell);

        if (strcmp(def->defname, "user") == 0)
        {
            appendStringInfo(buf, " user=%s", strVal(def->arg));
            got_user = true;
        }
        if (strcmp(def->defname, "password") == 0)
            appendStringInfo(buf, " password=%s", strVal(def->arg));
    }
    if (!got_user)
        appendStringInfo(buf, " user=%s", GetUserNameFromId(GetUserId(), false));
    return buf;
}

void
delete_plx_conn(PlxConn *plx_conn)
{
    if (plx_conn_lookup_cache(plx_conn->dsn))
        plx_conn_cache_delete(plx_conn->dsn);
    if (plx_conn->dsn)
        pfree(plx_conn->dsn);
    if (plx_conn->pq_conn)
        PQfinish(plx_conn->pq_conn);
    pfree(plx_conn);
}

static PlxConn*
new_plx_conn(PlxCluster *plx_cluster, char *dsn)
{
    PlxConn        *plx_conn;
    struct timeval  now;
    MemoryContext   old_ctx;

    old_ctx = MemoryContextSwitchTo(plx_conn_mctx);
    plx_conn = palloc0(sizeof(PlxConn));
    plx_conn->plx_cluster = plx_cluster;
    plx_conn->dsn = pstrdup(dsn);
    plx_conn->pq_conn = PQconnectdb(plx_conn->dsn);
    MemoryContextSwitchTo(old_ctx);
    gettimeofday(&now, NULL);
    plx_conn->connect_time = now.tv_sec;
    return plx_conn;
}

static bool
is_lifetime_is_over(PlxConn *plx_conn)
{
    PlxCluster *plx_cluster = plx_conn->plx_cluster;
    struct timeval now;

    gettimeofday(&now, NULL);
    if (plx_cluster->connection_lifetime > 0 &&
        (now.tv_sec - plx_conn->connect_time) > plx_cluster->connection_lifetime)
        return true;
    return false;
}

PlxConn*
get_plx_conn(PlxCluster *plx_cluster, int nnode)
{
    PlxConn    *plx_conn = NULL;
    char       *raw_dsn;
    StringInfo  dsn;

    raw_dsn = plx_cluster->nodes[nnode];
    if (!strlen(raw_dsn))
        elog(ERROR, "node %d of cluster (%s) not defined", nnode, plx_cluster->name);

    dsn = get_dsn(plx_cluster, raw_dsn);
    /* not necessary to free dsn, bacause it created in ExprContext */
    plx_conn = plx_conn_lookup_cache(dsn->data);
    if (plx_conn && is_lifetime_is_over(plx_conn))
    {
        delete_plx_conn(plx_conn);
        plx_conn = NULL;
    }

    if (plx_conn)
        return plx_conn;

    plx_conn = new_plx_conn(plx_cluster, dsn->data);
    if (PQsetnonblocking(plx_conn->pq_conn, 1))
    {
        char *error_message = pstrdup(PQerrorMessage(plx_conn->pq_conn));

        delete_plx_conn(plx_conn);
        elog(ERROR, "failed connect to '%s user=%s': %s",
            raw_dsn,
            get_user(plx_cluster),
            error_message);
    }
    plx_conn_insert_cache(plx_conn);
    return plx_conn;
}
