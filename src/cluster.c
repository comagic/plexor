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


/* Structure to keep plx_cluster in HTAB's context. */
typedef struct
{
    /* Key value. Must be at the start */
    char        key[NAMEDATALEN];
    /* Pointer to cluster data */
    PlxCluster *plx_cluster;
} PlxClusterHashEntry;

/* Permanent memory area for cluster info structures */
static MemoryContext plx_cluster_mctx;

/* Cluster cache */
static HTAB *plx_cluster_cache = NULL;


/* Initialize plexor cluster cache */
void
plx_cluster_cache_init(void)
{
    HASHCTL        ctl;
    int            flags;
    int            max_clusters = 128;
    MemoryContext  old_ctx;

    /* don't allow multiple initializations */
    if(plx_cluster_cache)
        return;

    plx_cluster_mctx = AllocSetContextCreate(TopMemoryContext,
                                             "Plexor clusters context",
                                             ALLOCSET_SMALL_MINSIZE,
                                             ALLOCSET_SMALL_INITSIZE,
                                             ALLOCSET_SMALL_MAXSIZE);
    MemSet(&ctl, 0, sizeof(ctl));
    ctl.keysize = NAMEDATALEN;
    ctl.entrysize = sizeof(PlxClusterHashEntry);
    ctl.hash = string_hash;
    ctl.hcxt = plx_cluster_mctx;
    flags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT;

    old_ctx = MemoryContextSwitchTo(plx_cluster_mctx);
    plx_cluster_cache = hash_create("Plexor clusters cache", max_clusters, &ctl, flags);
    MemoryContextSwitchTo(old_ctx);
}

/* Search for cluster in cache */
static PlxCluster*
plx_cluster_lookup_cache(const char *name)
{
    PlxClusterHashEntry *hentry;

    hentry = hash_search(plx_cluster_cache, name, HASH_FIND, NULL);
    if (hentry)
        return hentry->plx_cluster;
    return NULL;
}

/* Insert cluster into cache */
static void
plx_cluster_insert_cache(PlxCluster *plx_cluster)
{
    PlxClusterHashEntry *hentry;
    bool                 found;

    hentry = hash_search(plx_cluster_cache, &plx_cluster->name, HASH_ENTER, &found);
    if (found)
        elog(ERROR, "cluster '%s' is already in cache", plx_cluster->name);
    hentry->plx_cluster = plx_cluster;
}


/* Delete cluster from cache */
static void
plx_cluster_cache_delete(const char *name)
{
    hash_search(plx_cluster_cache, name, HASH_REMOVE, NULL);
}

/*
 * Extract a node number from foreign server option
 */
bool
extract_node_num(const char *node_name, int *node_num)
{
    char  *node_tags[] = { "n", "node_", NULL };
    char **node_tag;
    char  *endptr;

    for (node_tag = node_tags; *node_tag; node_tag++)
        if (strstr(node_name, *node_tag) == node_name)
        {
            *node_num = (int) strtoul(node_name + strlen(*node_tag), &endptr, 10);
            if (*endptr == '\0')
                return true;
        }
    return false;
}

void
delete_plx_cluster(PlxCluster *plx_cluster)
{
    if (plx_cluster_lookup_cache(plx_cluster->name))
        plx_cluster_cache_delete(plx_cluster->name);
    if (plx_cluster->isolation_level)
        pfree(plx_cluster->isolation_level);
}

static PlxCluster*
new_plx_cluster(char* name)
{
    ForeignServer *foreign_server;
    PlxCluster    *plx_cluster;
    ListCell      *cell;

    foreign_server = GetForeignServerByName(name, true);
    if (!foreign_server)
        elog(ERROR, "cluster (%s) not found", name);

    plx_cluster = MemoryContextAllocZero(plx_cluster_mctx, sizeof(PlxCluster));
    /* isolation_level default value */
    plx_cluster->isolation_level = mctx_strcpy(plx_cluster_mctx, "read committed");
    plx_cluster->connection_lifetime = 0;
    plx_cluster->oid = foreign_server->serverid;
    strcpy(plx_cluster->name, foreign_server->servername);

    foreach(cell, foreign_server->options)
    {
        DefElem *def = lfirst(cell);
        int      node_num;

        if (extract_node_num(def->defname, &node_num))
            strcpy(plx_cluster->nodes[node_num], strVal(def->arg));
        else if (!strcmp(def->defname, "isolation_level"))
        {
            pfree(plx_cluster->isolation_level);
            plx_cluster->isolation_level = mctx_strcpy(plx_cluster_mctx, defGetString(def));
        }
        else if (!strcmp(def->defname, "connection_lifetime"))
        {
            char *endptr;
            plx_cluster->connection_lifetime = (int) strtoul(defGetString(def), &endptr, 10);
        }
    }
    return plx_cluster;
}

PlxCluster*
get_plx_cluster(char* name)
{
    PlxCluster *plx_cluster = plx_cluster_lookup_cache(name);

    if (plx_cluster)
        return plx_cluster;

    plx_cluster = new_plx_cluster(name);
    plx_cluster_insert_cache(plx_cluster);
    return plx_cluster;
}
