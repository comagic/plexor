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


/* list of all the valid configuration options to plexor cluster */
static const char *cluster_config_options[] = {
    "connection_lifetime",
    "isolation_level",
    NULL
};

/* list of all the valid isolation levels */
static const char *isolation_levels[] = {
    "auto commit",
    "read committed",
    "read committed read only",
    NULL
};

// extern Datum plproxy_fdw_validator(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(plexor_fdw_validator);


static void
validate_isolation_level(const char *value)
{
    const char **level;

    for (level = isolation_levels; *level; level++)
        if (pg_strcasecmp(*level, value) == 0)
            break;
    if (*level == NULL)
        elog(ERROR, "Plexor: invalid isolation_level value: %s", value);
}

static void
validate_connection_lifetime(const char *value)
{
    char *endptr;

    strtoul(value, &endptr, 10);
    if (*endptr != '\0')
        elog(ERROR, "Plexor: invalid connection_lifetime value: %s", value);
}

static void
validate_cluster_option(const char *name, const char *value)
{
    const char **opt;

    /* see that a valid config option is specified */
    for (opt = cluster_config_options; *opt; opt++)
        if (pg_strcasecmp(*opt, name) == 0)
            break;
    if (*opt == NULL)
        elog(ERROR, "Plexor: invalid server option: %s", name);

    if (pg_strcasecmp("isolation_level", name) == 0)
        validate_isolation_level(value);
    if (pg_strcasecmp("connection_lifetime", name) == 0)
        validate_connection_lifetime(value);
}

/*
 * Validate plexor foreign server and user mapping options
 */
Datum
plexor_fdw_validator(PG_FUNCTION_ARGS)
{
    List     *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid       catalog      = PG_GETARG_OID(1);
    int       node_count   = 0;
    ListCell *cell;

    foreach(cell, options_list)
    {
        DefElem *def = lfirst(cell);
        char    *arg = strVal(def->arg);
        int      node_num;

        if (catalog == ForeignServerRelationId)
        {
            if (extract_node_num(def->defname, &node_num))
            {
                /* node definition */
                if (node_num != node_count)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("Plexor: nodes must be numbered consecutively"),
                             errhint("next valid node number is %d", node_count)));
                if (strstr(arg, "dbname") == NULL)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("Plexor: option %s, no 'dbname' in value '%s'",
                                    def->defname, arg)));
                if (strstr(arg, "user") != NULL)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("Plexor: 'user' must be set in user mapping, "\
                                    "not in option '%s'",
                                    def->defname)));
                if (strstr(arg, "password") != NULL)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("Plexor: 'password' must be set in user mapping, "\
                                    "not in option '%s'",
                                    def->defname)));
                ++node_count;
            }
            else
                /* option from cluster_config_options definition */
                validate_cluster_option(def->defname, arg);
        }
        else if (catalog == UserMappingRelationId)
        {
            /* user mapping only accepts "user" and "password" */
            if (pg_strcasecmp(def->defname, "user") != 0 &&
                pg_strcasecmp(def->defname, "password") != 0)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("PLexor: \"user\" or \"password\" was skipped")));
            }
        }
    }

    PG_RETURN_BOOL(true);
}