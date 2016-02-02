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