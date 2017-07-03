%{
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
#include "scanner.h"
#include <stdio.h>

#define YYDEBUG 0
int plexor_yydebug = 1;

/* avoid permanent allocations */
#define YYMALLOC palloc
#define YYFREE   pfree

/* remove unused code */
#define YY_LOCATION_PRINT(File, Loc) (0)
#define YY_(x) (x)

/* during parsing, keep reference to function here */
static PlxFn *xfunc;

/* remember what happened */
static int got_run, got_cluster;

/* keep the resetting code together with variables */
static void prepare_parser_vars(void)
{
    got_run = got_cluster = 0;
    xfunc = NULL;
    plexor_yylineno = 1;
}

/*
 * report parser error.
 */
void
plexor_yyerror(const char *fmt, ...)
{
    char    buf[1024];
    int     lineno = plexor_yyget_lineno();
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    plx_error(xfunc, "Compile error at line %d: %s", lineno, buf);
}

//#define YYDEBUG 1

%}

%name-prefix "plexor_yy"

%token <str> CLUSTER RUN ON NUMBER ANY ALL COALESCE
%token <str> IDENT FNCALL ',' ')'

// %printer { elog(NOTICE, "IDENT %s", $$); } IDENT
// %printer { elog(NOTICE, "FNCALL %s", $$); } FNCALL

%union
{
    const char *str;
}

%%

body: | body stmt ;

stmt: cluster_stmt | run_stmt;

cluster_stmt: CLUSTER cluster_name ';' { if (got_cluster)
                                             yyerror("Only one CLUSTER statement allowed");
                                         got_cluster = 1; }
            ;

cluster_name: IDENT { fill_plx_fn_cluster_name(xfunc, $1); }
            ;

run_stmt: RUN run_spec ON on_spec ';' { if (got_run)
                                            yyerror("Only one RUN statement allowed");
                                        got_run = 1; }
        ;

run_spec:
        | run_func run_args_token_list
        ;


run_func: FNCALL { xfunc->run_query = new_plx_query(xfunc->mctx);
                   appendStringInfo(xfunc->run_query->sql, "%s", $1); }
        ;

run_args_token_list: run_arg_token
                   | run_args_token_list run_arg_token
                   ;

run_arg_token: IDENT { append_plx_query_arg_index(xfunc->run_query, xfunc, $1); }
             | ','   { appendStringInfo(xfunc->run_query->sql, ","); }
             | ')'   { appendStringInfo(xfunc->run_query->sql, ")"); }
             ;

on_spec : hash_func hash_args_token_list { xfunc->run_on = RUN_ON_HASH; }
        | ANY                            { xfunc->run_on = RUN_ON_ANY; }
        | ALL                            { xfunc->run_on = RUN_ON_ALL; }
        | ALL COALESCE                   { xfunc->run_on = RUN_ON_ALL_COALESCE; }
        | NUMBER                         { xfunc->run_on = RUN_ON_NNODE;
                                           xfunc->nnode = atoi($1); }
        | IDENT                          { xfunc->run_on = RUN_ON_ANODE;
                                           fill_plx_fn_anode(xfunc, $1); }
        ;

hash_func: FNCALL { xfunc->hash_query = new_plx_query(xfunc->mctx);
                    appendStringInfo(xfunc->hash_query->sql, "select ");
                    appendStringInfo(xfunc->hash_query->sql, "%s", $1); }
         ;

hash_args_token_list: hash_args_token
                    | hash_args_token_list hash_args_token
                    ;

hash_args_token: IDENT { append_plx_query_arg_index(xfunc->hash_query, xfunc, $1); }
               | ','   { appendStringInfo(xfunc->hash_query->sql, ","); }
               | ')'   { appendStringInfo(xfunc->hash_query->sql, ")"); }
               ;
%%

/* actually run the flex/bison parser */
void
run_plexor_parser(PlxFn *plx_fn, const char *body, int len)
{
    /* prepare variables, in case there was error exit */
    prepare_parser_vars();
    /* make current function visible to parser */
    xfunc = plx_fn;
    /* prepare scanner */
    plexor_yylex_prepare();
    /* setup scanner */
    plexor_yy_scan_bytes(body, len);
    /* run parser */
    yyparse();
    /* check for mandatory statements */
    if (!got_cluster)
        plx_error(plx_fn, "CLUSTER statement missing");
    if (!got_run)
        plx_error(plx_fn, "RUN ON statement missing");
}

