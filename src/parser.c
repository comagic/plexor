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


/* lexer block begin */

typedef enum TokenType
{
    IDENT         = 1,
    CLUSTER       = 2,
    RUN           = 3,
    ON            = 4,
    ANY           = 5,
    ALL           = 6,
    ALL_COALESCE  = 7,
    FUNCTION      = 8,
    O_PARENTHESIS = 9,
    C_PARENTHESIS = 10,
    COMMA         = 11,
    NUMBER        = 12,
    SEMICOLON     = 13,
    COALESCE      = 14,
} TokenType;


typedef struct Token
{
    TokenType  type;
    char      *value;
} Token;


typedef struct TokenList
{
    Token            *token;
    struct TokenList *next;
    struct TokenList *prev;
} TokenList;


typedef struct Lexer
{
    TokenList  *token_list;
    Token     **tokens;
    int         count;
    int         cluster_i;
    int         run_i;
    int         on_i;
} Lexer;

static int
isnumber(char *p) {
    for (; *p; p++)
        if (!isdigit(*p))
            return 0;
    return 1;
}

static void
get_token_start_len(PlxFn *plx_fn, const char *text, int text_len, int *pos, int *start, int *len)
{

    for (; *pos < text_len; (*pos)++)
    {
        if (isalpha(text[*pos]) || isdigit(text[*pos]) || text[*pos] == '_')
        {
            if (*start == -1)
            {
                *start = *pos;
                *len = 1;
            }
            else
                *len = *pos - *start + 1;
        }
        else if (
            text[*pos] == ' '  ||
            text[*pos] == '\t' ||
            text[*pos] == '\n'
        )
        {
            if (*start != -1)
                return;
        }
        else if (
            text[*pos] == ';' ||
            text[*pos] == ',' ||
            text[*pos] == '(' ||
            text[*pos] == ')'
        )
        {
            if (*start != -1)
                return;
            else
            {
                *start = *pos;
                *len = 1;
                (*pos)++;
                return;
            }
        }
        else
            plx_syntax_error(plx_fn, "unexpected symbol '%c'", text[*pos]);
    }
}

static Token *
get_token(PlxFn *plx_fn, const char *text, int text_len, int *pos, Token *prev)
{
    Token *token = NULL;
    int    start = -1;
    int    len   = -1;

    get_token_start_len(plx_fn, text, text_len, pos, &start, &len);

    if (start != -1 && len != -1)
    {
        token = palloc0(sizeof(Token));
        token->value = palloc0(len + 1);
        memcpy(token->value, text + start, len);
        token->value[len] = 0;

        if      (!strcmp(token->value, "cluster"))
            token->type = CLUSTER;
        else if (!strcmp(token->value, "run"))
            token->type = RUN;
        else if (!strcmp(token->value, "on"))
            token->type = ON;
        else if (!strcmp(token->value, "any"))
            token->type = ANY;
        else if (!strcmp(token->value, "all"))
            token->type = ALL;
        else if (!strcmp(token->value, "coalesce"))
        {
            if (prev && prev->type == ALL)
            {
                int prev_len = strlen(prev->value);

                prev->type = ALL_COALESCE;
                prev->value = repalloc(prev->value, len + 2);

                prev->value[prev_len] = ' ';
                memcpy(prev->value + prev_len + 1, text + start, len);
                prev->value[prev_len + len + 2] = 0;

                pfree(token->value);
                pfree(token);

                return prev;
            }
            else
                token->type = IDENT;
        }
        else if (!strcmp(token->value, ";"))
            token->type = SEMICOLON;
        else if (!strcmp(token->value, ","))
            token->type = COMMA;
        else if (!strcmp(token->value, "("))
        {
            token->type = O_PARENTHESIS;
            if (prev->type == IDENT)
                prev->type = FUNCTION;

        }
        else if (!strcmp(token->value, ")"))
            token->type = C_PARENTHESIS;
        else if (isnumber(token->value))
            token->type = NUMBER;
        else
            token->type = IDENT;
    }
    return token;
}

static Lexer *
get_lexer(PlxFn *plx_fn, const char *text, int text_len)
{
    Lexer     *lexer      = palloc0(sizeof(Lexer));
    TokenList *token_list = NULL;
    TokenList *cur        = NULL;
    Token     *token      = NULL;
    int        pos        = 0;

    while((token = get_token(plx_fn, text, text_len, &pos, token)))
    {
        if (!cur)
        {
            token_list = cur = palloc0(sizeof(TokenList));
            cur->token = token;
        }
        else if (cur->token == token)
            continue;
        else
        {
            cur->next = palloc0(sizeof(TokenList));
            cur->next->prev = cur;
            cur = cur->next;
            cur->token = token;
        }
        lexer->count++;
    }

    lexer->token_list = token_list;
    lexer->tokens = palloc0(sizeof(Token *) * lexer->count);
    lexer->cluster_i = lexer->run_i = lexer->on_i = -1;
    cur = token_list;
    for (pos = 0; pos < lexer->count; pos++)
    {
        token = cur->token;
        cur   = cur->next;
        lexer->tokens[pos] = token;
        if      (token->type == CLUSTER)
            lexer->cluster_i = pos;
        else if (token->type == RUN)
            lexer->run_i = pos;
        else if (token->type == ON)
            lexer->on_i = pos;
    }

    return lexer;
}

/* lexer block end and parser block begin */

typedef struct PlxClusterStmt
{
    char *name;
} PlxClusterStmt;

typedef struct PlxFnStmt
{
    char   *name;
    Token **tokens;
    int     count;
} PlxFnStmt;

typedef struct PlxHashStmt
{
    PlxFnStmt *fn_stmt;
    char      *nnode;
    char      *anode;
    int        is_any;
    int        is_all;
    int        is_all_coalesce;
} PlxHashStmt;

typedef struct PlxRunStmt
{
    PlxFnStmt   *fn_stmt;
    PlxHashStmt *hash_stmt;
} PlxRunStmt;

typedef struct PlxStmt
{
    PlxClusterStmt *cluster_stmt;
    PlxRunStmt     *run_stmt;
} PlxStmt;


static int token_index(PlxFn *plx_fn, Lexer *lexer, int start, TokenType type, const char * fmt, ...)
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 5, 6)));

static int
token_index(PlxFn *plx_fn, Lexer *lexer, int start, TokenType type, const char * fmt, ...)
{
    char     msg[1024];
    va_list  ap;
    int i;

    for (i = start; i < lexer->count; i++)
        if (lexer->tokens[i]->type == type)
            return i;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    plx_syntax_error(plx_fn, "%s", msg);

    return -1;
}

static PlxClusterStmt *
get_cluster_stmt(PlxFn *plx_fn, Lexer *lexer)
{
    PlxClusterStmt *clustert_stmt;

    if (lexer->cluster_i == -1)
        plx_syntax_error(plx_fn, "'cluster' keyword not found");

    if (lexer->count < 3 || lexer->tokens[2]->type != SEMICOLON)
        plx_syntax_error(plx_fn, "no ';' at the end of cluster statement");

    if (lexer->tokens[1]->type != IDENT)
        plx_syntax_error(plx_fn, "cluster name is not valid identifier");

    clustert_stmt = palloc0(sizeof(ClusterStmt));
    clustert_stmt->name = lexer->tokens[1]->value;
    return clustert_stmt;
}

static PlxFnStmt *
get_fn_stmt(PlxFn *plx_fn, Lexer *lexer, int start)
{
    PlxFnStmt *fn_stmt = NULL;
    Token     *token   = NULL;
    int        i, j;
    int        end;

    end = token_index(plx_fn, lexer, start+1, C_PARENTHESIS,
        "')' missed for function '%s'",
        lexer->tokens[start]->value
    );

    fn_stmt = palloc0(sizeof(PlxFnStmt));
    fn_stmt->name = lexer->tokens[start]->value;
    fn_stmt->count = end - start + 1;
    fn_stmt->tokens = palloc0(sizeof(Token *) * fn_stmt->count);
    fn_stmt->tokens[0] = lexer->tokens[start];
    fn_stmt->tokens[1] = lexer->tokens[start+1];
    fn_stmt->tokens[fn_stmt->count-1] = lexer->tokens[end];

    if (fn_stmt->count == 3)
        return fn_stmt;
    else
    {
        int is_wait_arg = 1;

        for (i = start + 2, j = 2; i < end; i++, j++)
        {
            token = lexer->tokens[i];
            if (is_wait_arg)
            {
                if ((token->type != IDENT) && (token->type != NUMBER))
                    plx_syntax_error(plx_fn,
                        "function '%s' call corrupted at '%s'",
                        lexer->tokens[start]->value,
                        lexer->tokens[i]->value

                    );
                is_wait_arg--;
            }
            else
            {
                if (token->type != COMMA)
                    plx_syntax_error(plx_fn,
                        "function '%s' call corrupted at '%s'",
                        lexer->tokens[start]->value,
                        lexer->tokens[i]->value
                    );
                is_wait_arg++;
            }
            fn_stmt->tokens[j] = lexer->tokens[i];
        }
        if (is_wait_arg)
            plx_syntax_error(plx_fn,
                "function '%s' argument missed",
                lexer->tokens[start]->value
            );
    }

    return fn_stmt;
}

static PlxHashStmt *
get_hash_stmt(PlxFn * plx_fn, Lexer *lexer)
{
    PlxHashStmt *plx_hash_stmt;
    Token       *token;
    int          start = lexer->on_i + 1;

    if (start == token_index(plx_fn, lexer, start, SEMICOLON, "hash statement not closed by ';'"))
        plx_syntax_error(plx_fn, "hash statement missed");

    plx_hash_stmt = palloc0(sizeof(PlxHashStmt));

    token = lexer->tokens[start];
    if      (token->type == IDENT)
        plx_hash_stmt->anode = token->value;
    else if (token->type == NUMBER)
        plx_hash_stmt->nnode = token->value;
    else if (token->type == FUNCTION)
        plx_hash_stmt->fn_stmt = get_fn_stmt(plx_fn, lexer, start);
    else if (token->type == ANY)
        plx_hash_stmt->is_any = 1;
    else if (token->type == ALL)
        plx_hash_stmt->is_all = 1;
    else if (token->type == ALL_COALESCE)
        plx_hash_stmt->is_all_coalesce = 1;

    return plx_hash_stmt;
}


static PlxRunStmt *
get_run_stmt(PlxFn *plx_fn, Lexer *lexer)
{
    PlxRunStmt *run_stmt;
    int         start = lexer->run_i + 1;

    if (lexer->run_i == -1)
        plx_syntax_error(plx_fn, "'run' keyword not found");
    if (lexer->on_i == -1)
        plx_syntax_error(plx_fn, "'on' keyword not found");

    run_stmt = palloc0(sizeof(PlxRunStmt));

    if (lexer->tokens[start]->type == FUNCTION)
        run_stmt->fn_stmt = get_fn_stmt(plx_fn, lexer, start);
    else if (lexer->tokens[start]->type != ON)
        plx_syntax_error(plx_fn, "invalid symbols between 'run' and 'on'");

    run_stmt->hash_stmt = get_hash_stmt(plx_fn, lexer);
    return run_stmt;
}

static PlxStmt *
get_plx_stmt(PlxFn *plx_fn, Lexer *lexer)
{
    PlxStmt *plx_stmt = palloc0(sizeof(PlxStmt));

    plx_stmt->cluster_stmt = get_cluster_stmt(plx_fn, lexer);
    plx_stmt->run_stmt     = get_run_stmt(plx_fn, lexer);

    return plx_stmt;
}

/* parser block end */

static PlxQuery *
fill_plx_q(PlxFn *plx_fn, PlxQuery *plx_q, PlxFnStmt *fn_stmt, int is_insert_select)
{
    int        i;

    if (is_insert_select)
        appendStringInfo(plx_q->sql, "select ");

    for (i = 0; i < fn_stmt->count; i++)
    {
        Token *token = fn_stmt->tokens[i];

        if (token->type == FUNCTION ||
            token->type == O_PARENTHESIS ||
            token->type == C_PARENTHESIS ||
            token->type == NUMBER ||
            token->type == COMMA
        )
            appendStringInfo(plx_q->sql, "%s", fn_stmt->tokens[i]->value);
        else
            append_plx_query_arg_index(plx_q, plx_fn, fn_stmt->tokens[i]->value);
    }
    return plx_q;
}

static void
fill_plx_fn(PlxFn *plx_fn, PlxStmt *plx_stmt)
{
    PlxClusterStmt *cluster_stmt = plx_stmt->cluster_stmt;
    PlxRunStmt     *run_stmt     = plx_stmt->run_stmt;
    PlxHashStmt    *hash_stmt    = run_stmt->hash_stmt;

    plx_fn->cluster_name = mctx_strcpy(plx_fn->mctx, cluster_stmt->name);
    if (run_stmt->fn_stmt)
        plx_fn->run_query = fill_plx_q(plx_fn, new_plx_query(plx_fn->mctx), run_stmt->fn_stmt, 0);

    if (hash_stmt->anode)
    {
        plx_fn->run_on = RUN_ON_ANODE;
        fill_plx_fn_anode(plx_fn, hash_stmt->anode);
    }
    else if (hash_stmt->nnode)
    {
        plx_fn->run_on = RUN_ON_NNODE;
        plx_fn->nnode = atoi(hash_stmt->nnode);
    }
    else if (hash_stmt->is_any)
        plx_fn->run_on = RUN_ON_ANY;
    else if (hash_stmt->is_all)
        plx_fn->run_on = RUN_ON_ALL;
    else if (hash_stmt->is_all_coalesce)
        plx_fn->run_on = RUN_ON_ALL_COALESCE;
    else if (hash_stmt->fn_stmt)
    {
        plx_fn->run_on = RUN_ON_HASH;
        plx_fn->hash_query = fill_plx_q(plx_fn, new_plx_query(plx_fn->mctx), hash_stmt->fn_stmt, 1);
    }
}

// static void
// notice_plx_stmt(char *func_name, PlxStmt *plx_stmt)
// {
//     int i;

//     elog(NOTICE, "%s()", func_name);
//     if (plx_stmt)
//     {
//         if (plx_stmt->cluster_stmt)
//             elog(NOTICE, "\tcluster: %s", plx_stmt->cluster_stmt->name);
//         if (plx_stmt->run_stmt)
//         {
//             if (plx_stmt->run_stmt->fn_stmt)
//             {
//                 elog(NOTICE, "\trun function: %s %d", plx_stmt->run_stmt->fn_stmt->name, plx_stmt->run_stmt->fn_stmt->count);
//                 for (i = 0; i < plx_stmt->run_stmt->fn_stmt->count; i++)
//                     elog(NOTICE, "\t\t%s", plx_stmt->run_stmt->fn_stmt->tokens[i]->value);
//             }
//             else
//                 elog(NOTICE, "\trun function: %p", plx_stmt->run_stmt->fn_stmt);
//             if (plx_stmt->run_stmt->hash_stmt)
//             {
//                 elog(NOTICE, "\thash anode: %s", plx_stmt->run_stmt->hash_stmt->anode);
//                 elog(NOTICE, "\thash nnode: %s", plx_stmt->run_stmt->hash_stmt->nnode);
//                 elog(NOTICE, "\thash any: %d", plx_stmt->run_stmt->hash_stmt->is_any);
//                 elog(NOTICE, "\thash all: %d", plx_stmt->run_stmt->hash_stmt->is_all);
//                 elog(NOTICE, "\thash all_coalesce: %d", plx_stmt->run_stmt->hash_stmt->is_all_coalesce);
//                 if (plx_stmt->run_stmt->hash_stmt->fn_stmt)
//                 {
//                     elog(NOTICE, "\thash function: %s", plx_stmt->run_stmt->hash_stmt->fn_stmt->name);
//                     for (i = 0; i < plx_stmt->run_stmt->hash_stmt->fn_stmt->count; i++)
//                         elog(NOTICE, "\t\t%s", plx_stmt->run_stmt->hash_stmt->fn_stmt->tokens[i]->value);
//                 }
//                 else
//                     elog(NOTICE, "\thash function: %p", plx_stmt->run_stmt->hash_stmt->fn_stmt);

//             }
//         }
//     }
// }

void
parse(PlxFn *plx_fn, const char *body, int len)
{

    Lexer   *lexer    = get_lexer(plx_fn, body, len);
    PlxStmt *plx_stmt = get_plx_stmt(plx_fn, lexer);

    fill_plx_fn(plx_fn, plx_stmt);
}
