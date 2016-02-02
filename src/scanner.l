%{
#include "plexor.h"
#include "parser.tab.h"
#include <stdio.h>

/* disable stdio related code */
#define YY_INPUT(buf, res, maxlen) { res = 0; }

#define YY_NO_INPUT

/* point to parser value */
#define yylval plexor_yylval


/* flex version have to be >= 2.5.31 */
void *
yyalloc(size_t nbytes)
{
    return palloc0(nbytes);
}
void *
yyrealloc(void * ptr, size_t nbytes)
{
    return repalloc(ptr, nbytes);
}

void
yyfree(void * ptr)
{
    pfree(ptr);
}

void
plexor_yylex_prepare(void)
{
    (yy_buffer_stack) = NULL;
}

%}

%option noyyalloc noyyfree noyyrealloc
%option 8bit case-insensitive
%option warn nodefault yylineno
%option nounput noyywrap never-interactive
%option prefix="plexor_yy"

/* states */
%x args
%x comment

/* whitespace */
SPACE                  [ \t\n\r]

/* ident.  include dotted parts also */
WORD                   [_a-z\200-\377][a-z0-9_\200-\377]*
IDENT                  {WORD}({SPACE}*[.]{SPACE}*{WORD})*

/* argument ref by val: $1 */
NUMIDENT               [$][0-9]+

/* regular int value for hash spec */
NUMBER                 [0-9]+

%%

cluster                { return CLUSTER; }
run                    { return RUN;     }
on                     { return ON;      }
any                    { return ANY;     }

    /* function call */
{IDENT}{SPACE}*[(]     { BEGIN(args); yylval.str = yytext; return FNCALL; }

    /* Plexor language comments/whitespace */
{SPACE}+               { }
[-][-][^\n]*           { }
[/][*]                 { BEGIN(comment); }
<comment>[^*/]+        { }
<comment>[*]+[^*/]+    { }
<comment>[*]+[/]       { BEGIN(INITIAL); }
<comment>.             { }

    /* Plexor non-keyword elements */
{IDENT}                { yylval.str = yytext; return IDENT; }
{NUMIDENT}             { yylval.str = yytext; return IDENT; }
{NUMBER}               { yylval.str = yytext; return NUMBER; }

    /* unparsed symbol, let parser decide */
.                      { return *(yytext); }

    /* arg identifier */
<args>{IDENT}          { yylval.str = yytext; return IDENT; }

    /* $x argument reference */
<args>{NUMIDENT}       { yylval.str = yytext; return IDENT; }

    /* arg symbol, parse them one-by-one */
<args>,                { yylval.str = yytext; return ','; }

    /* compress whitespace to singe ' ' */
<args>{SPACE}+         { }

    /* arg statement end */
<args>[)]              { BEGIN(INITIAL); return ')'; }

    /* unparsed symbol, let the parser error out */
<args>.                { return *(yytext); }

%%
