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
all                    { return ALL;     }

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
