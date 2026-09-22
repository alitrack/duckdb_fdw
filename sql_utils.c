#include "postgres.h"
#include "duckdb_fdw.h"
#include "commands/defrem.h"

#include <ctype.h>

static bool
contains_keyword_ci(const char *input, const char *keyword)
{
	size_t		keyword_len;
	const char *p;

	if (!input || !keyword)
		return false;

	keyword_len = strlen(keyword);
	for (p = input; *p; p++)
	{
		if (pg_strncasecmp(p, keyword, keyword_len) == 0)
			return true;
	}
	return false;
}

char *
duckdb_fdw_quote_literal(const char *input)
{
	StringInfoData buf;
	const char *p;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '\'');
	for (p = input ? input : ""; *p; p++)
	{
		if (*p == '\'')
			appendStringInfoString(&buf, "''");
		else
			appendStringInfoChar(&buf, *p);
	}
	appendStringInfoChar(&buf, '\'');
	return buf.data;
}

char *
duckdb_fdw_quote_identifier(const char *input)
{
	StringInfoData buf;
	const char *p;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '"');
	for (p = input ? input : ""; *p; p++)
	{
		if (*p == '"')
			appendStringInfoChar(&buf, '"');
		appendStringInfoChar(&buf, *p);
	}
	appendStringInfoChar(&buf, '"');
	return buf.data;
}

bool
duckdb_fdw_is_valid_identifier(const char *input)
{
	const unsigned char *p;

	if (!input || input[0] == '\0')
		return false;
	if (!(isalpha((unsigned char) input[0]) || input[0] == '_'))
		return false;

	for (p = (const unsigned char *) input + 1; *p; p++)
	{
		if (!(isalnum(*p) || *p == '_'))
			return false;
	}
	return true;
}

bool
duckdb_fdw_is_safe_sql_fragment(const char *input)
{
	const unsigned char *p;

	if (!input)
		return false;
	for (p = (const unsigned char *) input; *p; p++)
	{
		/* Disallow statement chaining, comments, and control characters */
		if (*p == ';')
			return false;
		if (*p < 32 && *p != '\t' && *p != '\n' && *p != '\r')
			return false;
	}
	/*
	 * Comment sequences only matter outside string literals: DuckDB globs in
	 * table options (e.g. a parquet path with wildcard stars) legitimately
	 * contain an asterisk and a slash-asterisk inside quotes, so a plain
	 * strstr for those sequences produced false positives (issue #72). Walk
	 * the fragment tracking single- and double-quoted string state and reject
	 * a block or line comment only when it appears outside a quoted string.
	 */
	for (p = (const unsigned char *) input; *p; p++)
	{
		if (*p == '\'' || *p == '"')
		{
			char		q = *p;

			/* skip to the closing quote; '' or "" is an escaped quote */
			while (*++p)
			{
				if (*p == q)
				{
					if (p[1] == q)
						p++;
					else
						break;
				}
			}
			if (*p == '\0')
				return false;	/* unterminated string literal */
		}
		else if (*p == '/' && p[1] == '*')
			return false;	/* block comment */
		else if (*p == '-' && p[1] == '-')
			return false;	/* line comment */
	}
	return true;
}

char *
duckdb_fdw_redact_secret_text(const char *input)
{
	if (!input)
		return pstrdup("DuckDB error");

	if (contains_keyword_ci(input, "SECRET") ||
		contains_keyword_ci(input, "KEY_ID") ||
		contains_keyword_ci(input, "ACCESS_KEY") ||
		contains_keyword_ci(input, "s3_secret_access_key") ||
		contains_keyword_ci(input, "TOKEN") ||
		contains_keyword_ci(input, "motherduck"))
	{
		return pstrdup("DuckDB operation failed (details redacted for security)");
	}

	return pstrdup(input);
}

char *
duckdb_fdw_trim_token(char *token)
{
	char *end;

	if (!token)
		return NULL;

	while (*token && isspace((unsigned char) *token))
		token++;
	if (*token == '\0')
		return token;

	end = token + strlen(token) - 1;
	while (end > token && isspace((unsigned char) *end))
	{
		*end = '\0';
		end--;
	}
	return token;
}

char *
duckdb_fdw_next_token(char *str, const char *delim, char **saveptr)
{
#ifdef _WIN32
	return strtok_s(str, delim, saveptr);
#else
	return strtok_r(str, delim, saveptr);
#endif
}

/*
 * Returns true when the foreign server was created with
 * force_readonly = true. Server-level option, checked directly from the
 * catalog (no foreign table involved).
 */
bool
duckdb_fdw_server_is_readonly(ForeignServer *server)
{
	ListCell   *lc;

	if (!server || !server->options)
		return false;

	foreach(lc, server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "force_readonly") == 0 &&
			defGetBoolean(def))
			return true;
	}
	return false;
}

/*
 * Conservative read-only classifier for duckdb_execute on force_readonly
 * servers. Only statements that cannot possibly modify DuckDB state are
 * accepted; everything else is rejected. This intentionally errs on the
 * side of refusal: a statement that is hard to classify is treated as a
 * write.
 *
 * Allowed first keywords: SELECT, FROM, WITH, VALUES, DESCRIBE, DESC,
 * SHOW, EXPLAIN, PRAGMA (query pragmas only; see below).
 */
bool
duckdb_fdw_sql_is_readonly(const char *sql)
{
	static const struct
	{
		const char *keyword;
		int			len;
	} readonly_kw[] = {
		{"SELECT", 6},
		{"FROM", 4},
		{"WITH", 4},
		{"VALUES", 6},
		{"DESCRIBE", 8},
		{"DESC", 4},
		{"SHOW", 4},
		{"EXPLAIN", 7},
	};
	const char *p;
	size_t		i;

	if (!sql)
		return false;

	/* Skip leading whitespace and comments */
	for (p = sql; *p; p++)
	{
		if (*p == '/' && p[1] == '*')
		{
			/* block comment: skip to closing */
			p += 2;
			while (*p && !(*p == '*' && p[1] == '/'))
				p++;
			if (*p == '\0')
				return false;	/* unterminated comment */
			continue;
		}
		if (*p == '-' && p[1] == '-')
		{
			/* line comment: skip to end of line */
			p += 2;
			while (*p && *p != '\n')
				p++;
			continue;
		}
		if (!isspace((unsigned char) *p))
			break;
	}

	/* Statement chaining is never allowed on read-only servers */
	for (i = (size_t) (p - sql); i < strlen(sql); i++)
		if (sql[i] == ';')
			return false;

	for (i = 0; i < lengthof(readonly_kw); i++)
	{
		if ((size_t) strlen(p) >= readonly_kw[i].len &&
			pg_strncasecmp(p, readonly_kw[i].keyword, readonly_kw[i].len) == 0 &&
			(p[readonly_kw[i].len] == '\0' ||
			 isspace((unsigned char) p[readonly_kw[i].len])))
			return true;
	}

	return false;
}
