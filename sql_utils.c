#include "postgres.h"
#include "duckdb_fdw.h"

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
	if (strstr(input, "--") != NULL)
		return false;
	if (strstr(input, "/*") != NULL || strstr(input, "*/") != NULL)
		return false;
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
