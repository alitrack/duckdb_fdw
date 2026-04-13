#include "postgres.h"
#include "duckdb.h"
#include "runtime_guard.h"

#include <string.h>

extern bool duckdb_fdw_allow_unsupported_pg_duckdb_coexistence;

static bool duckdb_runtime_unsupported_warning_emitted = false;

#ifdef __linux__
#include <dlfcn.h>
#include <link.h>

typedef struct DuckDBRuntimeScanContext
{
	DuckDBRuntimeFingerprint fingerprint;
	int			pg_duckdb_module_count;
} DuckDBRuntimeScanContext;

static const char *
duckdb_runtime_basename(const char *path)
{
	const char *slash;

	if (path == NULL)
		return NULL;

	slash = strrchr(path, '/');
	return slash == NULL ? path : slash + 1;
}

static bool
duckdb_runtime_is_duckdb_library_path(const char *path)
{
	const char *base;

	if (path == NULL || path[0] == '\0')
		return false;

	base = duckdb_runtime_basename(path);
	return base != NULL && strncmp(base, "libduckdb", strlen("libduckdb")) == 0;
}

static bool
duckdb_runtime_is_pg_duckdb_module_path(const char *path)
{
	const char *base;

	if (path == NULL || path[0] == '\0')
		return false;

	base = duckdb_runtime_basename(path);
	return base != NULL && strncmp(base, "pg_duckdb", strlen("pg_duckdb")) == 0;
}

static int
duckdb_runtime_scan_loaded_object(struct dl_phdr_info *info, size_t size, void *data)
{
	DuckDBRuntimeScanContext *context = (DuckDBRuntimeScanContext *) data;

	(void) size;

	if (duckdb_runtime_is_pg_duckdb_module_path(info->dlpi_name))
	{
		context->pg_duckdb_module_count++;
		context->fingerprint.peer_loaded = true;
		if (context->fingerprint.peer_module_path == NULL)
			context->fingerprint.peer_module_path = info->dlpi_name;
	}

	if (!duckdb_runtime_is_duckdb_library_path(info->dlpi_name))
		return 0;

	if (context->fingerprint.module_path != NULL &&
		strcmp(info->dlpi_name, context->fingerprint.module_path) != 0 &&
		context->fingerprint.peer_runtime_path == NULL)
		context->fingerprint.peer_runtime_path = info->dlpi_name;

	return 0;
}

static DuckDBRuntimeCompatibilityStatus
duckdb_runtime_guard_status_linux(void)
{
	Dl_info		self_info;
	DuckDBRuntimeScanContext context;

	memset(&self_info, 0, sizeof(self_info));
	if (dladdr((void *) duckdb_library_version, &self_info) == 0)
		return DUCKDB_RUNTIME_COMPATIBLE_UNPROVEN;

	if (!duckdb_runtime_is_duckdb_library_path(self_info.dli_fname))
		return DUCKDB_RUNTIME_COMPATIBLE_UNPROVEN;

	memset(&context, 0, sizeof(context));
	context.fingerprint.module_path = self_info.dli_fname;
	context.fingerprint.duckdb_symbol_path = self_info.dli_fname;

	dl_iterate_phdr(duckdb_runtime_scan_loaded_object, &context);

	if (context.pg_duckdb_module_count > 0)
		return DUCKDB_RUNTIME_PEER_LOADED_NEED_VALIDATION;

	if (context.fingerprint.module_path == NULL)
		return DUCKDB_RUNTIME_COMPATIBLE_UNPROVEN;

	return DUCKDB_RUNTIME_NO_PEER_LOADED;
}
#endif

DuckDBRuntimeCompatibilityStatus
duckdb_runtime_guard_status(void)
{
#ifdef __linux__
	return duckdb_runtime_guard_status_linux();
#else
	return DUCKDB_RUNTIME_COMPATIBLE_UNPROVEN;
#endif
}

void
duckdb_runtime_guard_fingerprint(DuckDBRuntimeFingerprint *fingerprint)
{
	memset(fingerprint, 0, sizeof(*fingerprint));

#ifdef __linux__
	{
		Dl_info		self_info;
		DuckDBRuntimeScanContext context;

		memset(&self_info, 0, sizeof(self_info));
		if (dladdr((void *) duckdb_library_version, &self_info) == 0)
		{
			fingerprint->source_unproven = true;
			return;
		}

		fingerprint->module_path = self_info.dli_fname;
		fingerprint->duckdb_symbol_path = self_info.dli_fname;
		fingerprint->duckdb_version = duckdb_library_version();

		if (!duckdb_runtime_is_duckdb_library_path(self_info.dli_fname))
		{
			fingerprint->source_unproven = true;
			return;
		}

		memset(&context, 0, sizeof(context));
		context.fingerprint = *fingerprint;
		dl_iterate_phdr(duckdb_runtime_scan_loaded_object, &context);

		*fingerprint = context.fingerprint;
		fingerprint->duckdb_version = duckdb_library_version();
		if (fingerprint->module_path == NULL)
			fingerprint->source_unproven = true;
	}
#else
	fingerprint->source_unproven = true;
#endif
}

static void
duckdb_runtime_guard_error(DuckDBRuntimeCompatibilityStatus status)
{
	const char *detail;

	switch (status)
	{
		case DUCKDB_RUNTIME_PEER_LOADED_NEED_VALIDATION:
			detail = "The current backend already loaded pg_duckdb, but duckdb_fdw cannot prove same-backend runtime compatibility.";
			break;
		case DUCKDB_RUNTIME_COMPATIBLE_UNPROVEN:
			detail = "duckdb_fdw could not prove that the active DuckDB runtime is safe for supported execution.";
			break;
		case DUCKDB_RUNTIME_INCOMPATIBLE:
			detail = "duckdb_fdw detected an incompatible DuckDB runtime combination in the current backend.";
			break;
		default:
			detail = "duckdb_fdw rejected the active DuckDB runtime combination.";
			break;
	}

	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("strict coexistence policy rejected the current DuckDB runtime combination"),
			 errdetail("%s Runtime status: \"%s\".", detail, duckdb_runtime_status_name(status)),
			 errhint("Use a backend without peer-loaded pg_duckdb, or explicitly set duckdb_fdw.allow_unsupported_pg_duckdb_coexistence = on for unsupported experiments.")));
}

void
duckdb_runtime_guard_check(void)
{
	DuckDBRuntimeCompatibilityStatus status = duckdb_runtime_guard_status();

	switch (status)
	{
		case DUCKDB_RUNTIME_NO_PEER_LOADED:
		case DUCKDB_RUNTIME_COMPATIBLE_PROVEN:
			return;

		case DUCKDB_RUNTIME_PEER_LOADED_NEED_VALIDATION:
		case DUCKDB_RUNTIME_COMPATIBLE_UNPROVEN:
		case DUCKDB_RUNTIME_INCOMPATIBLE:
			if (duckdb_fdw_allow_unsupported_pg_duckdb_coexistence)
			{
				if (!duckdb_runtime_unsupported_warning_emitted)
				{
					duckdb_runtime_unsupported_warning_emitted = true;
					ereport(WARNING,
							(errmsg("duckdb_fdw is running in unsupported pg_duckdb coexistence mode"),
							 errdetail("The current backend reported runtime status \"%s\".", duckdb_runtime_status_name(status)),
							 errhint("Use a backend without peer-loaded pg_duckdb for supported execution.")));
				}
				return;
			}

			duckdb_runtime_guard_error(status);
			return;
	}
}

const char *
duckdb_runtime_status_name(DuckDBRuntimeCompatibilityStatus status)
{
	switch (status)
	{
		case DUCKDB_RUNTIME_NO_PEER_LOADED:
			return "no_peer_loaded";
		case DUCKDB_RUNTIME_PEER_LOADED_NEED_VALIDATION:
			return "peer_loaded_need_validation";
		case DUCKDB_RUNTIME_COMPATIBLE_PROVEN:
			return "compatible_proven";
		case DUCKDB_RUNTIME_COMPATIBLE_UNPROVEN:
			return "compatible_unproven";
		case DUCKDB_RUNTIME_INCOMPATIBLE:
			return "incompatible";
	}

	return "unknown";
}
