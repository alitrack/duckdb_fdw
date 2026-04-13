#include "postgres.h"
#include "duckdb.h"
#include "runtime_guard.h"

#include <string.h>

#ifdef __linux__
#include <dlfcn.h>
#include <link.h>

typedef struct DuckDBRuntimeScanContext
{
	const char *self_path;
	int			duckdb_object_count;
	int			foreign_duckdb_object_count;
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

static int
duckdb_runtime_scan_loaded_object(struct dl_phdr_info *info, size_t size, void *data)
{
	DuckDBRuntimeScanContext *context = (DuckDBRuntimeScanContext *) data;

	(void) size;

	if (!duckdb_runtime_is_duckdb_library_path(info->dlpi_name))
		return 0;

	context->duckdb_object_count++;
	if (strcmp(info->dlpi_name, context->self_path) != 0)
		context->foreign_duckdb_object_count++;

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
	context.self_path = self_info.dli_fname;

	dl_iterate_phdr(duckdb_runtime_scan_loaded_object, &context);

	if (context.foreign_duckdb_object_count > 0)
		return DUCKDB_RUNTIME_PEER_LOADED_NEED_VALIDATION;

	if (context.duckdb_object_count == 0)
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
duckdb_runtime_guard_check(void)
{
	switch (duckdb_runtime_guard_status())
	{
		case DUCKDB_RUNTIME_NO_PEER_LOADED:
		case DUCKDB_RUNTIME_COMPATIBLE_PROVEN:
		case DUCKDB_RUNTIME_COMPATIBLE_UNPROVEN:
			return;

		case DUCKDB_RUNTIME_PEER_LOADED_NEED_VALIDATION:
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("DuckDB runtime compatibility is not yet validated"),
					 errdetail("Detected another DuckDB shared library loaded in this backend."),
					 errhint("Use a backend without peer-loaded DuckDB libraries until runtime validation is implemented.")));
			return;

		case DUCKDB_RUNTIME_INCOMPATIBLE:
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("DuckDB runtime is incompatible"),
					 errdetail("The loaded DuckDB shared library set does not satisfy runtime compatibility requirements.")));
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
