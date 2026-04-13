#ifndef RUNTIME_GUARD_H
#define RUNTIME_GUARD_H

#include <stdbool.h>

typedef struct DuckDBRuntimeFingerprint
{
	const char *duckdb_version;
	const char *module_path;
	const char *duckdb_symbol_path;
	const char *peer_module_path;
	const char *peer_runtime_path;
	bool		peer_loaded;
	bool		source_unproven;
} DuckDBRuntimeFingerprint;

typedef enum DuckDBRuntimeCompatibilityStatus
{
	DUCKDB_RUNTIME_NO_PEER_LOADED,
	DUCKDB_RUNTIME_PEER_LOADED_NEED_VALIDATION,
	DUCKDB_RUNTIME_COMPATIBLE_PROVEN,
	DUCKDB_RUNTIME_COMPATIBLE_UNPROVEN,
	DUCKDB_RUNTIME_INCOMPATIBLE
} DuckDBRuntimeCompatibilityStatus;

extern DuckDBRuntimeCompatibilityStatus duckdb_runtime_guard_status(void);
extern void duckdb_runtime_guard_fingerprint(DuckDBRuntimeFingerprint *fingerprint);
extern void duckdb_runtime_guard_check(void);
extern const char *duckdb_runtime_status_name(DuckDBRuntimeCompatibilityStatus status);

#endif
