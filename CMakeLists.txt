cmake_minimum_required(VERSION 3.20)
project(duckdb_fdw LANGUAGES C CXX)

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 11)

# --- 1. Locate PostgreSQL Installation Environment ---
if(WIN32)
    set(PG_PATH "C:/Program Files/PostgreSQL/18" CACHE PATH "Path to PostgreSQL installation")
    
    include_directories("${PG_PATH}/include/server")
    include_directories("${PG_PATH}/include/server/port/win32")
    include_directories("${PG_PATH}/include/server/port/win32_msvc")
    include_directories("${PG_PATH}/include")
    link_directories("${PG_PATH}/lib")
else()
    find_program(PG_CONFIG pg_config)
    if(NOT PG_CONFIG)
        message(FATAL_ERROR "pg_config executable not found. Ensure PostgreSQL binaries are in your PATH.")
    endif()

    execute_process(COMMAND ${PG_CONFIG} --includedir-server OUTPUT_VARIABLE PG_INCLUDE_SERVER OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND ${PG_CONFIG} --includedir OUTPUT_VARIABLE PG_INCLUDE OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND ${PG_CONFIG} --pkglibdir OUTPUT_VARIABLE PG_PKGLIB OUTPUT_STRIP_TRAILING_WHITESPACE)

    include_directories(${PG_INCLUDE_SERVER} ${PG_INCLUDE})
    link_directories(${PG_PKGLIB})
endif()

include_directories(".")
set(DUCKDB_PATH "${CMAKE_CURRENT_SOURCE_DIR}" CACHE PATH "Path to DuckDB files")
include_directories(${DUCKDB_PATH})
link_directories(${DUCKDB_PATH})

# --- 2. Define the Extension Target ---
set(SOURCE_FILES
    duckdb_fdw.c
    connection.c
    option.c
    deparse.c
    nanoarrow.c
    import.c
    sql_utils.c
    runtime_guard.c
)

add_library(duckdb_fdw SHARED ${SOURCE_FILES})

# --- 3. Target Compiler Definitions and Platform Customization ---
if(WIN32)
    target_compile_definitions(duckdb_fdw PRIVATE
        WIN32
        _WINDOWS
        __POINTER_INT_DEFINED
    )
    
    if(MSVC)
        target_compile_options(duckdb_fdw PRIVATE "/wd4005" "/wd4273")
    endif()

    target_link_libraries(duckdb_fdw PRIVATE postgres.lib duckdb)
else()
    # Explicitly activate GNU extensions needed by runtime_guard.c on Linux
    target_compile_definitions(duckdb_fdw PRIVATE _GNU_SOURCE)
    
    # Safely set CXX standard via target properties to avoid corrupting C files
    set_target_properties(duckdb_fdw PROPERTIES CXX_STANDARD 11 CXX_STANDARD_REQUIRED ON)
    
    target_link_libraries(duckdb_fdw PRIVATE duckdb stdc++)

    if(CMAKE_SYSTEM_NAME MATCHES "Linux")
        target_link_libraries(duckdb_fdw PRIVATE dl)
        
        set_target_properties(duckdb_fdw PROPERTIES 
            INSTALL_RPATH "$ORIGIN:${PG_PKGLIB}"
            BUILD_WITH_INSTALL_RPATH TRUE
        )
    endif()
    set_target_properties(duckdb_fdw PROPERTIES PREFIX "")
endif()
