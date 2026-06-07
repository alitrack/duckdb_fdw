<#
.SYNOPSIS
    Deploys the compiled duckdb_fdw extension artifacts into a PostgreSQL installation on Windows.
.PARAMETER PostgresBase
    The root path of the target PostgreSQL installation. Defaults to 'C:\Program Files\PostgreSQL\18'.
.PARAMETER SourceDir
    The root directory of the duckdb_fdw source code repository. Defaults to the current working directory.
.PARAMETER BuildDir
    The directory containing the compiled duckdb_fdw.dll. Defaults to '$SourceDir\build'.
.EXAMPLE
    .\install_duckdb_fdw.ps1
.EXAMPLE
    .\install_duckdb_fdw.ps1 -PostgresBase "C:\Program Files\PostgreSQL\17"
#>
param (
    [Parameter(Mandatory = $false)]
    [string]$PostgresBase = "C:\Program Files\PostgreSQL\18",

    [Parameter(Mandatory = $false)]
    [string]$SourceDir = "",

    [Parameter(Mandatory = $false)]
    [string]$BuildDir = ""
)

# Step 1: Dynamically establish the Source Directory context
if ([string]::IsNullOrWhiteSpace($SourceDir)) {
    if (-not [string]::IsNullOrWhiteSpace($PSScriptRoot)) {
        # Use the folder location where this script file lives
        $SourceDir = $PSScriptRoot
    } else {
        # Fallback straight to your active terminal's current working directory
        $SourceDir = (Get-Location).Path
    }
}

# Step 2: Establish the Build Directory context relative to the Source Directory
if ([string]::IsNullOrWhiteSpace($BuildDir)) {
    $BuildDir = Join-Path $SourceDir "build"
}

# Resolve structural PostgreSQL target subdirectories
$pgLib = Join-Path $PostgresBase "lib"
$pgBin = Join-Path $PostgresBase "bin"
$pgExt = Join-Path $PostgresBase "share\extension"

# Validate administrative context required for Program Files mutation
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin -and ($PostgresBase -like "*Program Files*")) {
    Write-Error "Administrative privileges required. Please re-run this PowerShell session as an Administrator."
    exit 1
}

# Ensure destination paths physically exist before copying
foreach ($dir in @($pgLib, $pgBin, $pgExt)) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
}

Write-Host "Starting deployment of duckdb_fdw targeting: $PostgresBase" -ForegroundColor Cyan
Write-Host "Source Directory (Current Context): $SourceDir" -ForegroundColor Gray
Write-Host "Build Directory  (Binary Location): $BuildDir`n" -ForegroundColor Gray

# --- 1. Deploy Compiled Extension Binary (DLL) ---
$compiledDll = Join-Path $BuildDir "duckdb_fdw.dll"
if (Test-Path $compiledDll) {
    Copy-Item -Path $compiledDll -Destination $pgLib -Force
    Write-Host "[OK] duckdb_fdw.dll successfully deployed to $pgLib" -ForegroundColor Green
} else {
    Write-Error "Could not find duckdb_fdw.dll at '$BuildDir'. Verify your CMake compilation completed successfully in Release mode."
    exit 1
}

# --- 2. Deploy Extension Control and SQL Scripts ---
$controlFile = Join-Path $SourceDir "duckdb_fdw.control"
if (Test-Path $controlFile) {
    Copy-Item -Path $controlFile -Destination $pgExt -Force
    
    # Select and deploy all matching version migration mapping scripts
    $sqlFiles = Get-ChildItem -Path (Join-Path $SourceDir "duckdb_fdw--*.sql") -ErrorAction SilentlyContinue
    if ($sqlFiles) {
        Copy-Item -Path $sqlFiles.FullName -Destination $pgExt -Force
        Write-Host "[OK] Control file and SQL migration scripts deployed to $pgExt" -ForegroundColor Green
    } else {
        Write-Warning "Control file found, but no duckdb_fdw--*.sql initialization scripts detected in '$SourceDir'."
    }
} else {
    Write-Error "Could not locate 'duckdb_fdw.control' inside target source directory: $SourceDir"
    exit 1
}

# --- 3. Deploy Dependent DuckDB Core Engine Runtime ---
$duckdbDll = Join-Path $SourceDir "duckdb.dll"
if (Test-Path $duckdbDll) {
    Copy-Item -Path $duckdbDll -Destination $pgBin -Force
    Write-Host "[OK] Core engine runtime (duckdb.dll) deployed to $pgBin" -ForegroundColor Green
} else {
    Write-Warning "duckdb.dll was not found in the root of '$SourceDir'. Ensure that duckdb.dll is reachable via the PostgreSQL engine's system PATH."
}

# Extract and display target version identifier for the confirmation hint
$pgVersionHint = if ($PostgresBase -match "\d+$") { $Matches[0] } else { "PostgreSQL" }
Write-Host "`nDeployment completed successfully." -ForegroundColor Green
Write-Host "Remember to restart the PostgreSQL $pgVersionHint service before executing 'CREATE EXTENSION duckdb_fdw;'" -ForegroundColor Yellow
