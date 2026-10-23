# download_libduckdb.ps1
# PowerShell script to download the latest libduckdb for Windows

$ErrorActionPreference = "Stop"

function Get-LatestVersion {
    $url = "https://api.github.com/repos/duckdb/duckdb/releases/latest"
    try {
        $response = Invoke-RestMethod -Uri $url -Method Get
        return $response.tag_name
    }
    catch {
        Write-Error "Failed to fetch latest version from GitHub: $_"
    }
}

Write-Host "Checking for latest DuckDB version..."
$versionTag = Get-LatestVersion
# Remove 'v' prefix if present
$version = $versionTag -replace "^v", ""

Write-Host "Latest version: $versionTag"

$platform = "windows"
$arch = "amd64"

# DuckDB 2.0+ changed its release asset naming: instead of
# libduckdb-${platform}-${arch}.zip it ships
# duckdb-shared-libs-${platform}-${arch}.tar.gz (suffixes unchanged).
# Pick the matching asset base name + archive format by major version.
$major = ($version -split '\.')[0]
if ([int]$major -ge 2) {
    $assetBase = "duckdb-shared-libs"
    $assetExt  = "tar.gz"
    $assetName = "${assetBase}-${platform}-${arch}.${assetExt}"
    $output    = "duckdb-temp.tar.gz"
    # tar.gz is not a zip; Expand-Archive cannot handle it, but bsdtar
    # ships with Windows 10+.
    $extractCmd = { tar -xzf $using:output }
} else {
    $assetBase = "libduckdb"
    $assetExt  = "zip"
    $assetName = "${assetBase}-${platform}-${arch}.${assetExt}"
    $output    = "duckdb-temp.zip"
    $extractCmd = { Expand-Archive -Path $using:output -DestinationPath . -Force }
}

$downloadUrl = "https://github.com/duckdb/duckdb/releases/download/${versionTag}/${assetName}"

Write-Host "Downloading from: $downloadUrl"

try {
    Invoke-WebRequest -Uri $downloadUrl -OutFile $output
}
catch {
    Write-Error "Failed to download file: $_"
}

Write-Host "Extracting..."
try {
    & $extractCmd
}
catch {
    Write-Error "Failed to extract archive: $_"
}

# Cleanup
Remove-Item -Path $output -Force

# Verify the payload actually landed (fail here instead of at `make`).
if (-not (Test-Path "duckdb.h")) {
    Write-Error "download did not produce duckdb.h — is $downloadUrl valid? (404 assets yield an HTML page)"
}

Write-Host "Done. libduckdb.dll should be in the current directory."
