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
$zipName = "libduckdb-${platform}-${arch}.zip"
$downloadUrl = "https://github.com/duckdb/duckdb/releases/download/${versionTag}/${zipName}"
$outputZip = "duckdb-temp.zip"

Write-Host "Downloading from: $downloadUrl"

try {
    Invoke-WebRequest -Uri $downloadUrl -OutFile $outputZip
}
catch {
    Write-Error "Failed to download file: $_"
}

Write-Host "Extracting..."
try {
    Expand-Archive -Path $outputZip -DestinationPath . -Force
}
catch {
    Write-Error "Failed to extract archive: $_"
}

# Cleanup
Remove-Item -Path $outputZip -Force

Write-Host "Done. libduckdb.dll should be in the current directory."
