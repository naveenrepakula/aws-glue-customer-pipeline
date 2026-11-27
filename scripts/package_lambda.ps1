# Build the Lambda deployment ZIP with handler.py at the archive root.
# Run from any PowerShell working directory:
#   .\scripts\package_lambda.ps1

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$handlerPath = Join-Path $repoRoot "lambda\trigger_glue\handler.py"
$distPath = Join-Path $repoRoot "dist"
$zipPath = Join-Path $distPath "trigger_glue.zip"
$tempPath = Join-Path ([System.IO.Path]::GetTempPath()) ("customer-lambda-" + [guid]::NewGuid().ToString("N"))

if (-not (Test-Path -LiteralPath $handlerPath -PathType Leaf)) {
    throw "Lambda handler not found: $handlerPath"
}

New-Item -ItemType Directory -Path $distPath -Force | Out-Null
New-Item -ItemType Directory -Path $tempPath -Force | Out-Null

try {
    Copy-Item -LiteralPath $handlerPath -Destination (Join-Path $tempPath "handler.py")
    if (Test-Path -LiteralPath $zipPath) {
        Remove-Item -LiteralPath $zipPath -Force
    }

    Compress-Archive -LiteralPath (Join-Path $tempPath "handler.py") -DestinationPath $zipPath -CompressionLevel Optimal

    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $archive = [System.IO.Compression.ZipFile]::OpenRead($zipPath)
    try {
        $entries = @($archive.Entries | ForEach-Object { $_.FullName })
    }
    finally {
        $archive.Dispose()
    }

    if ($entries.Count -ne 1 -or $entries[0] -ne "handler.py") {
        throw "Unexpected Lambda ZIP layout. Expected exactly handler.py at the archive root; found: $($entries -join ', ')"
    }

    $hash = (Get-FileHash -LiteralPath $zipPath -Algorithm SHA256).Hash
    Write-Host "Lambda package created successfully:"
    Write-Host "  ZIP: $zipPath"
    Write-Host "  Contents: $($entries -join ', ')"
    Write-Host "  SHA256: $hash"
    Write-Host "No AWS resources were changed and no files were uploaded."
}
finally {
    if (Test-Path -LiteralPath $tempPath) {
        Remove-Item -LiteralPath $tempPath -Recurse -Force
    }
}
