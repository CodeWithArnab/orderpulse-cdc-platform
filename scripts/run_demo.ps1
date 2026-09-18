param(
    [int]$Port = 8000,
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonCommand = Get-Command python -ErrorAction SilentlyContinue
$codexPython = Join-Path $env:USERPROFILE ".cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"

if ($pythonCommand) {
    $pythonExe = $pythonCommand.Source
}
elseif (Test-Path -LiteralPath $codexPython) {
    $pythonExe = $codexPython
    Write-Host "Using the Python runtime bundled with Codex." -ForegroundColor DarkGray
}
else {
    throw "Python 3.11+ was not found. Install Python, reopen PowerShell, and run this script again."
}

$env:PYTHONPATH = Join-Path $projectRoot "src"

Push-Location $projectRoot
try {
    Write-Host "[1/3] Running correctness tests..." -ForegroundColor Cyan
    & $pythonExe -m unittest discover -s tests -v
    if ($LASTEXITCODE -ne 0) { throw "Tests failed." }

    Write-Host "[2/3] Generating CDC demo data..." -ForegroundColor Cyan
    & $pythonExe -m orderpulse.demo --output build/demo
    if ($LASTEXITCODE -ne 0) { throw "Demo generation failed." }

    $dashboardUrl = "http://localhost:$Port/dashboard/"
    Write-Host "[3/3] Serving the visual dashboard at $dashboardUrl" -ForegroundColor Green
    Write-Host "Press Ctrl+C to stop the server." -ForegroundColor DarkGray

    if (-not $NoBrowser) {
        Start-Process $dashboardUrl
    }

    & $pythonExe -m http.server $Port
}
finally {
    Pop-Location
}
