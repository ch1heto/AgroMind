param(
    [string]$PythonCommand = "python",
    [string]$VenvPath = "venv"
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$requirementsPath = Join-Path $projectRoot "requirements.txt"
$venvFullPath = Join-Path $projectRoot $VenvPath

$pythonExecutable = $null
$pythonCommandInfo = Get-Command $PythonCommand -ErrorAction SilentlyContinue

if ($pythonCommandInfo) {
    $pythonExecutable = $pythonCommandInfo.Source
} else {
    $fallbackPython = Get-ChildItem `
        -Path (Join-Path $env:LocalAppData "Programs\\Python") `
        -Filter "python.exe" `
        -Recurse `
        -ErrorAction SilentlyContinue |
        Sort-Object FullName -Descending |
        Select-Object -First 1

    if ($fallbackPython) {
        $pythonExecutable = $fallbackPython.FullName
        Write-Host "Python was not found in PATH. Using fallback interpreter: $pythonExecutable"
    } else {
        throw "Python command '$PythonCommand' not found. Install Python 3.11+ and rerun this script."
    }
}

if (-not (Test-Path $venvFullPath)) {
    Write-Host "Creating virtual environment at $venvFullPath"
    & $pythonExecutable -m venv $venvFullPath
}

$venvPython = Join-Path $venvFullPath "Scripts\\python.exe"

if (-not (Test-Path $venvPython)) {
    throw "Virtual environment was not created successfully. Expected interpreter: $venvPython"
}

Write-Host "Installing dependencies from $requirementsPath"
& $venvPython -m pip install --upgrade pip setuptools wheel
& $venvPython -m pip install --upgrade -r $requirementsPath

Write-Host "Verifying required packages"
& $venvPython -c "import influxdb_client, plotly, apscheduler, streamlit_autorefresh"

Write-Host ""
Write-Host "Environment is ready."
Write-Host "Activate it with:"
Write-Host ".\\venv\\Scripts\\Activate.ps1"
