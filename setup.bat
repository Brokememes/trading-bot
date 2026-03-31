@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

set "START_AFTER_SETUP=0"
set "DASHBOARD_HOST=127.0.0.1"
set "DASHBOARD_PORT=8765"

if /I "%~1"=="--start" (
    set "START_AFTER_SETUP=1"
)
if /I "%~1"=="--public-dashboard" (
    set "START_AFTER_SETUP=1"
    set "DASHBOARD_HOST=0.0.0.0"
)

call :resolve_python
if errorlevel 1 exit /b 1

if not exist ".venv\Scripts\python.exe" (
    echo [setup] Creating virtual environment...
    %PYTHON_CMD% -m venv .venv
    if errorlevel 1 goto :fail
)

call ".venv\Scripts\activate.bat"
if errorlevel 1 goto :fail

echo [setup] Upgrading packaging tools...
python -m pip install --upgrade pip setuptools wheel
if errorlevel 1 goto :fail

echo [setup] Installing fx-edge-lab...
python -m pip install -e .
if errorlevel 1 goto :fail

if not exist "data" mkdir "data"
if not exist "logs" mkdir "logs"

if not exist "crypto.local.json" (
    copy /Y "crypto.example.json" "crypto.local.json" >nul
    echo [setup] Created crypto.local.json from crypto.example.json
)

if not exist "config.local.json" (
    copy /Y "config.example.json" "config.local.json" >nul
    echo [setup] Created config.local.json from config.example.json
)

if /I "%DASHBOARD_HOST%"=="0.0.0.0" (
    echo [setup] Opening Windows firewall for TCP %DASHBOARD_PORT%...
    netsh advfirewall firewall add rule name="FX Edge Dashboard %DASHBOARD_PORT%" dir=in action=allow protocol=TCP localport=%DASHBOARD_PORT% >nul 2>nul
)

echo.
echo [setup] Done.
echo [setup] Repo root: %CD%
echo [setup] Config: %CD%\crypto.local.json
echo [setup] Start capture:
echo           %CD%\.venv\Scripts\python.exe %CD%\run_fx_edge_lab.py crypto-capture --config %CD%\crypto.local.json
echo [setup] Start dashboard:
echo           %CD%\.venv\Scripts\python.exe %CD%\run_fx_edge_lab.py crypto-dashboard --config %CD%\crypto.local.json --host %DASHBOARD_HOST% --port %DASHBOARD_PORT%
echo [setup] Dashboard URL: http://127.0.0.1:%DASHBOARD_PORT%
if /I "%DASHBOARD_HOST%"=="0.0.0.0" (
    echo [setup] Public dashboard URL: http://YOUR_SERVER_IP:%DASHBOARD_PORT%
)

if "%START_AFTER_SETUP%"=="1" (
    echo [setup] Launching collector and dashboard...
    start "FX Edge Capture" cmd /k "\"%CD%\.venv\Scripts\python.exe\" \"%CD%\run_fx_edge_lab.py\" crypto-capture --config \"%CD%\crypto.local.json\""
    start "FX Edge Dashboard" cmd /k "\"%CD%\.venv\Scripts\python.exe\" \"%CD%\run_fx_edge_lab.py\" crypto-dashboard --config \"%CD%\crypto.local.json\" --host %DASHBOARD_HOST% --port %DASHBOARD_PORT%"
)

exit /b 0

:resolve_python
set "PYTHON_CMD="
where py >nul 2>nul
if not errorlevel 1 (
    set "PYTHON_CMD=py -3.13"
    py -3.13 -V >nul 2>nul
    if not errorlevel 1 goto :python_ready
)

where python >nul 2>nul
if not errorlevel 1 (
    set "PYTHON_CMD=python"
    python -c "import sys; raise SystemExit(0 if sys.version_info[:2] >= (3, 13) else 1)" >nul 2>nul
    if not errorlevel 1 goto :python_ready
)

echo [setup] Python 3.13 was not found. Attempting install...
where winget >nul 2>nul
if not errorlevel 1 (
    winget install --exact --id Python.Python.3.13 --accept-package-agreements --accept-source-agreements
    if errorlevel 1 goto :python_install_failed
) else (
    powershell -NoProfile -ExecutionPolicy Bypass -Command "$ProgressPreference='SilentlyContinue'; $url='https://www.python.org/ftp/python/3.13.3/python-3.13.3-amd64.exe'; $out=Join-Path $env:TEMP 'python-3.13.3-amd64.exe'; Invoke-WebRequest -Uri $url -OutFile $out; Start-Process -FilePath $out -ArgumentList '/quiet InstallAllUsers=0 PrependPath=1 Include_launcher=1 Include_pip=1' -Wait"
    if errorlevel 1 goto :python_install_failed
)

set "PATH=%LocalAppData%\Programs\Python\Python313;%LocalAppData%\Programs\Python\Python313\Scripts;%PATH%"
where py >nul 2>nul
if not errorlevel 1 (
    set "PYTHON_CMD=py -3.13"
    py -3.13 -V >nul 2>nul
    if not errorlevel 1 goto :python_ready
)
where python >nul 2>nul
if not errorlevel 1 (
    set "PYTHON_CMD=python"
    python -c "import sys; raise SystemExit(0 if sys.version_info[:2] >= (3, 13) else 1)" >nul 2>nul
    if not errorlevel 1 goto :python_ready
)
goto :python_install_failed

:python_ready
for /f "delims=" %%I in ('%PYTHON_CMD% -c "import sys; print(sys.executable)"') do set "PYTHON_EXE=%%I"
echo [setup] Using Python: !PYTHON_EXE!
exit /b 0

:python_install_failed
echo [setup] Failed to install or find Python 3.13.
echo [setup] Install Python 3.13 manually, then rerun setup.bat.
exit /b 1

:fail
echo [setup] Setup failed.
exit /b 1
