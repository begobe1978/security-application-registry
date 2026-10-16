@echo off
setlocal

REM Always run from project root
cd /d "%~dp0"

REM Generate a random session secret if none is configured
if not defined SECRET_KEY if not defined SAR_SECRET_KEY (
    for /f "delims=" %%K in ('python -c "import secrets; print(secrets.token_urlsafe(48))"') do set "SECRET_KEY=%%K"
)

REM Never start the application without a session secret
if not defined SECRET_KEY if not defined SAR_SECRET_KEY (
    echo ERROR: Unable to configure a session secret.
    exit /b 1
)

uvicorn sar.app:app --reload --app-dir src

endlocal