@echo off
setlocal

REM Always run from project root
cd /d %~dp0

REM Provide a dev default SECRET_KEY if not already set
if "%SECRET_KEY%"=="" (
  set "SECRET_KEY=dev-secret-change-me"
)

uvicorn sar.app:app --reload
endlocal
