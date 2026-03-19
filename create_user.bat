@echo off
setlocal

REM Always run from project root
cd /d %~dp0
set "PYTHONPATH=%CD%\src"

python scripts\create_user.py
endlocal
