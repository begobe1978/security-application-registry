@echo off
setlocal

REM Always run from project root
cd /d %~dp0

python scripts\create_user.py
endlocal
