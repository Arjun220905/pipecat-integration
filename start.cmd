@echo off
setlocal
cd /d "%~dp0"
where py >nul 2>nul
if errorlevel 1 goto python
py -3 scripts\manage.py start %*
exit /b %errorlevel%
:python
python scripts\manage.py start %*
exit /b %errorlevel%
