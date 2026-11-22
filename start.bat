@echo off
title Flink Academy
cd /d "%~dp0"
where py >nul 2>nul
if %errorlevel%==0 (
  py -3 main.py
  goto :end
)
where python >nul 2>nul
if %errorlevel%==0 (
  python main.py
  goto :end
)
echo Python was not found. Opening the offline version instead (progress saved in the browser only).
start "" "%~dp0web\index.html"
:end
pause
