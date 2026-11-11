@echo off
setlocal
set "APP_PY=%~dp0app.py"
where python >nul 2>nul
if %ERRORLEVEL%==0 (
  python -B "%APP_PY%" %*
  exit /b %ERRORLEVEL%
)
where py >nul 2>nul
if %ERRORLEVEL%==0 (
  py -3 -B "%APP_PY%" %*
  exit /b %ERRORLEVEL%
)
echo Error: Python 3 is not installed or not found in PATH.>&2
exit /b 1
