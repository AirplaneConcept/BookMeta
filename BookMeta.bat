@echo off
title BookMeta
cd /d "%~dp0"

echo.
echo  ========================================
echo    BookMeta
echo  ========================================
echo.
echo  Starting server...

:: Check Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo  ERROR: Python not found. Make sure Python is installed
    echo  and added to your PATH.
    echo.
    pause
    exit /b 1
)

:: Open browser after 2 second delay (runs in background)
start /b "" cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:5001"

:: Run Flask in this window — closing the window stops the server
python app.py

echo.
echo  Server stopped. Press any key to close.
pause >nul
