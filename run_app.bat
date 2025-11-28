@echo off
echo ================================================
echo Starting Movie Database Application
echo ================================================
echo.

cd /d "%~dp0"
echo Current directory: %CD%
echo.

echo Checking virtual environment...
if not exist "venv314\Scripts\python.exe" (
    echo ERROR: Virtual environment not found!
    echo Please ensure venv314 exists.
    pause
    exit /b 1
)

echo Using Python from: venv314\Scripts\python.exe
echo.

echo Starting Flask application...
echo.
echo Application will be available at: http://localhost:5000
echo.
echo To switch to MongoDB:
echo   1. Open http://localhost:5000 in your browser
echo   2. Click the database dropdown (top-right corner)
echo   3. Select MongoDB
echo.
echo Press Ctrl+C to stop the server
echo ================================================
echo.

venv314\Scripts\python.exe app.py

pause
