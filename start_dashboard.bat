@echo off
setlocal
cd /d %~dp0

:: Activate virtual environment if present
if exist "venv\Scripts\activate.bat" (
  call "venv\Scripts\activate.bat"
) else (
  echo [INFO] Virtual environment not found. Proceeding with system Python.
)

:: Upgrade pip quietly
python -m pip install --upgrade pip >nul 2>&1

:: Ensure Streamlit is available and install requirements if present
if exist requirements.txt (
  echo [INFO] Installing/validating Python dependencies from requirements.txt ...
  python -m pip install -r requirements.txt
) else (
  echo [WARN] requirements.txt not found.
)

:: Ensure streamlit is installed
python -c "import streamlit" 2>nul
if errorlevel 1 (
  echo [INFO] Installing Streamlit ...
  python -m pip install streamlit
)

:: Launch the dashboard
streamlit run app.py
endlocal
