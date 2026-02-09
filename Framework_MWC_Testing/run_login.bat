@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM =========================
REM CONFIG
REM =========================
set "FEATURE=login"
set "TEST_PATH=tests\test_login_ddt.py"

set "REPORTS_DIR=reports"
set "ALLURE_RESULTS_ROOT=%REPORTS_DIR%\allure-results"
set "ALLURE_REPORT_ROOT=%REPORTS_DIR%\allure-report"

echo ===========================================
echo RUN LOGIN DDT - Select Data Source + Format
echo ===========================================
echo.
echo [1] Manual data (data\manual)
echo [2] AI data (data\ai_generated\processed)
set /p "DATA_SOURCE_CHOICE=Choose data source (1-2): "

if "!DATA_SOURCE_CHOICE!"=="1" (
  set "DATA_SOURCE=manual"
) else if "!DATA_SOURCE_CHOICE!"=="2" (
  set "DATA_SOURCE=ai"
) else (
  echo Invalid choice. Exit.
  exit /b 1
)

echo.
echo [1]  CSV
echo [2]  JSON
echo [3]  XLSX
echo [4]  XLS
echo [5]  YAML
echo [6]  YML
echo [7]  XML
echo [8]  DB (SQLite)
echo [9]  Custom file name (override)
set /p "DATA_MODE_CHOICE=Choose data format (1-9): "

set "DATA_MODE="
set "DATA_FILE="

REM =========================
REM MAP CHOICE -> mode + filename
REM =========================
if "!DATA_MODE_CHOICE!"=="1" (
  set "DATA_MODE=csv"
) else if "!DATA_MODE_CHOICE!"=="2" (
  set "DATA_MODE=json"
) else if "!DATA_MODE_CHOICE!"=="3" (
  set "DATA_MODE=xlsx"
) else if "!DATA_MODE_CHOICE!"=="4" (
  set "DATA_MODE=xls"
) else if "!DATA_MODE_CHOICE!"=="5" (
  set "DATA_MODE=yaml"
) else if "!DATA_MODE_CHOICE!"=="6" (
  set "DATA_MODE=yml"
) else if "!DATA_MODE_CHOICE!"=="7" (
  set "DATA_MODE=xml"
) else if "!DATA_MODE_CHOICE!"=="8" (
  set "DATA_MODE=db"
) else if "!DATA_MODE_CHOICE!"=="9" (
  echo.
  set /p "DATA_MODE=Enter data mode (csv/json/xlsx/xls/yaml/yml/xml/db): "
  set /p "DATA_FILE=Enter file name (e.g. MyLoginData.csv): "
) else (
  echo Invalid choice. Exit.
  exit /b 1
)

REM =========================
REM Default filenames if not override
REM =========================
if "!DATA_FILE!"=="" (
  if /i "!DATA_SOURCE!"=="manual" (
    REM Manual conventions:
    REM  - xlsx/xls: shared file in data\manual\
    REM  - others  : per-feature in data\manual\Login\
    if /i "!DATA_MODE!"=="xlsx" (
      set "DATA_FILE=TestData.xlsx"
    ) else if /i "!DATA_MODE!"=="xls" (
      set "DATA_FILE=TestData.xls"
    ) else if /i "!DATA_MODE!"=="csv" (
      set "DATA_FILE=LoginData.csv"
    ) else if /i "!DATA_MODE!"=="json" (
      set "DATA_FILE=LoginData.json"
    ) else if /i "!DATA_MODE!"=="yaml" (
      set "DATA_FILE=LoginData.yaml"
    ) else if /i "!DATA_MODE!"=="yml" (
      set "DATA_FILE=LoginData.yml"
    ) else if /i "!DATA_MODE!"=="xml" (
      set "DATA_FILE=LoginData.xml"
    ) else if /i "!DATA_MODE!"=="db" (
      set "DATA_FILE=LoginData.db"
    )
  ) else (
    REM AI conventions: processed\<fmt>\login.<ext>
    if /i "!DATA_MODE!"=="db" (
      set "DATA_FILE=login.sqlite"
    ) else (
      set "DATA_FILE=login.!DATA_MODE!"
    )
  )
)

REM =========================
REM Allure per feature+source
REM =========================
set "ALLURE_RESULTS=%ALLURE_RESULTS_ROOT%\%FEATURE%_!DATA_SOURCE!"
set "ALLURE_REPORT=%ALLURE_REPORT_ROOT%\%FEATURE%_!DATA_SOURCE!"

echo -------------------------------------------
echo Selected:
echo   DATA_SOURCE    = !DATA_SOURCE!
echo   DATA_MODE      = !DATA_MODE!
echo   DATA_FILE      = !DATA_FILE!
echo   ALLURE_RESULTS = !ALLURE_RESULTS!
echo   ALLURE_REPORT  = !ALLURE_REPORT!
echo -------------------------------------------

REM =========================
REM Run pytest
REM =========================
pytest -q "%TEST_PATH%" ^
  --data-source=!DATA_SOURCE! ^
  --data-mode=!DATA_MODE! ^
  --data-file=!DATA_FILE! ^
  --alluredir="!ALLURE_RESULTS!"

set "EXITCODE=%ERRORLEVEL%"

REM =========================
REM Generate Allure report
REM =========================
allure generate "!ALLURE_RESULTS!" -o "!ALLURE_REPORT!" --clean

echo Pytest exit code = !EXITCODE!
echo Report successfully generated to !ALLURE_REPORT!
exit /b !EXITCODE!
