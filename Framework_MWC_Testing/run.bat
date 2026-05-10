@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion

set REPORTS_DIR=reports
set ALLURE_RESULTS=%REPORTS_DIR%\allure-results
set ALLURE_REPORT=%REPORTS_DIR%\allure-report

if not exist "%ALLURE_RESULTS%" mkdir "%ALLURE_RESULTS%"
if not exist "%ALLURE_REPORT%" mkdir "%ALLURE_REPORT%"

:MENU
cls
echo ===========================================
echo           FRAMEWORK TEST RUNNER
echo ===========================================
echo.
echo [1] Login
echo [2] Register
echo [3] Search
echo [4] Order
echo [5] Profile Update
echo [6] Product Review
echo [0] Exit
echo.

set /p FEATURE_CHOICE=Chon chuc nang (0-6):

if "%FEATURE_CHOICE%"=="1" set FEATURE=login
if "%FEATURE_CHOICE%"=="2" set FEATURE=register
if "%FEATURE_CHOICE%"=="3" set FEATURE=search
if "%FEATURE_CHOICE%"=="4" set FEATURE=order
if "%FEATURE_CHOICE%"=="5" set FEATURE=profile_update
if "%FEATURE_CHOICE%"=="6" set FEATURE=product_review
if "%FEATURE_CHOICE%"=="0" goto END

if not defined FEATURE (
    echo Lua chon khong hop le.
    pause
    goto MENU
)

REM ============================
REM DATA SOURCE
REM ============================
echo.
echo [1] Manual data
echo [2] AI data
set /p DATA_SOURCE_CHOICE=Chon nguon du lieu (1-2):

if "%DATA_SOURCE_CHOICE%"=="1" set DATA_SOURCE=manual
if "%DATA_SOURCE_CHOICE%"=="2" set DATA_SOURCE=ai

if not defined DATA_SOURCE (
    echo Lua chon khong hop le.
    pause
    goto MENU
)

REM ============================
REM DATA FORMAT
REM ============================
echo.
echo [1] CSV
echo [2] JSON
echo [3] XLSX
echo [4] XLS
echo [5] YAML
echo [6] YML
echo [7] XML
echo [8] DB
set /p FORMAT_CHOICE=Chon dinh dang du lieu (1-8):

if "%FORMAT_CHOICE%"=="1" set DATA_MODE=csv
if "%FORMAT_CHOICE%"=="2" set DATA_MODE=json
if "%FORMAT_CHOICE%"=="3" set DATA_MODE=xlsx
if "%FORMAT_CHOICE%"=="4" set DATA_MODE=xls
if "%FORMAT_CHOICE%"=="5" set DATA_MODE=yaml
if "%FORMAT_CHOICE%"=="6" set DATA_MODE=yml
if "%FORMAT_CHOICE%"=="7" set DATA_MODE=xml
if "%FORMAT_CHOICE%"=="8" set DATA_MODE=db

if not defined DATA_MODE (
    echo Lua chon khong hop le.
    pause
    goto MENU
)

REM ============================
REM BUILD DATA FILE NAME
REM ============================

REM Helper: Capitalize first letter of FEATURE (login -> Login)
set FEATURE_CAP=%FEATURE:~0,1%%FEATURE:~1%
REM Uppercase first letter (Windows batch doesn't have built-in upper; but your folder uses LoginData not loginData)
REM So we hard-map the basename for AI by feature

REM AI/Manual basename mapping
set BASE_NAME=

if /I "%FEATURE%"=="login" set BASE_NAME=LoginData
if /I "%FEATURE%"=="register" set BASE_NAME=RegisterData
if /I "%FEATURE%"=="search" set BASE_NAME=SearchData
if /I "%FEATURE%"=="order" set BASE_NAME=OrderData
if /I "%FEATURE%"=="profile_update" set BASE_NAME=ProfileUpdateData
if /I "%FEATURE%"=="product_review" set BASE_NAME=ProductReviewData

REM Fallback if not mapped
if not defined BASE_NAME set BASE_NAME=%FEATURE_CAP%Data

if "%DATA_SOURCE%"=="manual" (
    REM Manual naming convention
    if "%DATA_MODE%"=="xlsx" (
        set DATA_FILE=data\manual\TestData.xlsx
    ) else if "%DATA_MODE%"=="xls" (
        set DATA_FILE=data\manual\TestData.xls
    ) else (
        REM Example: data\manual\LoginData.csv or data\manual\RegisterData.json (if you store like that)
        REM If your manual files are actually in data\manual\<Feature>\..., keep your old line.
        set DATA_FILE=data\manual\%FEATURE_CAP%\%BASE_NAME%.%DATA_MODE%
        REM If your current structure is data\manual\<FeatureFolder>\LoginData.csv then line above is correct.
        REM If you store manual flat directly under data\manual\, use:
        REM set DATA_FILE=data\manual\%BASE_NAME%.%DATA_MODE%
    )
) else (
    REM AI naming convention (FLAT): data\ai_processed\<BaseName>.<ext>
    if /I "%DATA_MODE%"=="db" (
        set DATA_FILE=data\ai_processed\%BASE_NAME%.db
    ) else (
        set DATA_FILE=data\ai_processed\%BASE_NAME%.%DATA_MODE%
    )
)

REM ============================
REM ALLURE
REM ============================

set FEATURE_KEY=%FEATURE%_%DATA_SOURCE%
set FEATURE_RESULTS=%ALLURE_RESULTS%\%FEATURE_KEY%
set FEATURE_REPORT=%ALLURE_REPORT%\%FEATURE_KEY%

if exist "%FEATURE_RESULTS%" rmdir /s /q "%FEATURE_RESULTS%"
mkdir "%FEATURE_RESULTS%"

echo.
echo ===========================================
echo FEATURE        = %FEATURE%
echo DATA_SOURCE    = %DATA_SOURCE%
echo DATA_MODE      = %DATA_MODE%
echo DATA_FILE      = %DATA_FILE%
echo ===========================================
echo.

REM ============================
REM RUN PYTEST
REM ============================

pytest -v tests\test_%FEATURE%_ddt.py ^
  --data-source=%DATA_SOURCE% ^
  --data-mode=%DATA_MODE% ^
  --data-file=%DATA_FILE% ^
  --alluredir=%FEATURE_RESULTS%

REM ============================
REM GENERATE REPORT
REM ============================

allure generate %FEATURE_RESULTS% -o %FEATURE_REPORT% --clean
start "" %FEATURE_REPORT%\index.html

pause
goto MENU

:END
exit