@echo off
setlocal EnableDelayedExpansion
chcp 65001 >nul

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
set /p CHOICE=Chon chuc nang (0-6):

if "%CHOICE%"=="0" exit /b
if "%CHOICE%"=="1" (
    set "FEATURE=login"
    set "TEST_FILE=tests/test_login_ddt.py"
    set "BASE_NAME=LoginData"
    set "MANUAL_FOLDER=Login"
    set "AI_FOLDER=login"
)
if "%CHOICE%"=="2" (
    set "FEATURE=register"
    set "TEST_FILE=tests/test_register_ddt.py"
    set "BASE_NAME=RegisterData"
    set "MANUAL_FOLDER=Register"
    set "AI_FOLDER=register"
    set "DB_TABLE=register"
)
if "%CHOICE%"=="3" (
    set "FEATURE=search"
    set "TEST_FILE=tests/test_search_ddt.py"
    set "BASE_NAME=SearchData"
    set "MANUAL_FOLDER=Search"
    set "AI_FOLDER=search"
    set "DB_TABLE=testdata"
)
if "%CHOICE%"=="4" (
    set "FEATURE=order"
    set "TEST_FILE=tests/test_order_ddt.py"
    set "BASE_NAME=OrderData"
    set "MANUAL_FOLDER=Order"
    set "AI_FOLDER=order"
    set "DB_TABLE=testdata"
)
if "%CHOICE%"=="5" (
    set "FEATURE=profile_update"
    set "TEST_FILE=tests/test_profile_update_ddt.py"
    set "BASE_NAME=ProfileData"
    set "MANUAL_FOLDER=Profile"
    set "AI_FOLDER=profile"
    set "DB_TABLE=testdata"
)
if "%CHOICE%"=="6" (
    set "FEATURE=product_review"
    set "TEST_FILE=tests/test_product_review_ddt.py"
    set "BASE_NAME=ProductReviewData"
    set "MANUAL_FOLDER=ProductReview"
    set "AI_FOLDER=productreview"
    set "DB_TABLE=testdata"
)

if not defined FEATURE (
    echo.
    echo Lua chon khong hop le!
    pause
    goto MENU
)

echo.
echo [1] Manual data
echo [2] AI data
set /p SOURCE_CHOICE=Chon nguon du lieu (1-2):

if "%SOURCE_CHOICE%"=="1" (
    set "DATA_SOURCE=manual"
) else if "%SOURCE_CHOICE%"=="2" (
    set "DATA_SOURCE=ai"
) else (
    echo.
    echo Lua chon nguon du lieu khong hop le!
    pause
    goto MENU
)

echo.
echo [1] CSV
echo [2] JSON
echo [3] XLSX
echo [4] XLS
echo [5] YAML
echo [6] YML
echo [7] XML
echo [8] DB
set /p MODE_CHOICE=Chon dinh dang du lieu (1-8):

if "%MODE_CHOICE%"=="1" set "DATA_MODE=csv"
if "%MODE_CHOICE%"=="2" set "DATA_MODE=json"
if "%MODE_CHOICE%"=="3" set "DATA_MODE=xlsx"
if "%MODE_CHOICE%"=="4" set "DATA_MODE=xls"
if "%MODE_CHOICE%"=="5" set "DATA_MODE=yaml"
if "%MODE_CHOICE%"=="6" set "DATA_MODE=yml"
if "%MODE_CHOICE%"=="7" set "DATA_MODE=xml"
if "%MODE_CHOICE%"=="8" set "DATA_MODE=db"

if not defined DATA_MODE (
    echo.
    echo Lua chon dinh dang khong hop le!
    pause
    goto MENU
)

REM ===========================================
REM BUILD DATA FILE PATH
REM ===========================================

if /I "%DATA_SOURCE%"=="manual" (
    if /I "%DATA_MODE%"=="xlsx" (
        set "DATA_FILE=data\manual\TestData.xlsx"
    ) else if /I "%DATA_MODE%"=="xls" (
        set "DATA_FILE=data\manual\TestData.xls"
    ) else (
        set "DATA_FILE=data\manual\!MANUAL_FOLDER!\!BASE_NAME!.!DATA_MODE!"
    )
) else (
    set "DATA_FILE=data\ai_processed\!AI_FOLDER!\!BASE_NAME!.!DATA_MODE!"
)

REM DB extension
if /I "%DATA_MODE%"=="db" (
    if /I "%DATA_SOURCE%"=="manual" (
        set "DATA_FILE=data\manual\!MANUAL_FOLDER!\!BASE_NAME!.db"
    ) else (
        set "DATA_FILE=data\ai_processed\!AI_FOLDER!\!BASE_NAME!.db"
    )
)

REM ===========================================
REM BUILD DB TABLE NAME
REM Manual DB dùng bảng testdata
REM AI DB dùng bảng theo feature/folder AI
REM ===========================================

if /I "%DATA_MODE%"=="db" (
    if /I "%DATA_SOURCE%"=="manual" (
        set "DB_TABLE=testdata"
    ) else (
        set "DB_TABLE=!AI_FOLDER!"
    )
) else (
    set "DB_TABLE="
)

set "ALLURE_RESULTS=reports\allure-results\%FEATURE%_%DATA_SOURCE%"
set "ALLURE_REPORT=reports\allure-report\%FEATURE%_%DATA_SOURCE%"
set "BUILD_ORDER_DIR=reports\allure-history"
set "BUILD_ORDER_FILE=%BUILD_ORDER_DIR%\%FEATURE%_%DATA_SOURCE%_build_order.txt"

if not exist "%BUILD_ORDER_DIR%" mkdir "%BUILD_ORDER_DIR%"

if exist "%BUILD_ORDER_FILE%" (
    set /p BUILD_ORDER=<"%BUILD_ORDER_FILE%"
    set /a BUILD_ORDER+=1
) else (
    set "BUILD_ORDER=1"
)


echo.
echo ===========================================
echo FEATURE        = %FEATURE%
echo DATA_SOURCE    = %DATA_SOURCE%
echo DATA_MODE      = %DATA_MODE%
echo DATA_FILE      = %DATA_FILE%
echo DB_TABLE       = %DB_TABLE%
echo ===========================================
echo.

set "HISTORY_BACKUP=%BUILD_ORDER_DIR%\%FEATURE%_%DATA_SOURCE%_history"

if exist "%HISTORY_BACKUP%" rmdir /s /q "%HISTORY_BACKUP%"

if exist "%ALLURE_REPORT%\history" (
    xcopy /E /I /Y "%ALLURE_REPORT%\history" "%HISTORY_BACKUP%" >nul
)

if exist "%ALLURE_RESULTS%" rmdir /s /q "%ALLURE_RESULTS%"
mkdir "%ALLURE_RESULTS%"

if exist "%HISTORY_BACKUP%" (
    xcopy /E /I /Y "%HISTORY_BACKUP%" "%ALLURE_RESULTS%\history" >nul
)

pytest -v %TEST_FILE% ^
  --data-source=%DATA_SOURCE% ^
  --data-mode=%DATA_MODE% ^
  --data-file="%DATA_FILE%" ^
  --db-table=%DB_TABLE% ^
  --alluredir="%ALLURE_RESULTS%"

(
echo {
echo   "name": "Local Pytest",
echo   "type": "local",
echo   "buildName": "%FEATURE%_%DATA_SOURCE% Run #%BUILD_ORDER%",
echo   "buildOrder": %BUILD_ORDER%,
echo   "reportName": "%FEATURE%_%DATA_SOURCE% Report"
echo }
) > "%ALLURE_RESULTS%\executor.json"

echo %BUILD_ORDER%>"%BUILD_ORDER_FILE%"

allure generate "%ALLURE_RESULTS%" -o "%ALLURE_REPORT%" --clean

echo.
echo Report successfully generated to %ALLURE_REPORT%
echo.

pause
goto MENU