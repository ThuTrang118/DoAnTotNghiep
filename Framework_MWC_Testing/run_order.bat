@echo off
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
title CHẠY TEST: ĐẶT HÀNG (ORDER)
set /p MODE="Nhập loại dữ liệu (excel / csv / json): "
if "%MODE%"=="" set MODE=excel
if /i "%MODE%"=="excel" (
    set "FILE=data\TestData.xlsx"
) else if /i "%MODE%"=="csv" (
    set "FILE=data\OrderData.csv"
) else if /i "%MODE%"=="json" (
    set "FILE=data\OrderData.json"
) else (
    echo Loại dữ liệu không hợp lệ.
    pause
    exit /b
)
REM =========================================================
REM 1) DỌN RÁC ALLURE-RESULTS NẾU TRƯỚC ĐÓ CHẠY SAI CÚ PHÁP
REM    (sinh ra folder -s / -p)
REM =========================================================
if exist "reports\allure-results\-s" (
    rmdir /s /q "reports\allure-results\-s"
)
if exist "reports\allure-results\-p" (
    rmdir /s /q "reports\allure-results\-p"
)

REM =========================================================
REM 2) CHUẨN BỊ THƯ MỤC RESULTS/REPORT CHO ORDER
REM =========================================================
if not exist "reports\allure-results" mkdir "reports\allure-results"
if not exist "reports\allure-report"  mkdir "reports\allure-report"

REM Xóa results order cũ để tránh trộn dữ liệu nhiều lần chạy
if exist "reports\allure-results\order" (
    rmdir /s /q "reports\allure-results\order"
)
mkdir "reports\allure-results\order" >nul 2>&1

REM =========================================================
REM 3) CHẠY PYTEST (KHÔNG -s, KHÔNG -p)
REM    LUÔN dùng --alluredir=... (có dấu '=')
REM =========================================================
pytest -v tests\test_order_ddt.py --data-mode=%MODE% --data-file=%FILE% --alluredir=reports/allure-results/order

REM =========================================================
REM 4) COPY HISTORY TỪ REPORT CŨ SANG RESULTS MỚI (QUAN TRỌNG)
REM    Để Allure hiển thị tab History/Trend
REM =========================================================
if exist "reports\allure-report\order\history" (
    if exist "reports\allure-results\order\history" (
        rmdir /s /q "reports\allure-results\order\history"
    )
    xcopy /E /I /Y "reports\allure-report\order\history" "reports\allure-results\order\history" >nul
)

REM =========================================================
REM 5) GENERATE REPORT (có --clean vẫn OK vì history nằm ở results)
REM =========================================================
allure generate reports/allure-results/order -o reports/allure-report/order --clean

start "" reports\allure-report\order\index.html
pause
