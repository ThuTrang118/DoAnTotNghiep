@echo off
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
title CHẠY TEST: ĐÁNH GIÁ SẢN PHẨM (PRODUCT REVIEW)
set /p MODE="Nhập loại dữ liệu (excel / csv / json): "
if "%MODE%"=="" set MODE=excel

if /i "%MODE%"=="excel" (
    set "FILE=data\TestData.xlsx"
) else if /i "%MODE%"=="csv" (
    set "FILE=data\ProductReviewData.csv"
) else if /i "%MODE%"=="json" (
    set "FILE=data\ProductReviewData.json"
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
REM 2) CHUẨN BỊ THƯ MỤC RESULTS/REPORT CHO PRODUCT _EVIEW
REM =========================================================
if not exist "reports\allure-results" mkdir "reports\allure-results"
if not exist "reports\allure-report"  mkdir "reports\allure-report"

REM Xóa results product_review cũ để tránh trộn dữ liệu nhiều lần chạy
if exist "reports\allure-results\product_review" (
    rmdir /s /q "reports\allure-results\product_review"
)
mkdir "reports\allure-results\product_review" >nul 2>&1

REM =========================================================
REM 3) CHẠY PYTEST (KHÔNG -s, KHÔNG -p)
REM    LUÔN dùng --alluredir=... (có dấu '=')
REM =========================================================
pytest -v tests\test_product_review_ddt.py --data-mode=%MODE% --data-file=%FILE% --alluredir=reports/allure-results/product_review

REM =========================================================
REM 4) COPY HISTORY TỪ REPORT CŨ SANG RESULTS MỚI (QUAN TRỌNG)
REM    Để Allure hiển thị tab History/Trend
REM =========================================================
if exist "reports\allure-report\product_review\history" (
    if exist "reports\allure-results\product_review\history" (
        rmdir /s /q "reports\allure-results\product_review\history"
    )
    xcopy /E /I /Y "reports\allure-report\product_review\history" "reports\allure-results\product_review\history" >nul
)

REM =========================================================
REM 5) GENERATE REPORT (có --clean vẫn OK vì history nằm ở results)
REM =========================================================
allure generate reports/allure-results/product_review -o reports/allure-report/product_review --clean

start "" reports\allure-report\product_review\index.html
pause
