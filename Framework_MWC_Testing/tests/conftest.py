from html import parser
import os
import io
import sys
import json
import shutil
from httpx import request
import pytest
import allure
import logging
import platform
import datetime
import subprocess
import webbrowser

from utils.allure_env import write_allure_environment
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from utils.data_io import ResultBook, ensure_dir

# =========================================================
# PATH CONFIG
# =========================================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

SS_DIR = os.path.join(REPORTS_DIR, "screenshots")
RES_DIR = os.path.join(REPORTS_DIR, "results")
ALLURE_RESULTS_ROOT = os.path.join(REPORTS_DIR, "allure-results")
ALLURE_REPORT_ROOT  = os.path.join(REPORTS_DIR, "allure-report")

for d in [SS_DIR, RES_DIR, ALLURE_RESULTS_ROOT, ALLURE_REPORT_ROOT]:
    ensure_dir(d)

# =========================================================
# PYTEST OPTIONS (CHỈ Ở CONFTST)
# =========================================================

def pytest_addoption(parser):
    parser.addoption("--data-mode", action="store", default="excel",
                     help="excel | xlsx | xls | csv | json | yaml | yml | xml | db | sqlite")

    parser.addoption("--data-file", action="store", default="",
                     help="Tên file dữ liệu (VD: LoginData.csv)")

    parser.addoption("--data-source", action="store", default="manual",
                     help="manual | ai")

    # NEW: DB table + XML item tag
    parser.addoption("--db-table", action="store", default="testdata",
                     help="Tên table trong SQLite DB (default: testdata)")

    parser.addoption("--xml-item-tag", action="store", default="item",
                     help="Tên tag item trong XML (default: item)")

# =========================================================
# RESULT WRITER (EXCEL)
# =========================================================
@pytest.fixture(scope="session")
def result_writer(request):
    writer = ResultBook(out_dir=RES_DIR, file_name="ResultsData.xlsx")

    def finalize():
        path = writer.save()
        print(f"\n[RESULT FILE SAVED]: {path}")

    request.addfinalizer(finalize)
    return writer


# =========================================================
# WEBDRIVER FIXTURE
# =========================================================
@pytest.fixture
def driver():
    opts = Options()
    opts.add_argument("--lang=vi")
    opts.add_experimental_option("prefs", {"intl.accept_languages": "vi,vi_VN"})
    opts.add_argument("--start-maximized")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--ignore-ssl-errors=yes")

    # Enable browser console log (để attach khi FAIL)
    opts.set_capability("goog:loggingPrefs", {"browser": "ALL"})

    drv = webdriver.Chrome(options=opts)
    yield drv
    drv.quit()


# =========================================================
# ALLURE – CAPTURE LOGGER OUTPUT (module-level logger)
# =========================================================
@pytest.fixture(autouse=True)
def _capture_logs_for_allure(request):
    """
    Capture logger của từng module test (nếu có biến `logger`).
    Attach log được thực hiện trong pytest_runtest_makereport.
    """
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))

    module = getattr(request.node, "module", None)
    test_logger = getattr(module, "logger", None) if module else None
    target_logger = test_logger if isinstance(test_logger, logging.Logger) else logging.getLogger()

    target_logger.addHandler(handler)

    request.node._allure_log_buffer = buffer
    request.node._allure_log_handler = handler
    request.node._allure_log_target_logger = target_logger

    yield

    try:
        target_logger.removeHandler(handler)
    except Exception:
        pass
    try:
        handler.close()
    except Exception:
        pass


# =========================================================
# HELPERS: ATTACH
# =========================================================
def _attach_text(name: str, text: str):
    try:
        if text is None:
            return
        text = str(text)
        if text.strip():
            allure.attach(text, name=name, attachment_type=allure.attachment_type.TEXT)
    except Exception as e:
        print(f"[WARN] Cannot attach text '{name}': {e}")

def _attach_json(name: str, obj):
    try:
        allure.attach(
            json.dumps(obj, ensure_ascii=False, indent=2),
            name=name,
            attachment_type=allure.attachment_type.JSON
        )
    except Exception as e:
        print(f"[WARN] Cannot attach json '{name}': {e}")

def _get_ddt_params(item):
    """Lấy parameters từ DDT (pytest_generate_tests) qua item.callspec.params."""
    callspec = getattr(item, "callspec", None)
    if callspec and hasattr(callspec, "params"):
        return dict(callspec.params)
    return {}


# =========================================================
# ALLURE HOOK – SCREENSHOT / LOG / PARAMS / PAGE SOURCE
# =========================================================
@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()

    if rep.when != "call":
        return

    # ---- Attach DDT parameters ----
    params = _get_ddt_params(item)
    if params:
        _attach_json(f"{item.name}-parameters", params)

    # ---- Attach data-mode / data-file ----
    try:
        cfg = item.config
        dm = cfg.getoption("--data-mode")
        df = cfg.getoption("--data-file")
        _attach_text(f"{item.name}-data-source", f"data-mode={dm}\ndata-file={df}")
    except Exception:
        pass

    # ---- Attach logger output ----
    buf = getattr(item, "_allure_log_buffer", None)
    if buf:
        _attach_text(f"{item.name}-log", buf.getvalue())

    # ---- Fail artifacts: screenshot + url + source + console ----
    drv = item.funcargs.get("driver", None)
    if not drv:
        return

    safe_name = item.name.replace("[", "_").replace("]", "_")
    img_path = os.path.join(SS_DIR, f"{safe_name}.png")

    if rep.failed:
        # Screenshot
        try:
            if os.path.exists(img_path):
                os.remove(img_path)
            drv.save_screenshot(img_path)
            with open(img_path, "rb") as f:
                allure.attach(f.read(), name="screenshot", attachment_type=allure.attachment_type.PNG)
        except Exception as e:
            print(f"[WARN] Screenshot failed: {e}")

        # URL
        try:
            _attach_text("current-url", drv.current_url)
        except Exception:
            pass

        # Page source
        try:
            _attach_text("page-source", drv.page_source)
        except Exception as e:
            print(f"[WARN] Page source attach failed: {e}")

        # Browser console logs
        try:
            logs = drv.get_log("browser")
            _attach_json("browser-console-log", logs)
        except Exception as e:
            _attach_text("browser-console-log", f"Cannot read console log: {e}")

    elif rep.passed:
        # Cleanup old screenshot
        if os.path.exists(img_path):
            try:
                os.remove(img_path)
            except Exception:
                pass


# =========================================================
# ALLURE ENVIRONMENT
# =========================================================
def _write_allure_environment(results_dir, session):
    """Ghi environment.properties vào results để Allure hiển thị Environment tab."""
    try:
        ensure_dir(results_dir)
        env_path = os.path.join(results_dir, "environment.properties")
        cfg = session.config

        lines = [
            f"os={platform.system()} {platform.release()}",
            f"os_version={platform.version()}",
            f"python={sys.version.split()[0]}",
            f"data_mode={cfg.getoption('--data-mode')}",
            f"data_file={cfg.getoption('--data-file')}",
            f"run_time={datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
        ]
        with open(env_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
    except Exception as e:
        print(f"[ALLURE] Cannot write environment.properties: {e}")


# =========================================================
# ALLURE HISTORY (PHẦN QUAN TRỌNG)
# =========================================================
def _preserve_allure_history(old_report_dir: str, new_results_dir: str):
    """
    Copy history từ report cũ -> results mới trước khi generate.
    Nhờ vậy Allure sẽ có History/Trend sau lần chạy thứ 2 trở đi.
    """
    try:
        src = os.path.join(old_report_dir, "history")
        dst = os.path.join(new_results_dir, "history")

        if os.path.isdir(src):
            ensure_dir(new_results_dir)
            if os.path.isdir(dst):
                shutil.rmtree(dst, ignore_errors=True)
            shutil.copytree(src, dst)
            print(f"[ALLURE] History copied: {src} -> {dst}")
        else:
            print(f"[ALLURE] No history to copy (first run or report was deleted): {src}")
    except Exception as e:
        print(f"[ALLURE] Cannot preserve history: {e}")


# =========================================================
# SESSION FINISH – GENERATE REPORT + HISTORY
# =========================================================
def pytest_sessionfinish(session, exitstatus):
    """
    CHỈ ghi environment.properties.
    TUYỆT ĐỐI không generate Allure report ở đây.
    """
    func_name = "login"
    allure_results = os.path.join(ALLURE_RESULTS_ROOT, func_name)
    ensure_dir(allure_results)
    _write_allure_environment(allure_results, session)
    
# =========================================================
# Environment
# =========================================================
def pytest_sessionstart(session):
    results_dir = session.config.getoption("--alluredir")
    write_allure_environment(
        results_dir,
        Project="MWC Website",
        Module="Login",
        DataSource=session.config.getoption("--data-source"),
        DataMode=session.config.getoption("--data-mode"),
        Browser="Chrome"
    )