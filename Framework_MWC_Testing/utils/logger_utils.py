import os
import logging
from datetime import datetime
import inspect

# --- Cache để đảm bảo mỗi chức năng chỉ tạo 1 logger duy nhất ---
_LOGGER_CACHE = {}


class _DailyLazyFileHandler(logging.Handler):
    """
    File handler "lười" (lazy):
    - Chỉ tạo/mở file khi có log record đầu tiên (emit lần đầu)
    - Tự đổi file theo ngày: nếu sang ngày mới thì đóng file cũ và mở file mới
    """

    def __init__(self, func_name: str, logs_dir: str, encoding: str = "utf-8"):
        super().__init__()
        self.func_name = func_name
        self.logs_dir = logs_dir
        self.encoding = encoding

        self._file_handler = None
        self._current_date = None  # dd.mm.YYYY
        self._header_written = False

    def _today_stamp(self) -> str:
        return datetime.now().strftime("%d.%m.%Y")

    def _build_log_path(self, date_stamp: str) -> str:
        filename = f"mwc_{self.func_name}_{date_stamp}.log"
        return os.path.join(self.logs_dir, filename)

    def _ensure_file_handler(self):
        """
        Đảm bảo file handler đã sẵn sàng cho đúng ngày hiện tại.
        - Nếu chưa có: tạo mới (lúc này file mới thực sự xuất hiện)
        - Nếu ngày đổi: rollover sang file mới
        """
        today = self._today_stamp()

        # Rollover nếu ngày đổi
        if self._current_date and self._current_date != today:
            try:
                if self._file_handler:
                    self._file_handler.close()
            finally:
                self._file_handler = None
                self._header_written = False

        # Nếu chưa có file handler, tạo (lazy)
        if self._file_handler is None:
            os.makedirs(self.logs_dir, exist_ok=True)
            log_path = self._build_log_path(today)

            # mode='a' để: cùng ngày -> ghi tiếp vào file cũ (nếu đã có)
            self._file_handler = logging.FileHandler(log_path, mode="a", encoding=self.encoding)
            self._file_handler.setLevel(self.level)

            # formatter của handler con sẽ dùng formatter của handler này
            if self.formatter:
                self._file_handler.setFormatter(self.formatter)

            self._current_date = today

            # Ghi header 1 lần (khi thật sự bắt đầu ghi log)
            if not self._header_written:
                try:
                    hdr_logger = logging.getLogger(f"_hdr_{self.func_name}")
                    hdr_logger.propagate = False
                    # ghi trực tiếp bằng file_handler (tránh vòng lặp emit)
                    self._file_handler.emit(logging.LogRecord(
                        name=self.func_name,
                        level=logging.INFO,
                        pathname=__file__,
                        lineno=0,
                        msg="=" * 60,
                        args=(),
                        exc_info=None
                    ))
                    self._file_handler.emit(logging.LogRecord(
                        name=self.func_name,
                        level=logging.INFO,
                        pathname=__file__,
                        lineno=0,
                        msg=f"=== Logger khởi tạo cho chức năng: {self.func_name.upper()} ===",
                        args=(),
                        exc_info=None
                    ))
                    self._file_handler.emit(logging.LogRecord(
                        name=self.func_name,
                        level=logging.INFO,
                        pathname=__file__,
                        lineno=0,
                        msg=f"Ghi log tại: {log_path}",
                        args=(),
                        exc_info=None
                    ))
                    self._file_handler.emit(logging.LogRecord(
                        name=self.func_name,
                        level=logging.INFO,
                        pathname=__file__,
                        lineno=0,
                        msg="=" * 60,
                        args=(),
                        exc_info=None
                    ))
                finally:
                    self._header_written = True

    def emit(self, record: logging.LogRecord):
        try:
            self._ensure_file_handler()
            # Delegate emit to real file handler
            self._file_handler.emit(record)
        except Exception:
            self.handleError(record)

    def close(self):
        try:
            if self._file_handler:
                self._file_handler.close()
        finally:
            self._file_handler = None
            super().close()


def _resolve_func_name(name: str = None) -> str:
    # --- 1. Xác định tên module gọi (nếu không truyền) ---
    if not name:
        frame = inspect.stack()[2]  # đi lùi thêm 1 frame vì create_logger gọi helper này
        caller_file = os.path.basename(frame.filename)
        base = os.path.splitext(caller_file)[0]
        name = base.replace("test_", "").replace("_page", "")
    else:
        name = name.lower().replace("test", "").replace("page", "").strip("_")

    return name.lower()


def create_logger(name: str = None) -> logging.Logger:
    """
    Tạo logger duy nhất cho từng chức năng.
    Yêu cầu:
    1) Chưa có file log -> chỉ tạo khi có log phát sinh (lazy)
    2) Có file log cùng ngày -> ghi tiếp vào file đó (append)
    3) Có file log nhưng khác ngày -> tự rollover sang file mới
    """
    global _LOGGER_CACHE

    func_name = _resolve_func_name(name)

    # --- 2. Nếu logger đã tồn tại, trả về luôn ---
    if func_name in _LOGGER_CACHE:
        return _LOGGER_CACHE[func_name]

    # --- 3. Tạo logger ---
    logger = logging.getLogger(func_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # tránh log bị nhân đôi qua root logger

    # --- 4. Cấu hình handlers (chỉ add 1 lần) ---
    if not logger.handlers:
        logs_dir = os.path.join(os.getcwd(), "reports", "logs")

        formatter = logging.Formatter(
            fmt="[%(asctime)s] [%(levelname)s] %(message)s",
            datefmt="%d/%m/%Y %H:%M:%S"
        )

        # Console handler (in ra màn hình ngay)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        # Lazy file handler (chỉ tạo file khi có log)
        lazy_file_handler = _DailyLazyFileHandler(func_name=func_name, logs_dir=logs_dir)
        lazy_file_handler.setLevel(logging.INFO)
        lazy_file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(lazy_file_handler)

    _LOGGER_CACHE[func_name] = logger
    return logger


# =====================================================================
# HÀM PHỤ: Tự động ghi loại dữ liệu và nguồn dữ liệu
# =====================================================================
def log_data_source_from_pytest(logger, pytestconfig):
    """
    Ghi tự động:
      - Nguồn dữ liệu: manual | ai
      - Định dạng dữ liệu: xlsx/csv/json/...
    lấy từ tham số pytest:
      --data-source
      --data-mode
    """

    try:
        source = pytestconfig.getoption("--data-source") or "manual"
    except Exception:
        source = "manual"

    try:
        mode = pytestconfig.getoption("--data-mode") or "excel"
    except Exception:
        mode = "excel"

    source = str(source).strip().lower()
    mode = str(mode).strip().lower()

    logger.info("=" * 60)
    logger.info(f"Đang truyền dữ liệu: {source.upper()}")
    logger.info(f"Định dạng file dữ liệu: {mode}")
    logger.info("=" * 60)

    return source, mode