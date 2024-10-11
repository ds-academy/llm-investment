import os
import logging
from logging.handlers import RotatingFileHandler


def setup_global_logging(
        log_dir="logs",
        log_level=logging.DEBUG,
        file_level=logging.DEBUG,
        stream_level=logging.INFO,
        max_bytes=10 * 1024 * 1024,  # 10 MB
        backup_count=5,
        encoding="utf-8",
        telegram_token=None,
        telegram_chat_id=None,
        telegram_level=logging.ERROR,
):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # 포맷터 생성
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    )
    console_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # 파일 핸들러 추가
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, "app.log"),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding=encoding,
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(file_level)
    root_logger.addHandler(file_handler)

    # 스트림 핸들러 추가 (콘솔 출력)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(console_formatter)
    stream_handler.setLevel(stream_level)
    root_logger.addHandler(stream_handler)

    logging.info(
        f"Global logging setup complete. Log file: {os.path.join(log_dir, 'app.log')}"
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
