import logging
from pathlib import Path


def setup_runtime_logging(log_filename: str = "runtime.log"):
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Avoid duplicated handlers when scripts import each other.
    existing_file_paths = {
        getattr(handler, "baseFilename", None)
        for handler in root_logger.handlers
        if isinstance(handler, logging.FileHandler)
    }
    has_stream_handler = any(
        isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler)
        for handler in root_logger.handlers
    )

    if not has_stream_handler:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)

    runtime_log = str((log_dir / "runtime.log").resolve())
    script_log = str((log_dir / log_filename).resolve())

    for target in [runtime_log, script_log]:
        if target not in existing_file_paths:
            file_handler = logging.FileHandler(target, encoding="utf-8")
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

    return root_logger
