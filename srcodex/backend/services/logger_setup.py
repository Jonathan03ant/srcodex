"""
Logging configuration for srcodex
Sets up file logging to .srcodex/.debug/backend.log
"""
import logging
from pathlib import Path
from .config_loader import get_config


def setup_backend_logging():
    """
    Configure backend logging to write to .srcodex/.debug/backend.log
    Creates the .debug directory if it doesn't exist
    """
    try:
        config = get_config()
        debug_dir = config.project_root / ".srcodex" / ".debug"
        debug_dir.mkdir(parents=True, exist_ok=True)

        log_file = debug_dir / "backend.log"

        # Get root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)

        # Clear any existing handlers to avoid duplicates
        root_logger.handlers.clear()

        # File handler
        file_handler = logging.FileHandler(log_file, mode='a')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        )
        root_logger.addHandler(file_handler)

        # Console handler (for debug mode)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
        )
        root_logger.addHandler(console_handler)

        logger = logging.getLogger(__name__)
        logger.info(f"Backend logging initialized: {log_file}")

        return log_file

    except Exception as e:
        # Fallback to console-only logging if config fails
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        logger = logging.getLogger(__name__)
        logger.warning(f"Could not set up file logging: {e}")
        return None
