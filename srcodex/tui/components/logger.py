"""
TUI Logging Setup
Logs TUI events to .srcodex/.debug/tui.log
"""
import logging
from pathlib import Path
import sys

# Add backend to path for config loader
backend_path = Path(__file__).parent.parent.parent / "backend"
sys.path.insert(0, str(backend_path))

from services.config_loader import get_config


def setup_tui_logging():
    """
    Configure TUI logging to write to .srcodex/.debug/tui.log
    Returns the logger instance
    """
    try:
        config = get_config()
        debug_dir = config.project_root / ".srcodex" / ".debug"
        debug_dir.mkdir(parents=True, exist_ok=True)

        log_file = debug_dir / "tui.log"

        # Create logger
        logger = logging.getLogger('srcodex.tui')
        logger.setLevel(logging.INFO)

        # File handler (append mode)
        file_handler = logging.FileHandler(log_file, mode='a')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        )

        logger.addHandler(file_handler)
        logger.info("=" * 80)
        logger.info("TUI session started")

        return logger

    except Exception as e:
        # Fallback logger
        logger = logging.getLogger('srcodex.tui')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(handler)
        logger.warning(f"Could not set up TUI file logging: {e}")
        return logger


# Initialize logger module-level
logger = setup_tui_logging()
