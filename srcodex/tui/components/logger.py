import logging
from pathlib import Path

# Create logs directory
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Single log file (overwrites on each run)
log_file = LOG_DIR / "srcodex.log"

# Setup logger
logger = logging.getLogger("srcodex")
logger.setLevel(logging.DEBUG)

# File handler (mode='w' overwrites the file)
handler = logging.FileHandler(log_file, mode='w')
handler.setLevel(logging.DEBUG)

# Format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
