from datetime import datetime
from pathlib import Path
import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DIR = Path(os.path.dirname(os.path.realpath(__file__)))

log_file = Path(f'{datetime.now().isoformat()[:-10]}_log.log'.replace(":", "_").replace("-", "_"))
log_file = Path(DIR / "logs" / log_file)
fh = logging.FileHandler(log_file)

formatter = logging.Formatter('%(message)s')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)