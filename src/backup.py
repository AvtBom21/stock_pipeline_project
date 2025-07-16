import os
import shutil
from datetime import datetime
from loguru import logger

def backup_files(src_dir="data/processed/", backup_dir="data/backup/"):
    """
    Sao lưu toàn bộ processed files vào thư mục backup theo timestamp
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_folder = os.path.join(backup_dir, f"backup_{timestamp}")
    os.makedirs(dest_folder, exist_ok=True)

    files = [f for f in os.listdir(src_dir) if f.endswith(".csv")]

    for file in files:
        src = os.path.join(src_dir, file)
        dest = os.path.join(dest_folder, file)
        shutil.copy2(src, dest)
        logger.info(f"Backup file: {file} → {dest_folder}")
