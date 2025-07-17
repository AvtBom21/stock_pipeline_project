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

    if not files:
        logger.warning("Không có file nào trong thư mục processed để backup.")
        return

    count = 0
    failed_files = []

    for file in files:
        try:
            src = os.path.join(src_dir, file)
            dest = os.path.join(dest_folder, file)

            if os.path.getsize(src) == 0:
                logger.warning(f"File rỗng, không backup: {file}")
                failed_files.append(file)
                continue

            shutil.copy2(src, dest)
            logger.info(f"Đã backup: {file}")
            count += 1
        except Exception as e:
            logger.error(f"Lỗi khi backup {file}: {e}")
            failed_files.append(file)

    logger.success(f"Đã backup {count} file vào thư mục: {dest_folder}")

    if failed_files:
        logger.warning(f"Không backup được {len(failed_files)} file:")
        for f in failed_files:
            logger.warning(f"  - {f}")
