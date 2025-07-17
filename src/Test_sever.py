from init_mysql_db import initialize_mysql
from upload_to_mysql import upload_to_mysql
from loguru import logger

if __name__ == "__main__":
    logger.info("Starting test: init database + upload CSV to MySQL")

    # Bước 1: Khởi tạo database và bảng nếu chưa có
    initialize_mysql()

    # Bước 2: Upload dữ liệu lên MySQL từ thư mục processed
    upload_to_mysql(processed_dir="data/processed/")

    logger.success("Test completed: Data successfully uploaded to MySQL")
