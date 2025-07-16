from datetime import datetime, timedelta
from loguru import logger
import os

# Tạo thư mục logs nếu chưa có
os.makedirs("logs", exist_ok=True)

# Cấu hình log ghi ra file theo ngày
log_path = f"logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log"
logger.add(log_path, rotation="1 day", level="INFO", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

# Cấu hình thư mục dữ liệu
RAW_DIR = "data/raw/"
PROCESSED_DIR = "data/processed/"
TICKER_FILE = "Stock_ID.xlsx"

# Import các module thành phần
from ingestion import read_ticker_list_from_excel, download_data
from processing import process_all_raw_data
from backup import backup_files
#from visualization import visualize_all_processed_data
# Optional: from upload import upload_processed_data

def main():
    logger.info("STARTING STOCK DATA PIPELINE")

    try:
        # Step 1: Load danh sách mã cổ phiếu
        ticker_list = read_ticker_list_from_excel(TICKER_FILE)
        logger.info(f"Loaded {len(ticker_list)} tickers from {TICKER_FILE}")

        # Step 2: Download dữ liệu trong 1 năm gần nhất
        end = datetime.today()
        start = end - timedelta(days=365)
        logger.info(f"Downloading data from {start.date()} to {end.date()}")
        download_data(ticker_list, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

        # Step 3: Làm sạch và tính toán chỉ số kỹ thuật
        logger.info("Starting data cleaning and feature engineering")
        process_all_raw_data(RAW_DIR, PROCESSED_DIR)

        # Step 4: Sao lưu dữ liệu đã xử lý
        logger.info("Backing up processed files")
        backup_files(PROCESSED_DIR)

        # Step 5: Vẽ biểu đồ phân tích kỹ thuật
        logger.info("Generating technical visualizations")
        #visualize_all_processed_data(PROCESSED_DIR)

        # Optional Step 6: Giả lập upload lên cloud
        # logger.info(" Uploading to cloud server (simulated)")
        # upload_processed_data(PROCESSED_DIR)

        logger.success("PIPELINE EXECUTION COMPLETED SUCCESSFULLY.")

    except Exception as e:
        logger.exception(f"PIPELINE FAILED: {e}")

if __name__ == "__main__":
    main()
