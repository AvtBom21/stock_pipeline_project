from datetime import datetime, timedelta
from loguru import logger
import os

# Import các module thành phần
from ingestion import read_ticker_list_from_excel, download_data
from processing import process_all_raw_data
from visualization import visualize_all_processed_data

# Cấu hình thư mục
RAW_DIR = "data/raw/"
PROCESSED_DIR = "data/processed/"
TICKER_FILE = "Stock_ID.xlsx"

def main():
    logger.info("STARTING STOCK DATA PIPELINE")

    try:
        # Step 1: Load danh sách mã cổ phiếu
        ticker_list = read_ticker_list_from_excel(TICKER_FILE)
        logger.info(f"Loaded {len(ticker_list)} tickers.")

        # Step 2: Download dữ liệu trong 1 năm gần nhất
        end = datetime.today()
        start = end - timedelta(days=365)
        download_data(ticker_list, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

        # Step 3: Làm sạch và tính chỉ số kỹ thuật
        process_all_raw_data(RAW_DIR, PROCESSED_DIR)

        # Step 4: Vẽ biểu đồ phân tích kỹ thuật
        visualize_all_processed_data(PROCESSED_DIR)

        logger.success("PIPELINE EXECUTION COMPLETED SUCCESSFULLY.")

    except Exception as e:
        logger.exception(f"PIPELINE FAILED: {e}")

if __name__ == "__main__":
    main()