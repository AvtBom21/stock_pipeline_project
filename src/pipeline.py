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
import pandas as pd
from ingestion import download_data
from processing import process_single_raw_file
from init_mysql_db import initialize_mysql
from upload_to_mysql import upload_to_mysql
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

        # Optional Step 6: khởi tạo và upload lên sever MySQL)
        logger.info("Initializing MySQL database")
        initialize_mysql()
        logger.info("Uploading processed data to MySQL")
        upload_to_mysql(processed_dir=PROCESSED_DIR)

        logger.success("PIPELINE EXECUTION COMPLETED SUCCESSFULLY.")

    except Exception as e:
        logger.exception(f"PIPELINE FAILED: {e}")

def run_daily_update():
    logger.info("STARTING DAILY STOCK DATA UPDATE")

    try:
        # Load danh sách ticker
        ticker_list = read_ticker_list_from_excel(TICKER_FILE)
        logger.info(f"Loaded {len(ticker_list)} tickers from {TICKER_FILE}")

        # Lấy ngày hôm nay
        today = datetime.today()
        start = end = today.strftime("%Y-%m-%d")
        logger.info(f"Fetching data for today: {start}")

        # Tạo thư mục tạm cho raw file ngày hôm nay
        temp_raw_dir = "data/raw_today/"
        os.makedirs(temp_raw_dir, exist_ok=True)

        # Download dữ liệu chỉ cho hôm nay
        download_data(ticker_list, start, end, save_dir=temp_raw_dir)

        # Xử lý từng file và gộp vào file đã có
        for ticker in ticker_list:
            raw_file = os.path.join(temp_raw_dir, f"{ticker}.VN.csv")
            if not os.path.exists(raw_file):
                logger.warning(f"No data file for {ticker} today.")
                continue

            processed_df_today = process_single_raw_file(raw_file)
            if processed_df_today is None or processed_df_today.empty:
                logger.warning(f"No data to append for {ticker}")
                continue

            processed_file = os.path.join(PROCESSED_DIR, f"{ticker}.csv")
            if os.path.exists(processed_file):
                old_df = pd.read_csv(processed_file)
                # Gộp và loại bỏ ngày trùng
                combined_df = pd.concat([old_df, processed_df_today])
                combined_df.drop_duplicates(subset=["Date"], keep="last", inplace=True)
                combined_df.sort_values(by="Date", inplace=True)
                combined_df.to_csv(processed_file, index=False)
                logger.success(f"Appended today's data to {ticker}.csv")
            else:
                processed_df_today.to_csv(processed_file, index=False)
                logger.success(f"Created new processed file for {ticker}")

        # Backup sau khi cập nhật
        backup_files(PROCESSED_DIR)

        # Optional: Upload dữ liệu mới lên MySQL
        logger.info("Uploading updated data to MySQL")
        upload_to_mysql(processed_dir=PROCESSED_DIR)
        logger.success("DAILY UPDATE COMPLETED SUCCESSFULLY.")

    except Exception as e:
        logger.exception(f"DAILY UPDATE FAILED: {e}")


if __name__ == "__main__":
    #main()
    run_daily_update()
