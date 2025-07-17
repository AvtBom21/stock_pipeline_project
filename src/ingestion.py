import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import os
from loguru import logger

RAW_DATA_DIR = "data/raw/"

def read_ticker_list_from_excel(excel_path: str) -> list:
    """
    Đọc danh sách mã cổ phiếu từ file Excel.
    """
    df = pd.read_excel(excel_path)
    tickers = df.iloc[:, 0].dropna().unique().tolist()
    valid_tickers = [f"{ticker.upper()}.VN" for ticker in tickers 
                     if len(ticker) == 3 and ticker.isalpha()]    
    return valid_tickers 

def download_data(ticker_list: list, start_date: str, end_date: str, save_dir="data/raw/") -> None:
    """
    Tải dữ liệu từ yfinance và lưu vào thư mục raw/ theo từng ticker.
    """
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    for ticker in ticker_list:
        try:
            logger.info(f"Downloading {ticker}")
            data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False)
            if not data.empty:
                data.reset_index(inplace=True)
                data['Ticker'] = ticker
                file_path = os.path.join(RAW_DATA_DIR, f"{ticker}.csv")
                data.to_csv(file_path, index=False)
                logger.success(f"Saved: {file_path}")
            else:
                logger.warning(f"No data found for {ticker}")
        except Exception as e:
            logger.error(f"Failed to download {ticker}: {e}")

if __name__ == "__main__":
    tickers = read_ticker_list_from_excel("Stock_ID.xlsx")
    end = datetime.today()
    start = end - timedelta(days=365)
    download_data(tickers, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
