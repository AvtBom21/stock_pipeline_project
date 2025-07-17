import pandas as pd
import numpy as np
import os
from loguru import logger

from schema_validation import validate_schema

PROCESSED_DATA_DIR = "data/processed/"

def convert_to_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """
    Làm sạch và ép kiểu các cột số trong DataFrame:
    - Xoá dấu phân cách số nếu có (, .)
    - Ép kiểu về float hoặc int
    """
    numeric_columns = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

    for col in numeric_columns:
        if col in df.columns:
            # Xoá dấu phân cách và ký tự không số (nếu có)
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)  # Remove thousands separator
                .str.replace("%", "", regex=False)  # Remove percent sign if any
                .str.strip()
            )
            # Ép kiểu
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Chuyển Date về datetime
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    return df

def clean_and_engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Làm sạch dữ liệu và tính toán các chỉ số tài chính:
    - MA10, MA50
    - % Price Change
    - Bollinger Bands
    """
    if not validate_schema(df):
        raise ValueError("Schema validation failed")
    df = df.copy()
    df.dropna(subset=["Close"], inplace=True)  # Xoá dòng không có giá đóng cửa
    df.reset_index(drop=True, inplace=True)

    # Chuyển datetime
    df["Date"] = pd.to_datetime(df["Date"]).dt.date

    # MA10, MA50
    df["MA10"] = df["Close"].rolling(window=10).mean()
    df["MA50"] = df["Close"].rolling(window=50).mean()

    # % thay đổi giá
    df["Pct_change"] = df["Close"].pct_change() * 100

    # Bollinger Bands
    rolling_std = df["Close"].rolling(window=20).std()
    df["Upper_BB"] = df["MA10"] + 2 * rolling_std
    df["Lower_BB"] = df["MA10"] - 2 * rolling_std

    return df

def process_all_raw_data(raw_dir: str, processed_dir: str) -> None:
    """
    Đọc tất cả file từ thư mục raw/, xử lý và lưu kết quả vào processed/
    """
    os.makedirs(processed_dir, exist_ok=True)
    files = [f for f in os.listdir(raw_dir) if f.endswith(".csv")]

    for file in files:
        ticker = file.replace(".VN.csv", "")
        try:
            logger.info(f"Processing {ticker}")
            df = pd.read_csv(os.path.join(raw_dir, file))
            df_convert = convert_to_numeric(df)
            processed_df = clean_and_engineer_features(df_convert)
            processed_df.to_csv(os.path.join(processed_dir, f"{ticker}.csv"), index=False)
            logger.success(f"Saved processed file for {ticker}")
        except Exception as e:
            logger.error(f"Failed to process {ticker}: {e}")

def process_single_raw_file(file_path: str) -> pd.DataFrame:
    """
    Xử lý một file raw riêng lẻ và trả về DataFrame đã làm sạch & tính chỉ số.
    """
    try:
        df = pd.read_csv(file_path)
        df_convert = convert_to_numeric(df)
        processed_df = clean_and_engineer_features(df_convert)
        return processed_df
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return None


if __name__ == "__main__":
    process_all_raw_data("data/raw/", "data/processed/")
