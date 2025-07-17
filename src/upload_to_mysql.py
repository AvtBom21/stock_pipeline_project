import os
import pandas as pd
import mysql.connector
from loguru import logger
from datetime import datetime

from init_mysql_db import DB_NAME

def upload_to_mysql(processed_dir="data/processed/"):
    today = datetime.today().date()

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",  # XAMPP mặc định không có mật khẩu
        database=DB_NAME
    )
    cursor = conn.cursor()

    files = [f for f in os.listdir(processed_dir) if f.endswith(".csv")]

    uploaded = []
    skipped = []

    for file in files:
        ticker = os.path.splitext(file)[0].upper()
        path = os.path.join(processed_dir, file)

        df = pd.read_csv(path, parse_dates=["Date"])
        if df.empty:
            logger.warning(f"Empty file skipped: {file}")
            skipped.append(ticker)
            continue

        # Chuyển đổi cột Date sang dạng date
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        df_today = df[df['Date'] == today]

        if df_today.empty:
            logger.warning(f"No data for today in {file}")
            skipped.append(ticker)
            continue

        for _, row in df_today.iterrows():
            sql = """
            INSERT INTO stock_prices 
            (ticker, date, open, high, low, close, adj_close, volume, ma10, ma50, pct_change, bb_upper, bb_lower)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                row['Ticker'],
                row['Date'],
                row['Open'],
                row['High'],
                row['Low'],
                row['Close'],
                row['Adj Close'],
                row['Volume'],
                row['MA10'],
                row['MA50'],
                row['Pct_change'],
                row['Upper_BB'],
                row['Lower_BB']
            )
            cursor.execute(sql, values)

        conn.commit()
        uploaded.append(ticker)

    cursor.close()
    conn.close()

    logger.success("MYSQL UPLOAD COMPLETE")
    logger.info(f"Tổng cộng xử lý {len(files)} file")
    logger.info(f"Đã upload {len(uploaded)} file có dữ liệu hôm nay ({today})")
    logger.info(", ".join(uploaded) if uploaded else "Không có file nào được upload")

    logger.warning(f"{len(skipped)} file KHÔNG có dữ liệu hôm nay ({today})")
    logger.warning(", ".join(skipped) if skipped else "Tất cả file đều có dữ liệu")
