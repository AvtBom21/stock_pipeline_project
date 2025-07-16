import os
import pandas as pd

RAW_DATA_DIR = "data/raw/"

def inspect_raw_files(raw_dir: str):
    files = [f for f in os.listdir(raw_dir) if f.endswith(".csv")]
    summary = []

    for file in files:
        path = os.path.join(raw_dir, file)
        ticker = file.replace(".csv", "")
        try:
            df = pd.read_csv(path)
            info = {
                "Ticker": ticker,
                "Rows": len(df),
                "Columns": df.columns.tolist(),
                "Has_Close": "Close" in df.columns,
                "Close_dtype": df["Close"].dtype if "Close" in df.columns else "N/A",
                "Missing_Close_%": df["Close"].isna().mean() * 100 if "Close" in df.columns else "N/A",
                "First_Date": df["Date"].iloc[0] if "Date" in df.columns else "N/A",
                "Last_Date": df["Date"].iloc[-1] if "Date" in df.columns else "N/A"
            }
            summary.append(info)
        except Exception as e:
            summary.append({"Ticker": ticker, "Error": str(e)})

    return pd.DataFrame(summary)

if __name__ == "__main__":
    summary_df = inspect_raw_files(RAW_DATA_DIR)
    summary_df.to_csv("notebooks/raw_data_summary.csv", index=False)
    print(summary_df.head(10))  # Xem nhanh
