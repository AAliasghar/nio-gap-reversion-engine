import yfinance as yf
import pandas as pd
import logging
from sqlalchemy import create_engine
from datetime import datetime
import time

# --- LOGGING (The "Black Box" Recorder) ---
logging.basicConfig(
    filename="pipeline.log",  # Saves technical details to a file, not your screen!
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class NIODataPipeline:
    def __init__(self, ticker_symbol, db_url):
        self.symbol = ticker_symbol
        self.engine = create_engine(db_url)
        print(f"\n{'='*40}")
        print(f"🚀 INITIALIZING: {self.symbol} QUANT PIPELINE")
        print(f"{'='*40}")

    def fetch_data(self):
        print(f"📡 STEP 1: Connecting to Market Data...")
        try:
            ticker = yf.Ticker(self.symbol)
            df = ticker.history(period="5d", interval="5m")

            if df.empty:
                print("❌ ERROR: No data found. Is the market open?")
                return pd.DataFrame()

            print(f"✅ SUCCESS: Captured {len(df)} price points for {self.symbol}.")
            # Show a little preview of what we grabbed
            print(f"\n--- DATA PREVIEW ---")
            print(df[["Open", "Close", "Volume"]].head(3))
            print(f"--------------------\n")
            return df
        except Exception as e:
            logging.error(f"Fetch failed: {e}")
            print(f"💥 FATAL ERROR during fetch. Check 'pipeline.log' for details.")
            return pd.DataFrame()


    def transform_and_load(self, df):
        if df.empty:
            return

        print(f"🧱 STEP 2: Preparing data for PostgreSQL...")
        df.reset_index(inplace=True)

        # 1. Standardize column names
        df.columns = [col.upper().replace(" ", "_") for col in df.columns]

        # 2.Only keep the columns we actually care about
        # This prevents "UndefinedColumn" errors if the API adds extra junk
        required_columns = ["DATETIME", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]

        # Only keep columns that actually exist in the dataframe
        df = df[[col for col in required_columns if col in df.columns]]

        df["EXTRACTED_AT"] = datetime.now()

        print(f"📥 STEP 3: Saving to 'trading_warehouse'...")
        try:
            # Using 'append' is correct for a warehouse, but since we
            # changed the columns, we might need to 'replace' once.
            df.to_sql("bronze_nio_prices", self.engine, if_exists="append", index=False)
            print(f"🎉 DONE: Warehouse updated successfully!")
        except Exception as e:
            print(f"❌ DATABASE ERROR: {e}")


if __name__ == "__main__":
    #  connection string
    DB_URL = "postgresql://quant_user:quant_password@localhost:5432/trading_warehouse"

    # Run the pipeline
    pipeline = NIODataPipeline("NIO", DB_URL)
    data = pipeline.fetch_data()
    pipeline.transform_and_load(data)
