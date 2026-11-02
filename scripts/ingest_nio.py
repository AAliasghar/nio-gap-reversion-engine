# from sklearn import pipeline
import yfinance as yf
import pandas as pd
import logging
from sqlalchemy import create_engine
from datetime import datetime

try:
    from fetch_window import fetch_start
except ImportError:  # pragma: no cover
    from scripts.fetch_window import fetch_start

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

    def get_last_timestamp(self):
        """Checks the database for the most recent data point."""
        try:
            query = 'SELECT MAX("DATETIME") FROM trading_warehouse.nio_strategy.bronze_nio_prices'
            result = pd.read_sql(query, self.engine)
            last_ts = result.iloc[0, 0]
            return last_ts
        except Exception:
            # If the table doesn't exist yet, return None
            return None

    def fetch_data(self):
        print("📡 STEP 1: Connecting to Market Data...")
        last_ts = self.get_last_timestamp()
        ticker = yf.Ticker(self.symbol)
        start, missing_days = fetch_start(last_ts, pd.Timestamp.now(tz="UTC"))

        if start is None:
            print("📝 No records found. Performing initial 60-day load (with Pre/Post market)...")
            df = ticker.history(period="60d", interval="5m", prepost=True)
        else:
            if missing_days:
                # Yahoo has no 5-minute bars older than 60 days, so this part of history is lost
                print(
                    f"⚠️ WARNING: last stored bar {last_ts} is older than the 60 days Yahoo serves. "
                    f"Loading from {start}; {missing_days} days of history are missing."
                )
            else:
                print(f"🔍 Last record found: {last_ts}. Fetching new data since then...")
            # Includes extended hours
            df = ticker.history(start=start, interval="5m", prepost=True)

        # Errors from Yahoo or the network are not caught here: they fail the Airflow task
        if df.empty:
            print("😴 Yahoo returned no new bars (market closed or already up to date).")
            return pd.DataFrame()

        print(f"✅ SUCCESS: Captured {len(df)} price points for {self.symbol}.")
        # Show a little preview of what we grabbed
        print("\n--- DATA PREVIEW ---")
        print(df[["Open", "Close", "Volume"]].head(3))
        print("--------------------\n")
        return df

    def transform_and_load(self, df, last_ts):
        if df.empty:
            print("😴 No new data found. Market might be closed or already up-to-date.")
            return

        print("🧱 STEP 2: Preparing data for PostgreSQL...")

        # 1. Standardize column
        df.reset_index(inplace=True)
        df.columns = [col.upper().replace(" ", "_") for col in df.columns]

        # 2.Only keep the columns we actually care about
        # This prevents "UndefinedColumn" errors if the API adds extra junk
        required_columns = ["DATETIME", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
        df = df[[c for c in required_columns if c in df.columns]].copy()

        # 3. Deduplicate (Fixes the SQL Error)
        if last_ts:
            # Only keep rows where DATETIME is GREATER than our last record
            # We use pd.to_datetime to ensure the types match perfectly
            df["DATETIME"] = pd.to_datetime(df["DATETIME"])
            df = df[df["DATETIME"] > pd.to_datetime(last_ts)]
        df["LOAD_TIME"] = datetime.now()

        if df.empty:
            print("✅ Database is already up to date. No new rows to add.")
            return

        print("📥 STEP 3: Saving to 'trading_warehouse'...")
        print(f"🧱 Adding {len(df)} brand new price points...")
        df["LOAD_TIME"] = datetime.now()

        try:
            # Using 'append' is correct for a warehouse, but since we
            # changed the columns, we might need to 'replace' once.
            df.to_sql(
                "bronze_nio_prices",
                self.engine,
                if_exists="append",
                index=False,
                schema="nio_strategy",
            )
            print(f"🎉 SUCCESS: Database updated up to {df['DATETIME'].max()}")

            # Show a small preview of the "Gap" if applicable
            self.display_gap_report(df)

        except Exception as e:
            print(f"❌ DATABASE ERROR: {e}")
            raise  # fail the Airflow task instead of reporting success

    def display_gap_report(self, df):
        """Simple terminal output to show price movement."""
        if len(df) > 1:
            start_price = df["CLOSE"].iloc[0]
            end_price = df["CLOSE"].iloc[-1]
            move = ((end_price - start_price) / start_price) * 100
            print(
                f"📈 Session Context: Price moved from ${start_price:.2f} to ${end_price:.2f} ({move:.2f}%)"
            )


if __name__ == "__main__":
    # Connection settings come from the NIO_DB_* env vars (see .env.example).
    try:
        from config import get_database_url
    except ImportError:
        from scripts.config import get_database_url

    DB_URL = get_database_url()

    # Rename 'pipeline' to 'nio_pipeline' to avoid sklearn conflicts
    nio_pipeline = NIODataPipeline("NIO", DB_URL)

    # 1. Get the last timestamp first
    last_ts = nio_pipeline.get_last_timestamp()

    # 2. Fetch the data
    raw_data = nio_pipeline.fetch_data()

    # 3. Pass the last_ts so we can filter duplicates
    nio_pipeline.transform_and_load(raw_data, last_ts)
