import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

try:
    from config import get_database_url
except ImportError:  # pragma: no cover
    from scripts.config import get_database_url


def scan_for_signals():
    db_url = get_database_url()
    engine = create_engine(db_url)

    print(
        f"🕵️ Scanning NIO for Gap Signals... [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
    )

    query = """
    SELECT *
    FROM nio_strategy.gold_gap_signals
    ORDER BY trading_date DESC
    LIMIT 1;
    """

    try:
        df = pd.read_sql(query, engine)

        if df.empty:
            print("📭 No data found in the dbt Analysis table.")
            return

        latest = df.iloc[0]
        gap_percentage = latest["gap_percentage"]
        gap_value = latest["gap_value"]

        print(f"📊 Latest Opening Gap: {gap_percentage:.2f}%")

        if abs(gap_percentage) > 1.0:
            print("🚨 SIGNAL DETECTED: Significant Gap!")
            if gap_value > 0:
                print(
                    f"📉 DIRECTION: SHORT (Betting on a Fill of {gap_percentage:.2f}%)"
                )
            else:
                print(
                    f"📈 DIRECTION: LONG (Betting on a Fill of {gap_percentage:.2f}%)"
                )
        else:
            print("😴 No trade today. Gap is too small to meet the 'Edge' criteria.")

    except Exception as e:
        raise e


if __name__ == "__main__":
    scan_for_signals()
