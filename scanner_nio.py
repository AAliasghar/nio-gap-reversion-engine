import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


def scan_for_signals():
    # 1. Connect to your Warehouse
    DB_URL = "postgresql://quant_user:quant_password@localhost:5432/trading_warehouse"
    engine = create_engine(DB_URL)

    print(
        f"🕵️ Scanning NIO for Gap Signals... [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
    )

    # 2. Query the Silver View for the most recent day
    query = """
    SELECT ny_time, gap_pct, gap_size, hybrid_vma_20, "CLOSE"
    FROM v_nio_gap_strategy 
    ORDER BY ny_time DESC 
    LIMIT 1;
    """

    try:
        df = pd.read_sql(query, engine)

        if df.empty:
            print("📭 No data found in the Silver View.")
            return

        latest = df.iloc[0]
        gap = latest["gap_pct"]

        # 3. Strategy Logic (Based on your 69% backtest)
        print(f"📊 Latest Opening Gap: {gap:.2f}%")

        if abs(gap) > 1.0:
            print("🚨 SIGNAL DETECTED: Significant Gap!")
            if gap > 0:
                print(
                    f"📉 DIRECTION: SHORT (Betting on a Fill down to ${latest['hybrid_vma_20']:.2f})"
                )
            else:
                print(
                    f"📈 DIRECTION: LONG (Betting on a Fill up to ${latest['hybrid_vma_20']:.2f})"
                )
        else:
            print("😴 No trade today. Gap is too small to meet the 'Edge' criteria.")

    except Exception as e:
        print(f"❌ Scanner Error: {e}")


if __name__ == "__main__":
    scan_for_signals()
