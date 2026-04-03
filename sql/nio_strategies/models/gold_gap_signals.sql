-- This model identifies high-probability Gap Up signals
-- models/gold_nio_prices.sql
{{ config(materialized='view') }}
WITH gap_calculation AS (
    SELECT 
        *,
        -- Calculate the % gap from yesterday's close to today's open
        ((open - LAG(close) OVER (ORDER BY timestamp)) / LAG(close) OVER (ORDER BY timestamp)) * 100 AS gap_pct,
          -- Get the Close from exactly 1 row ago (the previous 5-min candle)
        LAG(close, 1) OVER (ORDER BY timestamp) as prev_candle_close 
    -- Expected columns in 'silver_nio_prices': timestamp, open, high, low, close, volume, sma_20_daily, vol_ma_20_daily
    FROM {{ source('market_data', 'silver_nio_prices') }}
)
---
SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume,
    gap_pct,
    sma_20_daily,
    vol_ma_20_daily,
     -- Calculate how much of the gap was recovered
        CASE 
            WHEN gap_pct > 0 THEN (OPEN - CLOSE) / NULLIF(OPEN - prev_candle_close, 0)
            WHEN gap_pct < 0 THEN (CLOSE - OPEN) / NULLIF(prev_candle_close - OPEN, 0)
        END as fill_ratio
FROM gap_calculation
WHERE 
    -- 1. Strategy: Gap must be between 1% and 10% (Higher than 10% is often too risky)
    gap_pct BETWEEN 1.0 AND 10.0
    
    -- 2. Trend Filter: Price must be ABOVE the 20-day Moving Average
    -- AND close > sma_20_daily
    
    -- 3. Volume Filter: Volume must be at least 1.2x the average daily volume
    -- AND volume > (1.2 * vol_ma_20_daily)

    -- 4. Time Filter: Only consider the most recent day (Assuming daily data)
    -- AND timestamp >= CURRENT_DATE - INTERVAL '1 day'
    -- 5. Optional: Add a volatility filter (e.g., ATR or standard deviation) if you want to further refine signals
    -- AND (high - low) / sma_20_daily < 0.05  -- Example: Filter out days with >5% intraday volatility
    -- 6. Optional: Add a filter to ensure the gap occurs at market open (
    -- AND CAST("timestamp" AS TIME) = '04:00:00')
    ORDER BY timestamp DESC