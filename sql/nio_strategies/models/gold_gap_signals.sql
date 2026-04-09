{{ config(materialized='table') }}

WITH timezone_adj AS (
    SELECT
        *,
        "timestamp" AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS ny_time,
        ("timestamp" AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date AS trading_date
    FROM {{ source('nio_strategy', 'silver_nio_prices') }}
),

daily_summary AS (
    SELECT
        trading_date,
        -- 1. Prices for Gap & Daily SMA
        (ARRAY_AGG(open ORDER BY ny_time ASC) FILTER (WHERE ny_time::time >= '04:00:00'))[1] AS pre_market_open,
        (ARRAY_AGG("close" ORDER BY ny_time DESC) FILTER (WHERE ny_time::time <= '16:00:00'))[1] AS regular_close,
        
        -- 2. Volume for Vol MA (Sum of all 5-min volume bars in the day)
        SUM(volume) AS daily_total_volume,
        
        -- 3. VWAP Components (Daily VWAP = Total Value / Total Volume)
        SUM("close" * volume) / NULLIF(SUM(volume), 0) AS daily_raw_vwap,

        -- 4. Intraday High/Low for Gap Fill Logic
        MAX(high) FILTER (WHERE ny_time::time >= '09:30:00' AND ny_time::time <= '16:00:00') AS rth_high,
        MIN(low) FILTER (WHERE ny_time::time >= '09:30:00' AND ny_time::time <= '16:00:00') AS rth_low
    FROM timezone_adj
    GROUP BY trading_date
),

moving_averages AS (
    SELECT 
        *,
        -- Daily Price SMA (20-day)
        AVG(regular_close) OVER (ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS daily_sma_20,
        
        -- Daily Volume MA (20-day)
        AVG(daily_total_volume) OVER (ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS daily_vol_ma_20,
        
        -- Daily VWAP MA (20-day)
        AVG(daily_raw_vwap) OVER (ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS daily_vwap_20
    FROM daily_summary
),

lagged_features AS (
    SELECT
        *,
        LAG(regular_close) OVER (ORDER BY trading_date) AS prev_regular_close,
        LAG(daily_sma_20) OVER (ORDER BY trading_date) AS prev_daily_sma_20,
        LAG(daily_vol_ma_20) OVER (ORDER BY trading_date) AS prev_daily_vol_ma_20,
        LAG(daily_vwap_20) OVER (ORDER BY trading_date) AS prev_daily_vwap_20
    FROM moving_averages
),

gap_analysis AS (
    SELECT
        trading_date,
        pre_market_open,
        prev_regular_close,
        
        -- Gap Calculations
        pre_market_open - prev_regular_close AS gap_value,
        ((pre_market_open - prev_regular_close) / prev_regular_close) * 100 AS gap_percentage,
        
        -- Feature: Price vs Daily SMA
        CASE WHEN pre_market_open > prev_daily_sma_20 THEN 1 ELSE 0 END AS is_above_ma_20,
        
        -- Feature: Volume Regime (Is today's pre-market volume higher than the 20-day avg?)
        -- (Note: You can compare daily_total_volume vs prev_daily_vol_ma_20 here)
        
        -- Feature: Trend Identification
        CASE WHEN prev_regular_close > prev_daily_sma_20 THEN 'UPWARD' ELSE 'DOWNWARD' END AS daily_trend,
        
        -- Target Variable: Gap Fill
        CASE
            WHEN pre_market_open > prev_regular_close AND rth_low <= prev_regular_close THEN 1
            WHEN pre_market_open < prev_regular_close AND rth_high >= prev_regular_close THEN 1
            ELSE 0
        END AS gap_filled_flag,
        
        -- Metadata for ML
        prev_daily_vol_ma_20,
        prev_daily_vwap_20
        
    FROM lagged_features
    WHERE prev_regular_close IS NOT NULL AND pre_market_open IS NOT NULL
)

SELECT * FROM gap_analysis;