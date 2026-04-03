-----BRONZE LAYER----------------
--Raw Data
SELECT * FROM trading_warehouse.public.bronze_nio_prices order by "DATETIME" desc  ;

---
TRUNCATE TABLE bronze_nio_prices;
--This ensures that if the script tries to save the same 5-minute candle twice, the database will just say "No thanks" instead of making a mess.
ALTER TABLE bronze_nio_prices ADD CONSTRAINT unique_datetime UNIQUE ("DATETIME");

------------------------------------------------
ALTER TABLE trading_warehouse.public.bronze_nio_prices 
RENAME COLUMN "EXTRACTED_AT" TO "LOAD_TIME";


---SILVER LAYER --------------------------
------------------------------------------
DROP VIEW IF EXISTS v_nio_gap_strategy;
------------------------------------------
CREATE VIEW v_nio_gap_strategy AS
WITH base_calculations AS (
    SELECT 
        "DATETIME",
        -- Corrected: Convert to New York wall-clock time
        "DATETIME" AT TIME ZONE 'America/New_York' as ny_time,
        "CLOSE",
        "OPEN",
        "VOLUME",
        -- Get the Close from exactly 1 row ago (the previous 5-min candle)
        LAG("CLOSE", 1) OVER (ORDER BY "DATETIME") as prev_candle_close,
        AVG("CLOSE") OVER (ORDER BY "DATETIME" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sma_20_m, --20 candles -> 5 minutes --> 100 mins
         -- Calculate 20-period Volume Weighted Moving Average 5 mins candle
        SUM("CLOSE" * "VOLUME") OVER (ORDER BY "DATETIME" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as weighted_sum_m, --20 candles
        SUM("VOLUME") OVER (ORDER BY "DATETIME" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as total_vol_m,--20 candles
         -- Calculate 20-period Volume Weighted Moving Average -20 days
        AVG("CLOSE") OVER (ORDER BY "DATETIME" ROWS BETWEEN 1559 PRECEDING AND CURRENT ROW) as sma_20_d, --20 candles -> daily
        SUM("CLOSE" * "VOLUME") OVER (ORDER BY "DATETIME" ROWS BETWEEN 1559 PRECEDING AND CURRENT ROW) as weighted_sum_d, --20 candles
        SUM("VOLUME") OVER (ORDER BY "DATETIME" ROWS BETWEEN 1559 PRECEDING AND CURRENT ROW) as total_vol_d --20 candles
    FROM bronze_nio_prices
)
SELECT 
    "DATETIME",
    ny_time,
     "OPEN",
    "CLOSE",
    ROUND(prev_candle_close::numeric,2)prev_candle_close,
    ROUND(COALESCE(weighted_sum / NULLIF(total_vol, 0), sma_20)::NUMERIC, 2) as hybrid_vma_20,
	ROUND(("OPEN" - prev_candle_close)::NUMERIC, 2)*-1 as gap_size,
	ROUND( (("OPEN"::NUMERIC - prev_candle_close::NUMERIC) / NULLIF(prev_candle_close, 0)::NUMERIC) * 100, 2 ) * -1 AS gap_pct
FROM base_calculations
ORDER BY "DATETIME" DESC;
------------------------------------------
-----Volume‑Weighted Average Price per day 
WITH daily_vwap AS (
    SELECT 
        DATE("DATETIME" AT TIME ZONE 'America/New_York') AS trade_date,
        SUM("CLOSE" * "VOLUME") / NULLIF(SUM("VOLUME"), 0) AS vwap
    FROM bronze_nio_prices
    WHERE "VOLUME" > 0
    GROUP BY DATE("DATETIME" AT TIME ZONE 'America/New_York')
)
SELECT 
    trade_date,
    vwap,
    AVG(vwap) OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vwma_20d
FROM daily_vwap
ORDER BY trade_date DESC ;


---------Filter for Pre Market Time 
SELECT *
FROM v_nio_gap_strategy
WHERE 
    -- For dates before March 1st, 2026
    ("ny_time" < '2026-03-01' AND CAST("ny_time" AS TIME) = '04:00:00')
    OR
    -- For dates on or after March 1st, 2026
    ("ny_time" >= '2026-03-01' AND CAST("ny_time" AS TIME) = '04:00:00');

-----GOLD LAYER---------------------------
------------------------------------------
DROP VIEW IF EXISTS v_nio_gap_results;
------------------------------------------
CREATE VIEW v_nio_gap_results AS
WITH opening_gaps AS (
    SELECT 
        ny_time as gap_time,
        gap_pct,
        "OPEN" ,
        prev_candle_close as target_price
    FROM v_nio_gap_strategy
    WHERE ABS(gap_pct) > 0.5 -- Only look at gaps larger than 0.5%
)
,check_points AS (
    SELECT 
        g.*,
        b."CLOSE" as price_1030,
        -- Calculate how much of the gap was recovered
        CASE 
            WHEN g.gap_pct > 0 THEN (g. "OPEN" - b."CLOSE") / NULLIF(g. "OPEN" - g.target_price, 0)
            WHEN g.gap_pct < 0 THEN (b."CLOSE" - g. "OPEN") / NULLIF(g.target_price - g. "OPEN", 0)
        END as fill_ratio
    FROM opening_gaps g
    JOIN bronze_nio_prices b 
      ON b."DATETIME" AT TIME ZONE 'America/New_York' = g.gap_time + INTERVAL '1 hour'
)
SELECT 
    gap_time,
    gap_pct,
    fill_ratio * 100 as pct_of_gap_filled,
    CASE 
        WHEN fill_ratio >= 1 THEN '✅ FULL FILL'
        WHEN fill_ratio > 0 THEN '📈 PARTIAL FILL'
        ELSE '❌ FAILED (Trend Continued)'
    END as result,
     ROUND(
        COUNT(*) OVER (PARTITION BY 
            CASE 
                WHEN fill_ratio >= 1 THEN '✅ FULL FILL'
                WHEN fill_ratio > 0 THEN '📈 PARTIAL FILL'
                ELSE '❌ FAILED (Trend Continued)'
            END
        ) * 100.0 / COUNT(*) OVER (), 
        2
    ) AS category_percentage
FROM check_points
ORDER BY gap_time DESC;

----
SELECT * FROM  V_NIO_GAP_RESULTS;

--How to read these results:
--Gap %: How much the stock "jumped" overnight.
--Pct of Gap Filled: If this is 100%, the price returned exactly to yesterday's close by 10:30 AM. If it's 50%, it moved halfway back.
--The Goal: We are looking for consistency. If NIO has a "Full Fill" or "Partial Fill" 70% of the time, you have a high-probability trading signal.

SELECT * FROM trading_warehouse.public.bronze_nio_prices   order by "DATETIME" desc ;
--
SELECT * FROM trading_warehouse.public.silver_nio_prices   order by "timestamp" desc ;
SELECT CURRENT_TIME;

SELECT *, ((CLOSE -sma_20_daily)/sma_20_daily) * 100 AS SMA_deviation_per  , (CLOSE -sma_20_daily) AS SMA_deviation_val
    FROM silver_nio_prices 
    ORDER BY "timestamp"  DESC 
    LIMIT 100;
     
     
--     CREATE VIEW GOLD_nio_prices AS
WITH gap_calculation AS (
    SELECT 
        *,
        -- Calculate the % gap from yesterday's close to today's open
        ((open - LAG(close) OVER (ORDER BY timestamp)) / LAG(close) OVER (ORDER BY timestamp)) * 100 AS gap_pct
    -- Expected columns in 'silver_nio_prices': timestamp, open, high, low, close, volume, sma_20_daily, vol_ma_20_daily
    FROM silver_nio_prices
)
---
SELECT 
    TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI') AS timestamp,
    ROUND(OPEN::NUMERIC,2) AS OPEN,
    ROUND(high::NUMERIC,2) AS high,
    ROUND(low::NUMERIC,2) AS low,
    ROUND(close::NUMERIC,2) AS close,
    ROUND(volume::NUMERIC,2) AS volume,
    ROUND(gap_pct::NUMERIC,2) AS gap_pct,
    ROUND(sma_20_daily::NUMERIC,2) AS sma_20_daily,
    ROUND(vol_ma_20_daily::NUMERIC,2) AS vol_ma_20_daily
FROM gap_calculation
WHERE 
    -- 1. Strategy: Gap must be between 1% and 10% (Higher than 10% is often too risky)
    gap_pct BETWEEN 1 AND 10.0
    -- 2. Trend Filter: Price must be ABOVE the 20-day Moving Average
    AND close > sma_20_daily ;
    -- 3. Volume Filter: Volume must be at least 1.2x the average daily volume
--    AND volume > (1.2 * vol_ma_20_daily) 
    -- 4. Time Filter: Only consider the most recent day (Assuming daily data)
--    AND timestamp >= CURRENT_DATE - INTERVAL '1 day'
    -- 5. Optional: Add a volatility filter (e.g., ATR or standard deviation) if you want to further refine signals
--    AND (high - low) / sma_20_daily < 0.05  -- Example: Filter out days with >5% intraday volatility
    -- 6. Optional: Add a filter to ensure the gap occurs at market open (
    AND CAST("timestamp" AS TIME) = '04:00:00' ;

