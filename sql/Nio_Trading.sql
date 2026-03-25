SELECT * FROM trading_warehouse.public.bronze_nio_prices order by "DATETIME" desc ;


--This ensures that if the script tries to save the same 5-minute candle twice, the database will just say "No thanks" instead of making a mess.
ALTER TABLE bronze_nio_prices ADD CONSTRAINT unique_datetime UNIQUE ("DATETIME");

------------------------------------------------
ALTER TABLE trading_warehouse.public.bronze_nio_prices 
RENAME COLUMN "EXTRACTED_AT" TO "LOAD_TIME";

------------------------------------------
CREATE OR REPLACE VIEW v_nio_gap_strategy AS
WITH daily_markers AS (
    SELECT 
        "DATETIME",
        "CLOSE",
        "OPEN",
        "VOLUME",
        -- Get the Close from exactly 1 row ago (the previous 5-min candle)
        LAG("CLOSE", 1) OVER (ORDER BY "DATETIME") as prev_candle_close,
        -- Calculate 20-period Volume Weighted Moving Average
        SUM("CLOSE" * "VOLUME") OVER (ORDER BY "DATETIME" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) / 
        NULLIF(SUM("VOLUME") OVER (ORDER BY "DATETIME" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 0) as vwma_20
    FROM bronze_nio_prices
)
SELECT 
    *,
    -- The actual price gap in dollars
    ("OPEN" - prev_candle_close) as gap_size,
    -- The gap as a percentage
    (("OPEN" - prev_candle_close) / NULLIF(prev_candle_close, 0)) * 100 as gap_pct,
    -- Signal: If price is far from VWMA, it's a "Stretch"
    ("CLOSE" - vwma_20) as vwma_distance
FROM daily_markers
-- Focus on the market open (9:30 AM EST)
WHERE CAST("DATETIME" AS TIME) = '09:30:00'
ORDER BY "DATETIME" DESC;