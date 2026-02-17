-- Monthly volume distribution by implied probability (price) bands

WITH base AS (
  SELECT
    DATE_TRUNC('month', block_time) AS month,
    price,
    amount
  FROM polymarket_polygon.market_trades
  WHERE block_time >= date'2024-01-01'
    AND amount > 0
),

binned AS (
  SELECT
    month,
    CASE
      WHEN price < 0.20 THEN 'f. 0-20%'
      WHEN price < 0.40 THEN 'e. 20-40%'
      WHEN price < 0.60 THEN 'd. 40-60%'
      WHEN price < 0.80 THEN 'c. 60-80%'
      WHEN price < 0.99 THEN 'b. 80-99%'
      ELSE 'a. 99-100%'
    END AS price_band,
    amount AS volume_usd
  FROM base
),

monthly AS (
  SELECT
    month,
    price_band,
    SUM(volume_usd) AS volume_usd
  FROM binned
  GROUP BY 1, 2
)

SELECT
  month,
  price_band,
  volume_usd,
  ROUND(
    100 * volume_usd / NULLIF(SUM(volume_usd) OVER (PARTITION BY month), 0),
    2
  ) AS pct_of_month
FROM monthly
ORDER BY month, price_band;