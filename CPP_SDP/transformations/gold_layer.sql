-- Gold Layer: Business-level aggregations and analytics

-- Portfolio summary by fund: AUM, trade count, sector allocation
CREATE OR REFRESH MATERIALIZED VIEW gold_portfolio_summary
CLUSTER BY (fund_name)
COMMENT "Portfolio summary with AUM and trade metrics per fund"
AS SELECT
  fund_id,
  fund_name,
  fund_strategy,
  fund_currency,
  COUNT(DISTINCT trade_id) AS total_trades,
  COUNT(DISTINCT security_id) AS distinct_securities,
  SUM(notional_value) AS total_notional,
  AVG(notional_value) AS avg_trade_size,
  MIN(trade_date) AS first_trade_date,
  MAX(trade_date) AS last_trade_date
FROM silver_enriched_trades
GROUP BY ALL;

-- Daily trading activity by asset class
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_trading_activity
CLUSTER BY (trade_day, asset_class)
COMMENT "Daily trading volumes and notional by asset class"
AS SELECT
  DATE(trade_date) AS trade_day,
  asset_class,
  trade_type,
  COUNT(*) AS trade_count,
  SUM(quantity) AS total_quantity,
  SUM(notional_value) AS total_notional,
  AVG(price) AS avg_price
FROM silver_enriched_trades
GROUP BY ALL;

-- Counterparty exposure and risk summary
CREATE OR REFRESH MATERIALIZED VIEW gold_counterparty_exposure
CLUSTER BY (counterparty_name)
COMMENT "Counterparty exposure aggregated by credit rating"
AS SELECT
  counterparty_id,
  counterparty_name,
  counterparty_type,
  credit_rating,
  COUNT(DISTINCT trade_id) AS trade_count,
  COUNT(DISTINCT fund_id) AS funds_exposed,
  SUM(notional_value) AS total_exposure,
  SUM(CASE WHEN trade_type = 'BUY' THEN notional_value ELSE 0 END) AS buy_exposure,
  SUM(CASE WHEN trade_type = 'SELL' THEN notional_value ELSE 0 END) AS sell_exposure,
  MAX(trade_date) AS last_trade_date
FROM silver_enriched_trades
GROUP BY ALL;

-- Market performance summary by security
CREATE OR REFRESH MATERIALIZED VIEW gold_market_performance
CLUSTER BY (security_id)
COMMENT "Market performance metrics per security"
AS SELECT
  m.security_id,
  s.security_name,
  s.asset_class,
  s.sector,
  COUNT(*) AS trading_days,
  AVG(m.daily_return_pct) AS avg_daily_return_pct,
  MIN(m.daily_return_pct) AS worst_daily_return_pct,
  MAX(m.daily_return_pct) AS best_daily_return_pct,
  AVG(m.close_price) AS avg_close_price,
  AVG(m.volume) AS avg_daily_volume,
  MIN(m.price_date) AS first_date,
  MAX(m.price_date) AS last_date
FROM silver_market_data m
LEFT JOIN silver_securities_master s ON m.security_id = s.security_id
GROUP BY ALL;

-- Settlement status summary
CREATE OR REFRESH MATERIALIZED VIEW gold_settlement_status
COMMENT "Trade settlement status tracking by fund and venue"
AS SELECT
  fund_name,
  execution_venue,
  settlement_status,
  COUNT(*) AS trade_count,
  SUM(notional_value) AS total_notional,
  AVG(notional_value) AS avg_notional
FROM silver_enriched_trades
GROUP BY ALL;
