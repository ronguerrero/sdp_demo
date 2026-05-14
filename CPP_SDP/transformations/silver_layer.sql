-- Silver Layer: Cleaned, validated, and enriched data
-- All quality checks use ON VIOLATION DROP ROW for tracked metrics

-- Cleaned securities dimension (batch read from streaming table)
CREATE OR REFRESH MATERIALIZED VIEW silver_securities_master(
  CONSTRAINT valid_security_id EXPECT (security_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_isin EXPECT (isin IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_security_name EXPECT (security_name IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleaned securities master - invalid records dropped with metrics"
AS SELECT
  security_id,
  isin,
  ticker,
  security_name,
  asset_class,
  sector,
  country,
  currency,
  exchange,
  issuer_name,
  maturity_date,
  coupon_rate,
  esg_score
FROM bronze_securities_master;

-- Cleaned counterparties dimension
CREATE OR REFRESH MATERIALIZED VIEW silver_counterparties(
  CONSTRAINT valid_counterparty_id EXPECT (counterparty_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_counterparty_name EXPECT (counterparty_name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_lei EXPECT (lei IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleaned counterparties - invalid records dropped with metrics"
AS SELECT
  counterparty_id,
  counterparty_name,
  counterparty_type,
  lei,
  country,
  credit_rating
FROM bronze_counterparties;

-- Cleaned fund structures dimension
CREATE OR REFRESH MATERIALIZED VIEW silver_fund_structures(
  CONSTRAINT valid_fund_id EXPECT (fund_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_fund_name EXPECT (fund_name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_strategy EXPECT (strategy IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleaned fund structures - invalid records dropped with metrics"
AS SELECT
  fund_id,
  fund_name,
  strategy,
  benchmark_id,
  inception_date,
  target_return,
  base_currency
FROM bronze_fund_structures;

-- Enriched trades: stream-static join with dimensions, compute notional value
CREATE OR REFRESH STREAMING TABLE silver_enriched_trades(
  CONSTRAINT valid_trade_id EXPECT (trade_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_trade_date EXPECT (trade_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT positive_price EXPECT (price > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_trade_type EXPECT (trade_type IN ('BUY', 'SELL')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_notional EXPECT (notional_value > 0) ON VIOLATION DROP ROW
)
COMMENT "Enriched trades joined with security, counterparty, and fund dimensions"
AS SELECT
  t.trade_id,
  t.order_id,
  t.security_id,
  t.counterparty_id,
  t.fund_id,
  t.trade_date,
  t.trade_type,
  t.quantity,
  t.price,
  t.currency,
  t.quantity * t.price AS notional_value,
  t.broker_id,
  t.execution_venue,
  t.settlement_status,
  s.security_name,
  s.asset_class,
  s.sector,
  s.isin,
  c.counterparty_name,
  c.counterparty_type,
  c.credit_rating,
  f.fund_name,
  f.strategy AS fund_strategy,
  f.base_currency AS fund_currency
FROM STREAM(bronze_trade_executions) t
LEFT JOIN silver_securities_master s ON t.security_id = s.security_id
LEFT JOIN silver_counterparties c ON t.counterparty_id = c.counterparty_id
LEFT JOIN silver_fund_structures f ON t.fund_id = f.fund_id;

-- Cleaned market data with daily return calculation
CREATE OR REFRESH STREAMING TABLE silver_market_data(
  CONSTRAINT valid_security_id EXPECT (security_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_price_date EXPECT (price_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_close_price EXPECT (close_price > 0) ON VIOLATION DROP ROW,
  CONSTRAINT positive_open_price EXPECT (open_price > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_price_range EXPECT (low_price <= high_price) ON VIOLATION DROP ROW
)
COMMENT "Cleaned market data with daily return percentage"
AS SELECT
  security_id,
  price_date,
  open_price,
  high_price,
  low_price,
  close_price,
  volume,
  currency,
  source,
  ROUND((close_price - open_price) / open_price * 100, 4) AS daily_return_pct
FROM STREAM(bronze_market_data);

-- Cleaned FX rates
CREATE OR REFRESH STREAMING TABLE silver_fx_rates(
  CONSTRAINT valid_rate_date EXPECT (rate_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_from_currency EXPECT (from_currency IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_to_currency EXPECT (to_currency IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_rate EXPECT (rate > 0) ON VIOLATION DROP ROW
)
COMMENT "Cleaned FX rates - invalid records dropped with metrics"
AS SELECT
  rate_date,
  from_currency,
  to_currency,
  rate,
  source
FROM STREAM(bronze_fx_rates);
