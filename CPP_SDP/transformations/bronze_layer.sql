-- Bronze Layer: Raw data ingestion with data quality checks (warn only, no rows dropped)

CREATE OR REFRESH STREAMING TABLE bronze_trade_executions(
  CONSTRAINT valid_trade_id EXPECT (trade_id IS NOT NULL),
  CONSTRAINT valid_trade_date EXPECT (trade_date IS NOT NULL),
  CONSTRAINT positive_quantity EXPECT (quantity > 0),
  CONSTRAINT positive_price EXPECT (price > 0),
  CONSTRAINT valid_currency EXPECT (currency IS NOT NULL)
)
COMMENT "Bronze trade executions with data quality monitoring"
AS SELECT * FROM STREAM(ronguerrero.cpp_investment_ops.raw_trade_executions);

CREATE OR REFRESH STREAMING TABLE bronze_securities_master(
  CONSTRAINT valid_security_id EXPECT (security_id IS NOT NULL),
  CONSTRAINT valid_isin EXPECT (isin IS NOT NULL),
  CONSTRAINT valid_security_name EXPECT (security_name IS NOT NULL),
  CONSTRAINT valid_asset_class EXPECT (asset_class IS NOT NULL)
)
COMMENT "Bronze securities master with data quality monitoring"
AS SELECT * FROM STREAM(ronguerrero.cpp_investment_ops.raw_securities_master);

CREATE OR REFRESH STREAMING TABLE bronze_market_data(
  CONSTRAINT valid_security_id EXPECT (security_id IS NOT NULL),
  CONSTRAINT valid_price_date EXPECT (price_date IS NOT NULL),
  CONSTRAINT positive_close_price EXPECT (close_price > 0),
  CONSTRAINT positive_volume EXPECT (volume >= 0),
  CONSTRAINT valid_price_range EXPECT (low_price <= high_price)
)
COMMENT "Bronze market data with data quality monitoring"
AS SELECT * FROM STREAM(ronguerrero.cpp_investment_ops.raw_market_data);

CREATE OR REFRESH STREAMING TABLE bronze_fx_rates(
  CONSTRAINT valid_rate_date EXPECT (rate_date IS NOT NULL),
  CONSTRAINT valid_from_currency EXPECT (from_currency IS NOT NULL),
  CONSTRAINT valid_to_currency EXPECT (to_currency IS NOT NULL),
  CONSTRAINT positive_rate EXPECT (rate > 0)
)
COMMENT "Bronze FX rates with data quality monitoring"
AS SELECT * FROM STREAM(ronguerrero.cpp_investment_ops.raw_fx_rates);

CREATE OR REFRESH STREAMING TABLE bronze_counterparties(
  CONSTRAINT valid_counterparty_id EXPECT (counterparty_id IS NOT NULL),
  CONSTRAINT valid_counterparty_name EXPECT (counterparty_name IS NOT NULL),
  CONSTRAINT valid_lei EXPECT (lei IS NOT NULL),
  CONSTRAINT valid_credit_rating EXPECT (credit_rating IS NOT NULL)
)
COMMENT "Bronze counterparties with data quality monitoring"
AS SELECT * FROM STREAM(ronguerrero.cpp_investment_ops.raw_counterparties);

CREATE OR REFRESH STREAMING TABLE bronze_fund_structures(
  CONSTRAINT valid_fund_id EXPECT (fund_id IS NOT NULL),
  CONSTRAINT valid_fund_name EXPECT (fund_name IS NOT NULL),
  CONSTRAINT valid_inception_date EXPECT (inception_date IS NOT NULL),
  CONSTRAINT positive_target_return EXPECT (target_return >= 0)
)
COMMENT "Bronze fund structures with data quality monitoring"
AS SELECT * FROM STREAM(ronguerrero.cpp_investment_ops.raw_fund_structures);
