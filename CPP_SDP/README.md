# CPP_SDP - Investment Operations Pipeline

A Lakeflow Spark Declarative Pipeline (SDP) that processes investment operations data through a medallion architecture (Bronze → Silver → Gold), deployed as a Databricks Asset Bundle.

## Architecture

```
ronguerrero.cpp_investment_ops (raw)
        │
        ▼
┌─────────────────────────────────────────────┐
│  BRONZE (Streaming Tables)                  │
│  - Ingests append-only raw data             │
│  - Warn-only quality checks (no drops)      │
│  - Tracks violation metrics                 │
└─────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────┐
│  SILVER (Streaming Tables + MVs)            │
│  - DROP ROW quality checks with metrics     │
│  - Enriched trades via dimension joins      │
│  - Computed fields (notional, daily return) │
└─────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────┐
│  GOLD (Materialized Views)                  │
│  - Portfolio summary per fund               │
│  - Daily trading activity by asset class    │
│  - Counterparty exposure & risk             │
│  - Market performance by security           │
│  - Settlement status tracking               │
└─────────────────────────────────────────────┘
```

## Datasets

### Bronze Layer (`bronze_layer.sql`)

Streaming tables ingesting from `ronguerrero.cpp_investment_ops`:

| Dataset | Source | Quality Checks (warn) |
|---------|--------|-----------------------|
| `bronze_trade_executions` | `raw_trade_executions` | valid IDs, positive qty/price, valid currency |
| `bronze_securities_master` | `raw_securities_master` | valid security_id, isin, name, asset_class |
| `bronze_market_data` | `raw_market_data` | valid IDs, positive prices, valid price range |
| `bronze_fx_rates` | `raw_fx_rates` | valid dates, currencies, positive rate |
| `bronze_counterparties` | `raw_counterparties` | valid IDs, name, LEI, credit_rating |
| `bronze_fund_structures` | `raw_fund_structures` | valid IDs, name, inception_date, target_return |

### Silver Layer (`silver_layer.sql`)

Cleaned and enriched data with `ON VIOLATION DROP ROW`:

| Dataset | Type | Description |
|---------|------|-------------|
| `silver_securities_master` | Materialized View | Cleaned securities dimension |
| `silver_counterparties` | Materialized View | Cleaned counterparties dimension |
| `silver_fund_structures` | Materialized View | Cleaned fund structures dimension |
| `silver_enriched_trades` | Streaming Table | Trades enriched with dimensions + notional_value |
| `silver_market_data` | Streaming Table | Prices with daily_return_pct |
| `silver_fx_rates` | Streaming Table | Valid positive FX rates only |

### Gold Layer (`gold_layer.sql`)

Business-level aggregations (all Materialized Views with liquid clustering):

| Dataset | Description |
|---------|-------------|
| `gold_portfolio_summary` | AUM, trade count, distinct securities per fund |
| `gold_daily_trading_activity` | Daily volumes/notional by asset class |
| `gold_counterparty_exposure` | Exposure by counterparty with buy/sell split |
| `gold_market_performance` | Return statistics per security |
| `gold_settlement_status` | Settlement tracking by fund and venue |

## Project Structure

```
CPP_SDP/
├── databricks.yml              # Bundle configuration
├── README.md                   # This file
└── transformations/
    ├── bronze_layer.sql        # Raw ingestion + quality monitoring
    ├── silver_layer.sql        # Cleaning, enrichment, DROP ROW checks
    └── gold_layer.sql          # Business aggregations
```

## Deployment

### Prerequisites

- Databricks CLI installed (`pip install databricks-cli`)
- Authenticated to the workspace (`databricks configure`)

### Deploy to Development

```bash
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
```

Development mode prefixes the pipeline name with `[dev <username>]` and enables development settings.

### Deploy to Production

```bash
databricks bundle validate --strict -t prod
databricks bundle deploy -t prod
```

### Run the Pipeline

```bash
# Trigger an update (incremental)
databricks bundle run cpp_sdp_pipeline -t dev

# Full refresh (reprocess all data from scratch)
databricks bundle run cpp_sdp_pipeline -t dev --full-refresh
```

## Configuration

| Setting | Value |
|---------|-------|
| Catalog | `ronguerrero` |
| Schema | `cpp_sdp_demo` |
| Compute | Serverless |
| Photon | Enabled |
| Mode | Triggered (not continuous) |

## Data Quality Strategy

- **Bronze**: Warn-only expectations — all records pass through, violations tracked as metrics
- **Silver**: `ON VIOLATION DROP ROW` — invalid records are rejected with full metric visibility
- **Gold**: No expectations needed — aggregations over clean silver data