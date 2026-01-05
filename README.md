# Airflow DAGs for Financial Data

This repo contains Airflow DAGs to collect and sync crypto, global stock, and Vietnam stock data into Postgres, plus a news and sentiment pipeline.

## Directory layout
```
/opt/airflow/dags
├─ dags/
│  ├─ crypto_dags/              # Crypto OHLCV sync and aggregation DAGs
│  ├─ global_stock_dags/         # Global stock news and prices DAGs
│  └─ vn_stock_dags/             # Vietnam stock data DAGs
├─ include/
│  └─ config/                    # YAML configs for DAGs
└─ plugins/
   ├─ operators/                 # Custom operators
   └─ utils/                     # Helpers: API, DB, config loader
```

## DAGs
### Crypto
| DAG ID | Purpose | Schedule (cron) |
| --- | --- | --- |
| `sync_crypto_ohlcv_3m_dag` | Sync 3m OHLCV from CCXT to Postgres (incremental + checkpoint) | `*/3 * * * *` |
| `aggregate_crypto_ohlcv_15m_dag` | Aggregate 3m OHLCV -> 15m | `1/15 * * * *` |
| `aggregate_crypto_ohlcv_1h_dag` | Aggregate 3m OHLCV -> 1h | `1 * * * *` |
| `aggregate_crypto_ohlcv_4h_dag` | Aggregate 3m OHLCV -> 4h | `1 0-23/4 * * *` |
| `aggregate_crypto_ohlcv_8h_dag` | Aggregate 3m OHLCV -> 8h | `1 0-23/8 * * *` |
| `aggregate_crypto_ohlcv_1d_dag` | Aggregate 3m OHLCV -> 1d | `1 0 * * *` |
| `aggregate_crypto_ohlcv_7d_dag` | Aggregate 3m OHLCV -> 7d | `1 0 * * 0` |
| `sync_cmc_global_metrics_dag` | Sync CoinMarketCap global metrics | `0 5 * * *` |

### Global stocks
| DAG ID | Purpose | Schedule (cron) |
| --- | --- | --- |
| `sync_marketaux_news_dag` | Fetch news from Marketaux (Postgres only; MongoDB optional) | `0 6 * * *` |
| `sync_global_eod_stock_prices_dag` | Sync EOD prices from EODHD | `0 3 * * *` |
| `daily_sentiment_evaluations_dag` | Evaluate news sentiment via OpenRouter | `0 8 * * *` |
| `daily_sentiment_aggregation_dag` | Aggregate daily sentiment | Manual trigger / from `daily_sentiment_evaluations_dag` |

### Vietnam stocks
| DAG ID | Purpose | Schedule (cron) |
| --- | --- | --- |
| `sync_stock_list` | Sync stock list (Wifeed) | `0 1 * * *` |
| `sync_stock_basic_info` | Sync basic info (Wifeed) | `0 3 * * *` |
| `sync_eod_vn_stock_prices_dag` | Sync EOD prices (Wifeed) | `0 3 * * *` |
| `sync_financial_indicators_non_financial` | Sync financial indicators (TTM) | `0 4 * * *` |

## Configuration
DAGs read YAML config from `include/config/*` via `plugins/utils/config_loader.py`:
- `include/config/crypto_configs/*.yml`
- `include/config/global_stock_configs/*.yml`
- `include/config/vn_stock_configs/*.yml`

Config includes:
- API endpoints, timeouts, throttling, query params.
- JSON to DB mapping, conflict keys, source/target tables.
- Schedules and lookback windows for aggregation jobs.

## Airflow Variables
Set these variables as needed for the DAGs you run:
- `openrouter_apikey`
- `marketaux_apikey`
- `marketaux_pause_until` (optional)
- `eodhd_apikey`
- `cmc_api_key`
- `wifeed_api_key`
- `mongo_uri` (only if MongoDB is enabled)

## Airflow Connections
- Default Postgres connection: `keynum-central-pg` (override in YAML if needed).

## Python dependencies
- `apache-airflow`
- `apache-airflow-providers-postgres`
- `requests`
- `psycopg2`
- `PyYAML`
- `ccxt` (crypto DAGs)
- `pymongo` (only if MongoDB is enabled)

## Quick run
1. Place the repo in Airflow DAGs folder (`/opt/airflow/dags`).
2. Configure Airflow Variables and Connections.
3. Enable DAGs in the UI and monitor logs.

## Notes
- OHLCV aggregation DAGs read their schedules from the corresponding YAML files.
- `daily_sentiment_evaluations_dag` triggers `daily_sentiment_aggregation_dag` after it completes.
