# 🇫🇷 French Residual Load Scenario Builder v2

14-day hourly residual load scenarios for France with multi-model selection.

**Residual Load = Demand − Wind − Solar**

## Data Sources

| Component | Source | Access Method |
|-----------|--------|---------------|
| Wind generation | MetDesk | PostgreSQL (`silver.metdesk_forecasts`) |
| Solar generation | MetDesk | PostgreSQL (`silver.metdesk_forecasts`) |
| Demand forecasts | Volue Insight | REST API (OAuth2) |

## Available NWP Models

| Model | Label | Members | Horizon |
|-------|-------|---------|---------|
| `eceps` | ECMWF ENS | 50 | ~15 days |
| `ec46` | ECMWF Extended | 99 | ~46 days |
| `gfsens` | GFS Ensemble | 30 | ~16 days |
| `ecaifsens` | ECMWF AIFS ENS | 50 | ~15 days |

All models include pre-computed percentiles (0%, 10%, 25%, 40%, 60%, 75%, 90%, 100%) plus control, mean, and median.

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure credentials

Edit `.env` with your Volue credentials. PostgreSQL credentials are pre-filled.

### 3. Launch

```bash
streamlit run dashboard.py
```

### 4. In the sidebar:
- Select a **model** (eceps, ec46, gfsens, ecaifsens)
- Select an **issue time** (forecast run)
- Click **Refresh Data**

## Scenario Types

### Percentile Scenarios (Crossed)
Uses MetDesk pre-computed percentiles for renewables, crossed with Volue demand percentiles:
- **P90 Residual Load** = P90 demand − P10 renewables (worst case thermal)
- **P50 Residual Load** = Median demand − Median renewables (central)
- **P10 Residual Load** = P10 demand − P90 renewables (best case for renewables)

### Ensemble Scenarios
Individual member-by-member renewable generation with spread statistics (mean, std, min, max, P10/P50/P90).

## Project Structure

```
french_residual_load_v2/
├── config.py                  # DB connection, model configs, parameters
├── engine.py                  # Core residual load computation
├── scheduler.py               # Auto-update scheduler  
├── dashboard.py               # Streamlit + Plotly dashboard
├── data_sources/
│   ├── metdesk_db.py          # PostgreSQL client for MetDesk data
│   └── volue_client.py        # Volue Insight API client
├── data/                      # Cached CSV outputs
├── .env                       # Credentials (DO NOT COMMIT)
├── requirements.txt
└── README.md
```
