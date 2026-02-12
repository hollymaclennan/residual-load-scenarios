"""
Configuration for French Residual Load Scenario Builder
"""
import os
from dotenv import load_dotenv

load_dotenv()


def _get_secret(key: str, default: str) -> str:
    """Read from env first, then Streamlit secrets if available."""
    if key in os.environ:
        return os.environ[key]
    try:
        import streamlit as st

        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return default

# =============================================================================
# DATABASE (PostgreSQL - Azure)
# =============================================================================
DB_HOST = _get_secret("DB_HOST", "psql-analytics-uk-gwc.postgres.database.azure.com")
DB_PORT = _get_secret("DB_PORT", "5432")
DB_NAME = _get_secret("DB_NAME", "psql-analytics-uk-gwc")
DB_USER = _get_secret("DB_USER", "analytics_viewer")
DB_PASSWORD = _get_secret("DB_PASSWORD", "")
DB_SSLMODE = _get_secret("DB_SSLMODE", "require")

DB_CONNECTION_STRING = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    f"?sslmode={DB_SSLMODE}"
)

# =============================================================================
# VOLUE INSIGHT / WATTSIGHT (for demand forecasts)
# =============================================================================
VOLUE_CLIENT_ID = os.getenv("VOLUE_CLIENT_ID", "YOUR_CLIENT_ID")
VOLUE_CLIENT_SECRET = os.getenv("VOLUE_CLIENT_SECRET", "YOUR_CLIENT_SECRET")
VOLUE_TOKEN_URL = "https://auth.volueinsight.com/oauth2/token"
VOLUE_BASE_URL = "https://api.volueinsight.com/api"

# Volue curve names for France — adjust to your subscription
VOLUE_CURVES = {
    "demand_forecast": "pro fr demand mwh/h cet h f",
    "demand_ensemble": "pro fr demand mwh/h cet h ec00ens",
}

# =============================================================================
# METDESK (from PostgreSQL)
# =============================================================================
METDESK_TABLE = "silver.metdesk_forecasts"
COUNTRY = "FR"

# Available ensemble models for selection
AVAILABLE_MODELS = {
    "eceps": {
        "label": "ECMWF ENS (eceps)",
        "description": "ECMWF Ensemble - 50 members, 15-day horizon",
        "n_members": 50,
    },
    "ec46": {
        "label": "ECMWF Extended (ec46)",
        "description": "ECMWF Extended Range - 99 members, 46-day horizon",
        "n_members": 99,
    },
    "gfsens": {
        "label": "GFS Ensemble (gfsens)",
        "description": "NCEP GFS Ensemble - 30 members, 16-day horizon",
        "n_members": 30,
    },
    "ecaifsens": {
        "label": "ECMWF AIFS ENS (ecaifsens)",
        "description": "ECMWF AI-based Ensemble - 50 members, 15-day horizon",
        "n_members": 50,
    },
}

# Pre-computed percentile members available in MetDesk data
METDESK_PERCENTILE_MEMBERS = ["0%", "10%", "25%", "40%", "60%", "75%", "90%", "100%"]
METDESK_SPECIAL_MEMBERS = ["control", "mean", "median"]

# =============================================================================
# FORECAST PARAMETERS
# =============================================================================
FORECAST_HORIZON_DAYS = 14

# Percentiles for residual load scenarios (crossed)
PERCENTILES_FOR_CROSSING = {
    "P90_RL": {"demand": "90%", "renewables": "10%"},   # High residual load
    "P75_RL": {"demand": "75%", "renewables": "25%"},
    "P50_RL": {"demand": "median", "renewables": "median"},  # Central
    "P25_RL": {"demand": "25%", "renewables": "75%"},
    "P10_RL": {"demand": "10%", "renewables": "90%"},   # Low residual load
}

# =============================================================================
# SCHEDULING
# =============================================================================
REFORECAST_TIMES_UTC = ["06:00", "12:00", "18:00"]
REFRESH_INTERVAL_MINUTES = 60

# =============================================================================
# OUTPUT
# =============================================================================
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)
