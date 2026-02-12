"""
Volue Insight (Wattsight) API client for French demand forecasts.

Fetches:
- Deterministic demand forecast
- Ensemble demand forecast (for probabilistic scenarios)
"""
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional
import logging

from config import (
    VOLUE_CLIENT_ID,
    VOLUE_CLIENT_SECRET,
    VOLUE_TOKEN_URL,
    VOLUE_BASE_URL,
    VOLUE_CURVES,
    FORECAST_HORIZON_DAYS,
)

logger = logging.getLogger(__name__)


class VolueInsightClient:
    """Client for Volue Insight / Wattsight API."""

    def __init__(self):
        self.base_url = VOLUE_BASE_URL
        self.token = None
        self.token_expiry = None

    def _authenticate(self):
        """Get OAuth2 token from Volue Insight."""
        if self.token and self.token_expiry and datetime.utcnow() < self.token_expiry:
            return

        logger.info("Authenticating with Volue Insight...")
        response = requests.post(
            VOLUE_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": VOLUE_CLIENT_ID,
                "client_secret": VOLUE_CLIENT_SECRET,
            },
        )
        response.raise_for_status()
        data = response.json()
        self.token = data["access_token"]
        self.token_expiry = datetime.utcnow() + timedelta(
            seconds=data.get("expires_in", 3600) - 60
        )
        logger.info("Authenticated successfully.")

    def _headers(self):
        self._authenticate()
        return {"Authorization": f"Bearer {self.token}"}

    def _get_curve_id(self, curve_name: str) -> int:
        """Search for a curve by name and return its ID."""
        response = requests.get(
            f"{self.base_url}/curves",
            headers=self._headers(),
            params={"query": curve_name},
        )
        response.raise_for_status()
        curves = response.json()
        if not curves:
            raise ValueError(f"No curve found matching: {curve_name}")
        for curve in curves:
            if curve["name"].lower() == curve_name.lower():
                return curve["id"]
        logger.warning(f"No exact match for '{curve_name}', using: {curves[0]['name']}")
        return curves[0]["id"]

    def get_demand_forecast(self, issue_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Fetch the latest deterministic demand forecast for France.

        Returns:
            DataFrame with columns: [utc_datetime, demand_mw]
        """
        curve_name = VOLUE_CURVES["demand_forecast"]
        curve_id = self._get_curve_id(curve_name)

        now = datetime.utcnow()
        end = now + timedelta(days=FORECAST_HORIZON_DAYS)

        params = {
            "from": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if issue_date:
            params["issue_date"] = issue_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        response = requests.get(
            f"{self.base_url}/instances/{curve_id}/latest",
            headers=self._headers(),
            params=params,
        )
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(data.get("points", data))
        if df.empty:
            logger.warning("No demand forecast data returned.")
            return pd.DataFrame(columns=["utc_datetime", "demand_mw"])

        df = self._normalize_timeseries(df, value_col="demand_mw")
        logger.info(f"Fetched demand forecast: {len(df)} hourly points")
        return df

    def get_demand_ensembles(self, issue_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Fetch ensemble demand forecast for France.

        Returns:
            DataFrame with columns: [utc_datetime, ens_01, ens_02, ..., ens_51]
        """
        curve_name = VOLUE_CURVES["demand_ensemble"]
        curve_id = self._get_curve_id(curve_name)

        now = datetime.utcnow()
        end = now + timedelta(days=FORECAST_HORIZON_DAYS)

        params = {
            "from": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if issue_date:
            params["issue_date"] = issue_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        response = requests.get(
            f"{self.base_url}/instances/{curve_id}/latest",
            headers=self._headers(),
            params=params,
        )
        response.raise_for_status()
        data = response.json()

        df = self._parse_ensemble_response(data)
        logger.info(f"Fetched demand ensembles: {df.shape}")
        return df

    def get_demand_percentiles(
        self, percentiles: list = [10, 25, 50, 75, 90], issue_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """Compute percentile scenarios from ensemble demand forecast."""
        ensembles = self.get_demand_ensembles(issue_date)
        if ensembles.empty:
            return pd.DataFrame()

        ens_cols = [c for c in ensembles.columns if c.startswith("ens_")]
        result = pd.DataFrame({"utc_datetime": ensembles["utc_datetime"]})

        for p in percentiles:
            result[f"demand_{p}%"] = np.percentile(
                ensembles[ens_cols].values, p, axis=1
            )
        # Add mean and median
        result["demand_mean"] = ensembles[ens_cols].mean(axis=1)
        result["demand_median"] = ensembles[ens_cols].median(axis=1)

        return result

    def _normalize_timeseries(self, df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        """Normalize API response into clean timestamp + value DataFrame."""
        if "t" in df.columns:
            df = df.rename(columns={"t": "utc_datetime", "v": value_col})
        elif "time" in df.columns:
            df = df.rename(columns={"time": "utc_datetime", "value": value_col})
        df["utc_datetime"] = pd.to_datetime(df["utc_datetime"], utc=True)
        df = df.sort_values("utc_datetime").reset_index(drop=True)
        return df[["utc_datetime", value_col]]

    def _parse_ensemble_response(self, data: dict) -> pd.DataFrame:
        """Parse ensemble API response into wide-format DataFrame."""
        records = []
        points = data if isinstance(data, list) else data.get("points", [])

        for point in points:
            row = {"utc_datetime": point.get("t") or point.get("time")}
            scenarios = point.get("scenarios") or point.get("values", [])
            for i, val in enumerate(scenarios):
                row[f"ens_{i+1:02d}"] = val
            records.append(row)

        df = pd.DataFrame(records)
        if not df.empty:
            df["utc_datetime"] = pd.to_datetime(df["utc_datetime"], utc=True)
            df = df.sort_values("utc_datetime").reset_index(drop=True)
        return df
