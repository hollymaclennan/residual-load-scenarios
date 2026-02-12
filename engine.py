"""
Residual Load Scenario Engine v2

Computes:
    Residual Load = EQ Consumption - Wind - Solar

Data Sources:
    - Consumption: EQ data (PostgreSQL)
    - Wind & Solar: MetDesk forecasts (PostgreSQL)

Supports:
    1. Ensemble scenarios (member-by-member residual loads)
    2. Multi-model selection (eceps, ec46, gfsens, ecaifsens)
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Dict, List
import logging
import os

from config import (
    AVAILABLE_MODELS,
    METDESK_PERCENTILE_MEMBERS,
    METDESK_SPECIAL_MEMBERS,
    DATA_DIR,
    COUNTRY,
)
from metdesk_db import MetDeskDBClient

logger = logging.getLogger(__name__)


class ResidualLoadEngine:
    """Compute French residual load scenarios."""

    def __init__(self):
        self.metdesk = MetDeskDBClient()
        self.last_update: Optional[datetime] = None
        self.current_model: Optional[str] = None
        self.scenarios: Dict[str, pd.DataFrame] = {}

    def update(
        self,
        model: str = "eceps",
        issue: Optional[datetime] = None,
        countries: Optional[List[str]] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch latest forecasts and compute all scenario types.

        Args:
            model: NWP model to use for renewables ('eceps', 'ec46', 'gfsens', 'ecaifsens')
            issue: Specific issue time, or None for latest

        Returns dict with keys:
            - 'percentile_scenarios': crossed percentile residual load
            - 'ensemble_scenarios': per-member residual load + stats
            - 'demand': raw demand data
            - 'renewables_pct': renewable percentile data
            - 'renewables_ens': renewable ensemble data
            - 'metadata': info about the run
        """
        logger.info(f"Updating residual load scenarios (model={model}, issue={issue})")
        self.current_model = model

        # =================================================================
        # 1. FETCH RENEWABLE DATA FROM POSTGRESQL (MetDesk)
        # =================================================================
        logger.info(f"Step 1: Fetching renewable ensemble data from MetDesk for countries={countries}...")
        # default to configured country if none provided
        if countries is None or len(countries) == 0:
            countries = [COUNTRY]

        # fetch and aggregate renewable ensembles across selected countries
        ren_list = []
        for c in countries:
            df = self.metdesk.get_renewable_ensembles(model, issue, location=c.upper())
            if not df.empty:
                ren_list.append(df)

        if not ren_list:
            logger.error("FAILED: No renewable ensemble data for selected countries. Cannot compute residual loads.")
            return {}

        # aggregate by summing member columns across countries (align on utc_datetime)
        ren_agg = None
        for df in ren_list:
            df_idx = df.set_index("utc_datetime")
            if ren_agg is None:
                ren_agg = df_idx.copy()
            else:
                ren_agg = ren_agg.add(df_idx, fill_value=0)
        ren_ens = ren_agg.reset_index()

        # =================================================================
        # 2. FETCH CONSUMPTION DATA FROM EQ
        # =================================================================
        logger.info(f"Step 2: Fetching consumption data from EQ for countries={countries}...")
        # fetch and sum consumption across countries
        cons_list = []
        for c in countries:
            dfc = self.metdesk.get_eq_consumption(issue, location=c.lower())
            if not dfc.empty:
                cons_list.append(dfc)

        if not cons_list:
            logger.error("FAILED: No consumption data for selected countries. Cannot compute residual loads.")
            return {}

        # concat and sum by utc_datetime
        cons_concat = pd.concat(cons_list, ignore_index=True)
        consumption = (
            cons_concat.groupby("utc_datetime", as_index=False)["consumption_mw"].sum().sort_values("utc_datetime")
        )
        
        if consumption.empty:
            logger.error("FAILED: No consumption data. Cannot compute residual loads.")
            return {}

        # =================================================================
        # 3. COMPUTE RESIDUAL LOAD SCENARIOS
        # =================================================================
        logger.info("Step 3: Computing residual load scenarios...")
        residual_scenarios = self._compute_residual_scenarios(consumption, ren_ens, model)

        # =================================================================
        # 5. GET ISSUE TIME FOR METADATA
        # =================================================================
        # attempt to get an issue time for metadata (use first country)
        first_loc = countries[0].upper() if countries else COUNTRY.upper()
        actual_issue = self.metdesk._get_latest_issue(model, "wind", location=first_loc) if issue is None else issue

        # =================================================================
        # 6. STORE RESULTS
        # =================================================================
        self.scenarios = {
            "residual_scenarios": residual_scenarios,
            "consumption": consumption,
            "renewables_ens": ren_ens,
            "metadata": {
                "model": model,
                "model_label": AVAILABLE_MODELS[model]["label"],
                "issue": actual_issue,
                "updated_at": datetime.utcnow(),
                "n_members": AVAILABLE_MODELS[model]["n_members"],
                "countries": countries,
            },
        }
        self.last_update = datetime.utcnow()

        self._save_to_disk(model)
        logger.info(f"Scenarios updated at {self.last_update.strftime('%Y-%m-%d %H:%M UTC')}")
        return self.scenarios

    def _compute_residual_scenarios(
        self,
        consumption: pd.DataFrame,
        ren_ens: pd.DataFrame,
        model: str,
    ) -> pd.DataFrame:
        """
        Compute residual load for each ensemble member.

        Residual Load = Consumption (EQ) - Total Renewables (Wind + Solar)
        For each ensemble member: RL_i = Consumption - (Wind_i + Solar_i)
        """
        if consumption.empty:
            logger.warning("No consumption data available.")
            return pd.DataFrame()

        if ren_ens.empty:
            logger.warning("No renewable ensemble data available.")
            return pd.DataFrame()

        # Merge consumption and renewables by datetime
        merged = consumption.merge(ren_ens, on="utc_datetime", how="inner")

        if merged.empty:
            logger.warning("No overlapping data between consumption and renewables.")
            return pd.DataFrame()

        result = pd.DataFrame({"utc_datetime": merged["utc_datetime"]})

        n_members = AVAILABLE_MODELS[model]["n_members"]
        ens_cols = []

        # Compute residual load for each ensemble member
        for i in range(1, n_members + 1):
            ren_col = f"total_ren_ens_{i:02d}"
            if ren_col in merged.columns:
                # Residual Load = Consumption - Renewables
                result[f"residual_ens_{i:02d}"] = merged["consumption_mw"] - merged[ren_col]
                ens_cols.append(f"residual_ens_{i:02d}")

        # Compute ensemble statistics over all members
        if ens_cols:
            values = result[ens_cols].values
            result["ens_mean"] = np.nanmean(values, axis=1)
            result["ens_std"] = np.nanstd(values, axis=1)
            result["ens_min"] = np.nanmin(values, axis=1)
            result["ens_max"] = np.nanmax(values, axis=1)
            
            # Compute detailed percentiles (P0, P5, P10, ..., P95, P100)
            for p in range(0, 101, 5):
                result[f"ens_P{p}"] = np.nanpercentile(values, p, axis=1)

            logger.info(f"Computed residual load for {len(ens_cols)} ensemble members with percentiles P0-P100")
        else:
            logger.warning("No ensemble members found in renewable data.")

        return result

    def get_available_issues(self, model: str, location: Optional[str] = None) -> list:
        """Get available forecast issues for model selector (optionally per location)."""
        try:
            # Convert location to uppercase for MetDesk queries
            if location:
                location = location.upper()
            return self.metdesk.get_available_issues(model, location=location)
        except Exception as e:
            logger.error(f"Error fetching available issues for {model} (location={location}): {e}")
            return []

    def _save_to_disk(self, model: str):
        """Save latest scenarios to CSV."""
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M")
        for name, data in self.scenarios.items():
            if isinstance(data, pd.DataFrame) and not data.empty:
                path = os.path.join(DATA_DIR, f"{model}_{name}_{ts}.csv")
                data.to_csv(path, index=False)
                logger.info(f"Saved {name} -> {path}")

    def compute_forecast_delta(
        self,
        model: str,
        element: str,
        issue_new: datetime,
        issue_old: datetime,
        valid_start: datetime,
        valid_end: datetime,
        countries: Optional[List[str]] = None,
        location: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Compare forecasts from two different issues (runs) at the same valid times.
        Computes delta (change) in mean forecast.

        Args:
            model: forecast model (eceps, ec46, etc.)
            element: 'wind' or 'solar'
            issue_new: newer (current) forecast run
            issue_old: older (historical) forecast run
            valid_start, valid_end: time range to compare
            countries: list of country codes (for multi-country aggregation)
            location: single location code (if countries not provided)

        Returns:
            DataFrame with columns: [utc_datetime, old_mean, new_mean, delta, delta_pct]
        """
        if location is None and countries:
            location = countries[0]
        if location is None:
            location = COUNTRY

        location = location.upper()

        # Fetch forecasts for both issues
        old_df = self.metdesk.get_ensemble_by_issue_and_time(
            model, element, issue_old, valid_start, valid_end, location=location
        )
        new_df = self.metdesk.get_ensemble_by_issue_and_time(
            model, element, issue_new, valid_start, valid_end, location=location
        )

        if old_df.empty or new_df.empty:
            logger.warning(f"Missing data for {element} comparison: old={old_df.shape}, new={new_df.shape}")
            return pd.DataFrame()

        # Merge on utc_datetime
        comparison = pd.merge(
            old_df[["utc_datetime", "ens_mean"]],
            new_df[["utc_datetime", "ens_mean"]],
            on="utc_datetime",
            how="inner",
            suffixes=("_old", "_new"),
        )

        if comparison.empty:
            logger.warning("No overlapping times found between issues.")
            return pd.DataFrame()

        # Compute delta
        comparison["delta"] = comparison["ens_mean_new"] - comparison["ens_mean_old"]
        comparison["delta_pct"] = (comparison["delta"] / (comparison["ens_mean_old"] + 0.1)) * 100

        return comparison.rename(
            columns={"ens_mean_old": "old_mean", "ens_mean_new": "new_mean"}
        )

    def compute_residual_load_delta(
        self,
        model: str,
        issue_new: datetime,
        issue_old: datetime,
        valid_start: datetime,
        valid_end: datetime,
        countries: Optional[List[str]] = None,
        location: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Compare residual load forecasts from two different issues.
        Residual Delta = -(Wind Delta + Solar Delta)
        Combines wind and solar deltas to show how residual load forecast changed.

        Args:
            model: forecast model (eceps, ec46, etc.)
            issue_new: newer (current) forecast run
            issue_old: older (historical) forecast run
            valid_start, valid_end: time range to compare
            countries: list of country codes (for multi-country aggregation)
            location: single location code (if countries not provided)

        Returns:
            DataFrame with columns: [utc_datetime, wind_delta, solar_delta, residual_delta, residual_delta_pct]
        """
        if location is None and countries:
            location = countries[0]
        if location is None:
            location = COUNTRY

        # Get wind and solar deltas
        wind_delta = self.compute_forecast_delta(
            model, "wind", issue_new, issue_old, valid_start, valid_end, countries, location
        )
        solar_delta = self.compute_forecast_delta(
            model, "solar", issue_new, issue_old, valid_start, valid_end, countries, location
        )

        if wind_delta.empty and solar_delta.empty:
            logger.warning("Missing wind and solar data for residual delta computation")
            return pd.DataFrame()

        # Use outer merge to keep all times, fill missing with 0
        if not wind_delta.empty and not solar_delta.empty:
            result = pd.merge(
                wind_delta[["utc_datetime", "delta"]].rename(columns={"delta": "wind_delta"}),
                solar_delta[["utc_datetime", "delta"]].rename(columns={"delta": "solar_delta"}),
                on="utc_datetime",
                how="outer",
            )
        elif not wind_delta.empty:
            result = wind_delta[["utc_datetime", "delta"]].rename(columns={"delta": "wind_delta"}).copy()
            result["solar_delta"] = 0.0
        else:
            result = solar_delta[["utc_datetime", "delta"]].rename(columns={"delta": "solar_delta"}).copy()
            result["wind_delta"] = 0.0

        if result.empty:
            return pd.DataFrame()

        result = result.sort_values("utc_datetime").reset_index(drop=True)
        result["wind_delta"] = result["wind_delta"].fillna(0)
        result["solar_delta"] = result["solar_delta"].fillna(0)

        # Residual delta: negative because if wind+solar increase, residual load decreases
        result["residual_delta"] = -(result["wind_delta"] + result["solar_delta"])
        
        # Approximate percentage (using mean of old values)
        result["residual_delta_pct"] = (result["residual_delta"] / 100.0) * 100  # Normalize

        logger.info(f"Computed residual delta: {len(result)} points from {result['utc_datetime'].min()} to {result['utc_datetime'].max()}")
        return result
