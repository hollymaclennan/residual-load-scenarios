"""
PostgreSQL client for MetDesk wind and solar forecasts.

Pulls data from silver.metdesk_forecasts table.
Supports:
- Pre-computed percentiles (0%, 10%, 25%, 40%, 60%, 75%, 90%, 100%)
- Individual ensemble members
- Special members (control, mean, median)
"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from typing import Optional, List
import logging

from config import (
    DB_CONNECTION_STRING,
    METDESK_TABLE,
    COUNTRY,
    AVAILABLE_MODELS,
    METDESK_PERCENTILE_MEMBERS,
    METDESK_SPECIAL_MEMBERS,
    FORECAST_HORIZON_DAYS,
)

logger = logging.getLogger(__name__)


class MetDeskDBClient:
    """Client for MetDesk data stored in PostgreSQL."""

    def __init__(self):
        self._engine = None

    @property
    def engine(self):
        if self._engine is None:
            # Add connection pool settings (timeout handled via statement_timeout)
            self._engine = create_engine(
                DB_CONNECTION_STRING,
                pool_size=3,
                max_overflow=5,
                pool_pre_ping=True,
                pool_recycle=3600,
            )
        return self._engine

    def _get_latest_issue(self, model: str, element: str, location: Optional[str] = None) -> Optional[datetime]:
        """Get the most recent issue (forecast run) time for a model/element for a location."""
        if location is None:
            location = COUNTRY

        query = text(f"""
            SELECT MAX(issue) as latest_issue
            FROM {METDESK_TABLE}
            WHERE location = :location
              AND model = :model
              AND element = :element
            LIMIT 1
        """)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    query, {"location": location, "model": model, "element": element}
                )
                row = result.fetchone()
                if row and row[0]:
                    logger.info(f"Latest issue for {model}/{element} ({location}): {row[0]}")
                    return row[0]
                else:
                    logger.warning(f"No data found for {model}/{element}/{location}")
                    return None
        except Exception as e:
            logger.error(f"Error getting latest issue: {e}")
            return None

    # =========================================================================
    # PERCENTILE FORECASTS (pre-computed by MetDesk)
    # =========================================================================

    def get_percentile_forecasts(
        self,
        model: str,
        element: str,
        issue: Optional[datetime] = None,
        location: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Fetch pre-computed percentile forecasts from MetDesk.

        Returns:
            DataFrame with columns: [utc_datetime, 0%, 10%, 25%, 40%, 60%, 75%, 90%, 100%, mean, median, control]
            Pivoted wide: one column per percentile/special member.
        """
        if issue is None:
            issue = self._get_latest_issue(model, element)
            if issue is None:
                logger.warning(f"No data found for {model}/{element}")
                return pd.DataFrame()

        members = METDESK_PERCENTILE_MEMBERS + METDESK_SPECIAL_MEMBERS

        if location is None:
            location = COUNTRY

        query = text(f"""
            SELECT utc_datetime, member, value
            FROM {METDESK_TABLE}
            WHERE location = :location
              AND model = :model
              AND element = :element
              AND issue = :issue
              AND member = ANY(:members)
            ORDER BY utc_datetime, member
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(
                query,
                conn,
                params={
                    "location": location,
                    "model": model,
                    "element": element,
                    "issue": issue,
                    "members": members,
                },
            )

        if df.empty:
            logger.warning(f"No percentile data for {model}/{element} issue={issue}")
            return pd.DataFrame()

        # Pivot: rows=utc_datetime, columns=member, values=value
        pivot = df.pivot_table(
            index="utc_datetime", columns="member", values="value", aggfunc="first"
        ).reset_index()
        pivot["utc_datetime"] = pd.to_datetime(pivot["utc_datetime"], utc=True)
        pivot = pivot.sort_values("utc_datetime").reset_index(drop=True)

        logger.info(
            f"Fetched {element} percentiles ({model}): {len(pivot)} hours, issue={issue}"
        )
        return pivot

    # =========================================================================
    # ENSEMBLE MEMBER FORECASTS
    # =========================================================================

    def get_ensemble_forecasts(
        self,
        model: str,
        element: str,
        issue: Optional[datetime] = None,
        location: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Fetch individual ensemble member forecasts.

        Returns:
            DataFrame with columns: [utc_datetime, ens_1, ens_2, ..., ens_N]
            Pivoted wide: one column per ensemble member.
        """
        if location is None:
            location = COUNTRY

        if issue is None:
            issue = self._get_latest_issue(model, element)
            if issue is None:
                logger.warning(f"No data found for {model}/{element}/{COUNTRY}")
                return pd.DataFrame()
        n_members = AVAILABLE_MODELS[model]["n_members"]
        # Numeric members only (exclude percentile and special members)
        numeric_members = [str(i) for i in range(1, n_members + 1)]

        query = text(f"""
            SELECT utc_datetime, member, value
            FROM {METDESK_TABLE}
            WHERE location = :location
              AND model = :model
              AND element = :element
              AND issue = :issue
              AND member = ANY(:members)
              AND utc_datetime >= CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '2 days'
            ORDER BY utc_datetime, member
            LIMIT 50000
        """)

        try:
            logger.info(f"Fetching {element} ensemble ({model}) from issue {issue}...")
            with self.engine.connect() as conn:
                df = pd.read_sql(
                    query,
                    conn,
                    params={
                        "location": location,
                        "model": model,
                        "element": element,
                        "issue": issue,
                        "members": numeric_members,
                    },
                )
        except Exception as e:
            logger.error(f"Error fetching ensemble forecasts: {e}")
            return pd.DataFrame()

        if df.empty:
            logger.warning(f"No ensemble data for {model}/{element} issue={issue}")
            return pd.DataFrame()

        # Pivot wide: one column per member
        pivot = df.pivot_table(
            index="utc_datetime", columns="member", values="value", aggfunc="first"
        ).reset_index()

        # Rename columns: "1" -> "ens_01", "2" -> "ens_02", etc.
        rename_map = {}
        for col in pivot.columns:
            if col != "utc_datetime":
                try:
                    rename_map[col] = f"ens_{int(col):02d}"
                except ValueError:
                    rename_map[col] = f"ens_{col}"
        pivot = pivot.rename(columns=rename_map)

        pivot["utc_datetime"] = pd.to_datetime(pivot["utc_datetime"], utc=True)
        pivot = pivot.sort_values("utc_datetime").reset_index(drop=True)

        logger.info(
            f"\u2713 Fetched {element} ensembles ({model}): {pivot.shape[0]} hours x {len([c for c in pivot.columns if c.startswith('ens_')])} members"
        )
        return pivot

    # =========================================================================
    # CONVENIENCE: COMBINED WIND + SOLAR
    # =========================================================================

    def get_renewable_percentiles(
        self, model: str, issue: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Fetch wind + solar percentile forecasts and combine.

        Returns DataFrame with columns like:
            [utc_datetime, wind_0%, wind_10%, ..., solar_0%, solar_10%, ..., total_ren_mean, ...]
        """
        wind = self.get_percentile_forecasts(model, "wind", issue)
        solar = self.get_percentile_forecasts(model, "solar", issue)

        if wind.empty and solar.empty:
            return pd.DataFrame()

        # Prefix columns
        if not wind.empty:
            wind = wind.rename(
                columns={c: f"wind_{c}" for c in wind.columns if c != "utc_datetime"}
            )
        if not solar.empty:
            solar = solar.rename(
                columns={c: f"solar_{c}" for c in solar.columns if c != "utc_datetime"}
            )

        if wind.empty:
            return solar
        if solar.empty:
            return wind

        df = wind.merge(solar, on="utc_datetime", how="outer").fillna(0)

        # Add total renewables for key percentiles
        for pct in METDESK_PERCENTILE_MEMBERS + METDESK_SPECIAL_MEMBERS:
            w_col = f"wind_{pct}"
            s_col = f"solar_{pct}"
            if w_col in df.columns and s_col in df.columns:
                df[f"total_ren_{pct}"] = df[w_col] + df[s_col]

        return df

    def get_renewable_ensembles(
        self, model: str, issue: Optional[datetime] = None
    , location: Optional[str] = None) -> pd.DataFrame:
        """
        Fetch wind + solar ensemble members and sum them.

        Returns DataFrame with columns:
            [utc_datetime, wind_ens_01, ..., solar_ens_01, ..., total_ren_ens_01, ...]
        """
        logger.info(f"Fetching renewable ensembles for {model}...")
        wind = self.get_ensemble_forecasts(model, "wind", issue, location=location)
        solar = self.get_ensemble_forecasts(model, "solar", issue, location=location)

        if wind.empty and solar.empty:
            logger.error(f"No wind or solar ensemble data available for {model}")
            return pd.DataFrame()
        
        if wind.empty:
            logger.warning(f"No wind ensemble data, using solar only")
        if solar.empty:
            logger.warning(f"No solar ensemble data, using wind only")

        # Prefix columns
        if not wind.empty:
            wind_renamed = wind.rename(
                columns={c: f"wind_{c}" for c in wind.columns if c != "utc_datetime"}
            )
        else:
            wind_renamed = pd.DataFrame()

        if not solar.empty:
            solar_renamed = solar.rename(
                columns={c: f"solar_{c}" for c in solar.columns if c != "utc_datetime"}
            )
        else:
            solar_renamed = pd.DataFrame()

        if wind_renamed.empty:
            logger.info("Returning solar ensembles only")
            return solar_renamed
        if solar_renamed.empty:
            logger.info("Returning wind ensembles only")
            return wind_renamed

        df = wind_renamed.merge(solar_renamed, on="utc_datetime", how="outer").fillna(0)

        # Sum wind + solar per member
        n_members = AVAILABLE_MODELS[model]["n_members"]
        total_ren_cols = []
        for i in range(1, n_members + 1):
            w_col = f"wind_ens_{i:02d}"
            s_col = f"solar_ens_{i:02d}"
            if w_col in df.columns and s_col in df.columns:
                df[f"total_ren_ens_{i:02d}"] = df[w_col] + df[s_col]
                total_ren_cols.append(f"total_ren_ens_{i:02d}")

        logger.info(f"\u2713 Renewable ensembles ready: {df.shape[0]} hours, {len(total_ren_cols)} members")
        return df

    # =========================================================================
    # AVAILABLE ISSUES (for UI dropdown)
    # =========================================================================

    def get_available_issues(
        self, model: str, element: str = "wind", n_latest: int = 10, location: Optional[str] = None
    ) -> List[datetime]:
        """Get the N most recent issue times for a model."""
        if location is None:
            location = COUNTRY
        query = text(f"""
            SELECT DISTINCT issue
            FROM {METDESK_TABLE}
            WHERE location = :location
              AND model = :model
              AND element = :element
            ORDER BY issue DESC
            LIMIT :n
        """)
        with self.engine.connect() as conn:
            result = conn.execute(
                query,
                {"location": location, "model": model, "element": element, "n": n_latest},
            )
            return [row[0] for row in result.fetchall()]

    def get_ensemble_by_issue_and_time(
        self,
        model: str,
        element: str,
        issue: datetime,
        valid_start: datetime,
        valid_end: datetime,
        location: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Fetch ensemble member data for a specific issue and valid time range.
        Used for comparing forecasts from different issues at the same valid time.

        Returns:
            DataFrame with columns: [utc_datetime, ens_00, ens_01, ..., ens_mean]
        """
        if location is None:
            location = COUNTRY

        query = text(f"""
            SELECT utc_datetime, member, value
            FROM {METDESK_TABLE}
            WHERE location = :location
              AND model = :model
              AND element = :element
              AND issue = :issue
              AND utc_datetime >= :valid_start
              AND utc_datetime <= :valid_end
            ORDER BY utc_datetime, member
            LIMIT 50000
        """)

        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(
                    query,
                    conn,
                    params={
                        "location": location,
                        "model": model,
                        "element": element,
                        "issue": issue,
                        "valid_start": valid_start,
                        "valid_end": valid_end,
                    },
                )

            if df.empty:
                logger.warning(f"No {element} data for {model} issue {issue}")
                return pd.DataFrame()

            # Pivot to wide format (utc_datetime x members)
            pivot = df.pivot_table(
                index="utc_datetime", columns="member", values="value", aggfunc="first"
            )
            pivot = pivot.reset_index()

            # Rename member columns to ens_00, ens_01, etc.
            member_cols = [c for c in pivot.columns if c != "utc_datetime"]
            for col in member_cols:
                pivot.rename(columns={col: f"ens_{col}"}, inplace=True)

            # Add mean
            ens_cols = [c for c in pivot.columns if c.startswith("ens_")]
            if ens_cols:
                pivot["ens_mean"] = pivot[ens_cols].mean(axis=1)

            logger.info(f"Fetched {element} for issue {issue}: {pivot.shape[0]} times")
            return pivot

        except Exception as e:
            logger.error(f"Error fetching ensemble by issue/time: {e}")
            return pd.DataFrame()

    # =========================================================================
    # EQ CONSUMPTION DATA (from SQL database)
    # =========================================================================

    def get_eq_consumption(
        self, issue: Optional[datetime] = None, n_latest_days: int = 14, location: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Fetch EQ consumption data from the database.
        Uses consumption_fcst_latest (latest forecast) or falls back to actual consumption.

        Returns:
            DataFrame with columns: [utc_datetime, consumption_mw]
        """
        if location is None:
            location = 'fr'

        query = text("""
            SELECT utc_datetime, 
                   COALESCE(consumption_fcst_latest, consumption_act) as consumption_mw
            FROM silver.eq_consumption
            WHERE LOWER(country) = :location
              AND utc_datetime >= CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '2 days'
            ORDER BY utc_datetime DESC
            LIMIT 500
        """)

        try:
            logger.info(f"Fetching EQ consumption data for {location}...")
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"location": location})
            
            if df.empty:
                logger.warning("No EQ consumption data found for FR.")
                return pd.DataFrame(columns=["utc_datetime", "consumption_mw"])

            df["utc_datetime"] = pd.to_datetime(df["utc_datetime"], utc=True)
            df = df.sort_values("utc_datetime").reset_index(drop=True)
            logger.info(f"✓ Fetched EQ consumption: {len(df)} hourly points ({df['utc_datetime'].min()} to {df['utc_datetime'].max()})")
            return df

        except Exception as e:
            logger.error(f"Error fetching EQ consumption: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame(columns=["utc_datetime", "consumption_mw"])
