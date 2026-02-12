"""
Streamlit Dashboard for French Residual Load Scenarios v2

Run with: streamlit run dashboard.py
"""
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime
import logging

from engine import ResidualLoadEngine
from scheduler import ReforecastScheduler
from config import AVAILABLE_MODELS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# PAGE CONFIG
# =============================================================================
st.set_page_config(
    page_title="Residual Load Scenarios",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# SESSION STATE
# =============================================================================
if "engine" not in st.session_state:
    st.session_state.engine = ResidualLoadEngine()

if "scheduler" not in st.session_state:
    st.session_state.scheduler = ReforecastScheduler(
        update_callback=st.session_state.engine.update
    )

engine: ResidualLoadEngine = st.session_state.engine
scheduler: ReforecastScheduler = st.session_state.scheduler

# =============================================================================
# SIDEBAR
# =============================================================================
with st.sidebar:
    st.title("⚡ Control Panel")
    st.markdown("---")

    # ----- MODEL SELECTOR -----
    st.subheader("🌤️ NWP Model")
    model_options = {k: v["label"] for k, v in AVAILABLE_MODELS.items()}
    selected_model = st.selectbox(
        "Select forecast model",
        options=list(model_options.keys()),
        format_func=lambda x: model_options[x],
        index=0,
    )

    # Show model info
    model_info = AVAILABLE_MODELS[selected_model]
    st.caption(model_info["description"])

    # ----- ISSUE SELECTOR -----
    st.subheader("📅 Forecast Run")
    # ----- COUNTRY SELECTOR -----
    st.subheader("🇪🇺 Country Selector")
    country_options = {
        "fr": "France",
        "de": "Germany",
        "gb": "United Kingdom",
        "es": "Spain",
    }
    selected_countries = st.multiselect(
        "Select country or countries (sum across selected)",
        options=list(country_options.keys()),
        format_func=lambda x: country_options[x],
        default=["fr"],
    )

    try:
        # use first selected country (if any) to list available issues
        issue_location = selected_countries[0] if selected_countries else None
        available_issues = engine.get_available_issues(selected_model, location=issue_location)
        if available_issues:
            issue_options = ["Latest"] + [
                dt.strftime("%Y-%m-%d %H:%M UTC") if hasattr(dt, 'strftime') else str(dt)
                for dt in available_issues
            ]
            selected_issue_idx = st.selectbox(
                "Select issue time",
                options=range(len(issue_options)),
                format_func=lambda i: issue_options[i],
                index=0,
            )
            selected_issue = None if selected_issue_idx == 0 else available_issues[selected_issue_idx - 1]
        else:
            st.warning(f"Could not fetch available issues for {selected_model} ({issue_location}).")
            selected_issue = None
    except Exception as e:
        st.warning(f"DB error fetching issues: {e}")
        logger.error(f"Issue fetch error: {e}", exc_info=True)
        selected_issue = None

    st.markdown("---")

    # ----- REFRESH -----
    if st.button("🔄 Refresh Data", use_container_width=True, type="primary"):
        with st.spinner(f"Fetching {model_info['label']} data..."):
            try:
                # pass selected countries to engine (list of codes)
                engine.update(model=selected_model, issue=selected_issue, countries=selected_countries)
                st.success("Updated!")
            except Exception as e:
                st.error(f"Update failed: {e}")
                logger.error(f"Update error: {e}", exc_info=True)

    # Auto-refresh toggle
    auto_refresh = st.toggle("Auto-refresh (hourly)", value=False)
    if auto_refresh:
        if not hasattr(st.session_state, "_scheduler_running") or not st.session_state._scheduler_running:
            scheduler.start()
            st.session_state._scheduler_running = True
    else:
        if hasattr(st.session_state, "_scheduler_running") and st.session_state._scheduler_running:
            scheduler.stop()
            st.session_state._scheduler_running = False

    st.markdown("---")

    # ----- DISPLAY SETTINGS -----
    st.subheader("Display")
    show_individual_members = st.checkbox("Show individual ensemble members", value=False)
    chart_height = st.slider("Chart height (px)", 400, 800, 550)
    
    # Percentile selector
    st.subheader("Percentile Bands")
    available_percentiles = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100]
    default_percentiles = [10, 25, 50, 75, 90]
    selected_percentiles = st.multiselect(
        "Show percentile lines (P0-P100)",
        options=available_percentiles,
        default=default_percentiles,
        format_func=lambda x: f"P{x}"
    )

    # Separate percentile selectors for Wind and Solar (single-select)
    st.subheader("Wind / Solar Percentiles (for combined residual)")
    wind_pct = st.selectbox("Wind percentile", options=available_percentiles, index=available_percentiles.index(50), format_func=lambda x: f"P{x}")
    solar_pct = st.selectbox("Solar percentile", options=available_percentiles, index=available_percentiles.index(50), format_func=lambda x: f"P{x}")

    st.markdown("---")

    # ----- FORECAST COMPARISON -----
    st.subheader("📈 Forecast Comparison")
    enable_comparison = st.checkbox("Compare forecasts", value=False)
    
    if enable_comparison:
        # Fetch available issues for comparison
        try:
            historical_issues = engine.get_available_issues(selected_model, location=issue_location)
            if len(historical_issues) > 1:
                # Issue selector for comparison (pick an old one)
                issue_options_comp = [
                    dt.strftime("%Y-%m-%d %H:%M UTC") if hasattr(dt, 'strftime') else str(dt)
                    for dt in historical_issues[1:]  # Skip the latest
                ]
                if issue_options_comp:
                    selected_comp_idx = st.selectbox(
                        "Historical issue to compare",
                        options=range(len(issue_options_comp)),
                        format_func=lambda i: issue_options_comp[i],
                        key="comparison_issue_select",
                    )
                    comparison_issue = historical_issues[selected_comp_idx + 1]
                else:
                    comparison_issue = None
            else:
                st.info("Need at least 2 historical issues to compare.")
                comparison_issue = None
        except Exception as e:
            st.warning(f"Error loading historical issues: {e}")
            comparison_issue = None
    else:
        comparison_issue = None

    st.markdown("---")

    # ----- STATUS -----
    st.subheader("Status")
    meta = engine.scenarios.get("metadata", {})
    if meta:
        st.metric("Model", meta.get("model_label", "—"))
        issue_dt = meta.get("issue")
        st.metric("Issue", issue_dt.strftime("%Y-%m-%d %H:%M") if hasattr(issue_dt, 'strftime') else str(issue_dt) if issue_dt else "—")
        st.metric("Members", meta.get("n_members", "—"))
        updated = meta.get("updated_at")
        st.metric("Updated", updated.strftime("%H:%M UTC") if updated else "—")
    else:
        st.info("Click **Refresh Data** to load.")


# =============================================================================
# MAIN CONTENT
# =============================================================================
st.title("⚡ Residual Load Scenarios")

# Plotly template based on optional session state (no sidebar toggle)
template = "plotly_dark" if st.session_state.get("dark_mode", False) else "plotly_white"

meta = engine.scenarios.get("metadata", {})
if meta:
    st.caption(
        f"Model: **{meta.get('model_label', '')}** | "
        f"Issue: **{meta.get('issue', 'N/A')}** | "
        f"Countries: **{', '.join(meta.get('countries', ['fr']))}** | "
        f"Consumption: EQ Database | Renewables: MetDesk via PostgreSQL"
    )
else:
    st.caption("Consumption: EQ Database | Wind & Solar: MetDesk via PostgreSQL")

scenarios = engine.scenarios
if not scenarios or not any(
    isinstance(v, pd.DataFrame) and not v.empty for v in scenarios.values()
):
    st.info("👈 Select a model and click **Refresh Data** in the sidebar.")
    st.stop()


# =============================================================================
# TABS
# =============================================================================
tab_residual, tab_consumption, tab_renewables, tab_comparison, tab_data = st.tabs(
    ["📊 Residual Load Scenarios", "🔌 Consumption", "🌬️ Renewables", "📈 Forecast Comparison", "📋 Data"]
)

# --- TAB: RESIDUAL LOAD SCENARIOS ---
with tab_residual:
    residual = scenarios.get("residual_scenarios", pd.DataFrame())

    if not residual.empty:
        fig = go.Figure()

        # Define color palette for percentile lines
        colors = {
            0: "#ff0000", 5: "#ff3333", 10: "#ff6666", 15: "#ff9999", 20: "#ffcccc",
            25: "#0066ff", 30: "#3d7fb8", 35: "#2d5a8c", 40: "#1d3a60", 45: "#0d1a34",
            50: "#1f77b4", 55: "#6daed5", 60: "#9bc4db", 65: "#b5d4e5", 70: "#cfe5f0",
            75: "#00aa00", 80: "#33cc33", 85: "#66ff66", 90: "#99ff99", 95: "#ccffcc",
            100: "#00ff00"
        }
        
        # Add selected percentile lines
        for pct in sorted(selected_percentiles):
            col_name = f"ens_P{pct}"
            if col_name in residual.columns:
                is_bold = pct in [10, 25, 50, 75, 90]  # Highlight common percentiles
                fig.add_trace(go.Scatter(
                    x=residual["utc_datetime"],
                    y=residual[col_name],
                    mode="lines",
                    name=f"P{pct}",
                    line=dict(color=colors.get(pct, "#000000"), width=2.5 if is_bold else 1.5),
                    opacity=1.0 if is_bold else 0.7,
                ))

        # Mean line
        if "ens_mean" in residual.columns:
            fig.add_trace(go.Scatter(
                x=residual["utc_datetime"], y=residual["ens_mean"],
                mode="lines", name="Mean",
                line=dict(color="#ff7f0e", width=3, dash="dash"),
            ))

        # Percentile-based residual using selected wind/solar percentiles
        renewables = scenarios.get("renewables_ens", pd.DataFrame())
        consumption = scenarios.get("consumption", pd.DataFrame())
        try:
            if (not renewables.empty) and (not consumption.empty):
                # identify member columns for wind/solar
                wind_cols = [c for c in renewables.columns if c.startswith("wind_ens_")]
                solar_cols = [c for c in renewables.columns if c.startswith("solar_ens_")]

                if wind_cols and solar_cols:
                    # compute selected percentiles from ensemble members
                    wind_vals = renewables[wind_cols].values
                    solar_vals = renewables[solar_cols].values

                    wind_pct_series = np.nanpercentile(wind_vals, wind_pct, axis=1)
                    solar_pct_series = np.nanpercentile(solar_vals, solar_pct, axis=1)

                    pct_df = pd.DataFrame({
                        "utc_datetime": renewables["utc_datetime"],
                        f"wind_P{wind_pct}": wind_pct_series,
                        f"solar_P{solar_pct}": solar_pct_series,
                    })

                    merged_pct = consumption.merge(pct_df, on="utc_datetime", how="inner")
                    if not merged_pct.empty:
                        merged_pct["residual_pct"] = merged_pct["consumption_mw"] - (
                            merged_pct[f"wind_P{wind_pct}"] + merged_pct[f"solar_P{solar_pct}"]
                        )
                        fig.add_trace(go.Scatter(
                            x=merged_pct["utc_datetime"],
                            y=merged_pct["residual_pct"],
                            mode="lines",
                            name=f"Residual (Wind P{wind_pct} + Solar P{solar_pct})",
                            line=dict(color="#800080", width=3, dash="dot"),
                        ))
        except Exception:
            # don't break the dashboard if percentile calc fails
            logger.exception("Error computing percentile-based residual")

        fig.update_layout(
            title=f"Residual Load Scenarios — {meta.get('model_label', '')} Ensemble",
            xaxis_title="Time (UTC)",
            yaxis_title="Residual Load (MW)",
            template=template,
            height=chart_height,
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, xanchor="left",),
        )
        fig.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5)
        st.plotly_chart(fig, use_container_width=True)

        st.info(
            "💡 **Residual Load** = EQ Consumption − (Wind + Solar). "
            "Select percentiles from the sidebar to customize display."
        )
    else:
        st.warning("No residual load data available.")


# --- TAB: CONSUMPTION ---
with tab_consumption:
    consumption = scenarios.get("consumption", pd.DataFrame())

    if not consumption.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=consumption["utc_datetime"], y=consumption["consumption_mw"],
            mode="lines",
            name="EQ Consumption",
            line=dict(color="#d62728", width=2),
        ))

        fig.update_layout(
            title=f"Electricity Consumption ({meta.get('model_label', '')})",
            xaxis_title="Time (UTC)",
            yaxis_title="Consumption (MW)",
            template=template,
            height=chart_height,
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No consumption data available.")


# --- TAB: RENEWABLES ---
with tab_renewables:
    renewables = scenarios.get("renewables_ens", pd.DataFrame())

    if not renewables.empty:
        fig = go.Figure()

        ens_cols = [c for c in renewables.columns if c.startswith("total_ren_ens_")]

        # Individual members (spaghetti)
        if show_individual_members and ens_cols:
            for col in ens_cols:
                fig.add_trace(go.Scatter(
                    x=renewables["utc_datetime"], y=renewables[col],
                    mode="lines",
                    line=dict(color="rgba(100,200,100,0.2)", width=0.8),
                    showlegend=False, hoverinfo="skip",
                ))

        # Ensemble statistics
        if "ens_mean" in renewables.columns:
            fig.add_trace(go.Scatter(
                x=renewables["utc_datetime"], y=renewables["ens_mean"],
                mode="lines", name="Ensemble Mean",
                line=dict(color="#2ca02c", width=2.5),
            ))

        fig.update_layout(
            title=f"Total Renewable Generation ({meta.get('model_label', '')})",
            xaxis_title="Time (UTC)",
            yaxis_title="Generation (MW)",
            template=template,
            height=chart_height,
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        )
        # Add selected wind/solar percentile lines (computed from ensembles)
        try:
            renewables = scenarios.get("renewables_ens", pd.DataFrame())
            if not renewables.empty:
                wind_cols = [c for c in renewables.columns if c.startswith("wind_ens_")]
                solar_cols = [c for c in renewables.columns if c.startswith("solar_ens_")]
                if wind_cols:
                    wind_pct_series = np.nanpercentile(renewables[wind_cols].values, wind_pct, axis=1)
                    fig.add_trace(go.Scatter(
                        x=renewables["utc_datetime"], y=wind_pct_series,
                        mode="lines", name=f"Wind P{wind_pct}",
                        line=dict(color="#1f77b4", width=2, dash="dash"),
                    ))
                if solar_cols:
                    solar_pct_series = np.nanpercentile(renewables[solar_cols].values, solar_pct, axis=1)
                    fig.add_trace(go.Scatter(
                        x=renewables["utc_datetime"], y=solar_pct_series,
                        mode="lines", name=f"Solar P{solar_pct}",
                        line=dict(color="#ff7f0e", width=2, dash="dash"),
                    ))
        except Exception:
            logger.exception("Error adding wind/solar percentile lines")

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No renewable data available.")


# --- TAB: FORECAST COMPARISON ---
with tab_comparison:
    if enable_comparison and comparison_issue and selected_issue:
        st.subheader(f"Residual Load Forecast Comparison")
        st.caption(f"Current issue: {selected_issue.strftime('%Y-%m-%d %H:%M') if hasattr(selected_issue, 'strftime') else str(selected_issue)} | Historical: {comparison_issue.strftime('%Y-%m-%d %H:%M') if hasattr(comparison_issue, 'strftime') else str(comparison_issue)}")

        # Compute forecast horizon (valid times) as next 14 days to capture full forecast period
        now = pd.Timestamp.utcnow()
        valid_start = now.floor("h")
        valid_end = now + pd.Timedelta(days=14)

        try:
            # Compare residual load (combines wind + solar)
            residual_delta = engine.compute_residual_load_delta(
                model=selected_model,
                issue_new=selected_issue,
                issue_old=comparison_issue,
                valid_start=valid_start,
                valid_end=valid_end,
                location=issue_location,
            )

            if not residual_delta.empty:
                # Plot residual load delta
                fig = go.Figure()
                
                # Residual delta line
                fig.add_trace(go.Scatter(
                    x=residual_delta["utc_datetime"],
                    y=residual_delta["residual_delta"],
                    mode="lines+markers",
                    name="Residual Load Delta (MW)",
                    line=dict(color="#d62728", width=3),
                    marker=dict(size=6),
                ))
                
                # Add wind and solar component deltas as lighter traces
                fig.add_trace(go.Scatter(
                    x=residual_delta["utc_datetime"],
                    y=residual_delta["wind_delta"],
                    mode="lines",
                    name="Wind Δ (reference)",
                    line=dict(color="#1f77b4", width=1, dash="dot"),
                    opacity=0.5,
                ))
                
                fig.add_trace(go.Scatter(
                    x=residual_delta["utc_datetime"],
                    y=residual_delta["solar_delta"],
                    mode="lines",
                    name="Solar Δ (reference)",
                    line=dict(color="#ff7f0e", width=1, dash="dot"),
                    opacity=0.5,
                ))
                
                fig.add_hline(y=0, line_dash="solid", line_color="gray", opacity=0.3)
                fig.update_layout(
                    title="Residual Load Forecast Delta (Current - Historical)",
                    xaxis_title="Valid Time (UTC)",
                    yaxis_title="Delta (MW)",
                    template=template,
                    height=chart_height,
                    hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                )
                st.plotly_chart(fig, use_container_width=True)

                # Show statistics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric(
                        "Mean Residual Delta",
                        f"{residual_delta['residual_delta'].mean():.1f} MW",
                    )
                with col2:
                    st.metric(
                        "Max Delta",
                        f"{residual_delta['residual_delta'].max():.1f} MW",
                    )
                with col3:
                    st.metric(
                        "Min Delta",
                        f"{residual_delta['residual_delta'].min():.1f} MW",
                    )

                # Interpretation
                st.info(
                    "💡 **Interpretation**: Positive delta means residual load increased (higher system need). "
                    "Negative delta means residual load decreased (lower system need). "
                    "Deltas combine wind and solar forecast changes."
                )

                # Data table
                st.subheader("Delta Details")
                display_df = residual_delta[["utc_datetime", "residual_delta", "wind_delta", "solar_delta"]].copy()
                display_df.columns = ["Time (UTC)", "Residual Δ (MW)", "Wind Δ (MW)", "Solar Δ (MW)"]
                st.dataframe(display_df, use_container_width=True, height=300)

            else:
                st.warning("No overlapping forecast times found between the selected issues.")

        except Exception as e:
            st.error(f"Error computing residual load deltas: {e}")
            logger.exception("Residual load comparison error")
    else:
        if not enable_comparison:
            st.info("👈 Enable **Compare forecasts** in the sidebar to view how residual load forecasts changed between runs.")
        else:
            st.warning("Please select both a current issue and a historical issue to compare.")


# --- TAB: DATA ---
with tab_data:
    st.subheader("Raw Data Explorer")

    data_options = {
        "Residual Load Scenarios": "residual_scenarios",
        "Consumption": "consumption",
        "Renewable Ensembles": "renewables_ens",
    }

    selected_data = st.selectbox("Select dataset", list(data_options.keys()))
    df = scenarios.get(data_options[selected_data], pd.DataFrame())

    if isinstance(df, pd.DataFrame) and not df.empty:
        st.dataframe(df, use_container_width=True, height=400)

        csv = df.to_csv(index=False)
        st.download_button(
            label=f"📥 Download {selected_data} CSV",
            data=csv,
            file_name=f"fr_residual_load_{data_options[selected_data]}_{selected_model}.csv",
            mime="text/csv",
        )
    else:
        st.info("No data available.")


# =============================================================================
# FOOTER
# =============================================================================
st.markdown("---")
st.caption(
    f"Renewables: MetDesk via PostgreSQL | Demand: Volue Insight | "
    f"Model: {meta.get('model_label', 'N/A')} | "
    f"Updated: {meta.get('updated_at', datetime.min).strftime('%Y-%m-%d %H:%M UTC') if meta.get('updated_at') else 'N/A'}"
)
