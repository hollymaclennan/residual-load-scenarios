
@echo off
cd /d "%~dp0"
echo Starting Dashboard...
py -m streamlit run dashboard.py
pause
SELECT location, MIN(utc_datetime) as earliest, MAX(utc_datetime) as latest, COUNT(*) as record_count
FROM silver.metdesk_forecasts
GROUP BY location
ORDER BY location;
