import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import os
from models import VolumeRecord, LocationRecord, ForecastRecord, ValidationError
import pathlib

# File paths
BASE_DIR = pathlib.Path(__file__).resolve().parent.parent
VOLUME_FILE = BASE_DIR / "data" / "power_volume_data.parquet"
LOCATION_FILE = BASE_DIR / "data" / "location_data.parquet"
FORECAST_FILE = BASE_DIR / "data" / "forecast_prices.csv"
OUTPUT_DIR = BASE_DIR / "output"

# Timezone constants
TZ_REPORT = "Europe/Zurich"

def load_data():
    volumes = pd.read_parquet(VOLUME_FILE)
    locations = pd.read_parquet(LOCATION_FILE)
    forecast = pd.read_csv(FORECAST_FILE, parse_dates=["Month"])
    # Validate and coerce types
    VolumeRecord.validate_df(volumes)
    LocationRecord.validate_df(locations)
    ForecastRecord.validate_df(forecast)
    return volumes, locations, forecast

def compute_period_datetime(volumes):
    def to_zh(row):
        dt = row["Date"] + pd.Timedelta(hours=int(row["Period"]) - 1)
        tz = row["TimeZone"] if pd.notnull(row["TimeZone"]) else TZ_REPORT
        return pytz.timezone(tz).localize(dt).astimezone(pytz.timezone(TZ_REPORT))
    return pd.to_datetime(volumes.apply(to_zh, axis=1))

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    try:
        volumes, locations, forecast = load_data()
    except ValidationError as e:
        print(f"Input validation error: {e}")
        return
    # Merge location info
    volumes = volumes.merge(locations, on="LocationId", how="left")
    # Fill missing or invalid time zones with Europe/Zurich
    volumes["TimeZone"] = volumes["TimeZone"].fillna(TZ_REPORT)
    # Compute period datetime in Europe/Zurich
    volumes["PeriodDateTime"] = compute_period_datetime(volumes)
    # Filter for 2025 in Europe/Zurich
    mask_2025 = (volumes["PeriodDateTime"] >= pd.Timestamp("2025-01-01", tz=TZ_REPORT)) & \
                (volumes["PeriodDateTime"] < pd.Timestamp("2026-01-01", tz=TZ_REPORT))
    volumes = volumes[mask_2025].copy()
    # Assign Month and Peak/OffPeak
    volumes["Month"] = volumes["PeriodDateTime"].dt.to_period("M").dt.to_timestamp()
    hour = volumes["PeriodDateTime"].dt.hour
    volumes["PeakOffPeak"] = np.where((hour >= 8) & (hour < 20), "Peak", "OffPeak")
    # Vectorized volume and trade value
    is_buy = volumes["BuySell"].str.lower() == "buy"
    volumes["AdjVolume"] = np.where(is_buy, volumes["Volume"], -volumes["Volume"])
    volumes["TradeValue"] = np.where(is_buy, -volumes["Price"] * volumes["Volume"], volumes["Price"] * volumes["Volume"])
    # Merge forecast prices
    volumes = volumes.merge(forecast, left_on="Month", right_on="Month", how="left")
    volumes["ForecastPrice"] = np.where(volumes["PeakOffPeak"] == "Peak", volumes["PeakPrice"], volumes["OffPeakPrice"])
    volumes["ForecastCost"] = volumes["ForecastPrice"].astype(float) * volumes["AdjVolume"].abs()
    # Group and aggregate
    group_cols = ["Month", "LocationName", "PeakOffPeak", "Book"]
    agg = volumes.groupby(group_cols, sort=False).agg(
        TotalVolume=("AdjVolume", "sum"),
        TotalTradeValue=("TradeValue", "sum"),
        ForecastCost=("ForecastCost", "sum")
    ).reset_index()
    agg["ForecastPnL"] = agg["TotalTradeValue"] - agg["ForecastCost"]
    # Output one CSV per Book
    for book, df_book in agg.groupby("Book"):
        out_file = OUTPUT_DIR / f"{book}_PnL_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df_book_out = df_book[["Month", "LocationName", "PeakOffPeak", "TotalVolume", "TotalTradeValue", "ForecastCost", "ForecastPnL"]]
        df_book_out.to_csv(out_file, index=False)
        print(f"Report generated: {out_file}")


if __name__ == "__main__":
    main()
