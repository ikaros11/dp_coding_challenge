# Power Trading PnL Report

## Project Structure

```
.
├── data/
│   ├── power_volume_data.parquet
│   ├── location_data.parquet
│   └── forecast_prices.csv
├── output/
│   └── [PnL CSV reports]
├── src/
│   ├── generate_pnl_report.py
│   └── models.py
├── requirements.txt
├── README.md
└── CHALLENGE.md
```

## Setup

1. Ensure you have Python 3.8+ and pip installed.
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Place the input data files in the `data/` directory:
   - `power_volume_data.parquet`
   - `location_data.parquet`
   - `forecast_prices.csv`

## Usage

Run the report generator from the project root:
```
python src/generate_pnl_report.py
```
Output CSV reports for each trading book will be saved in the `output/` directory.

## Implementation Details

- **Data Validation & Schema Enforcement:**  
  All input files are validated and coerced to the expected schema using the logic in `src/models.py`. This ensures correct types, required columns, and value ranges before any processing.

- **Business Logic:**  
  The main script (`src/generate_pnl_report.py`) merges trade, location, and forecast data. It computes the correct datetime for each trade period, adjusts for time zones, and classifies each hour as Peak or OffPeak according to the challenge rules.

- **Aggregation:**  
  The script aggregates data by Month, Location, Peak/OffPeak, and Book, computing:
  - TotalVolume (sign-adjusted by Buy/Sell)
  - TotalTradeValue (price * volume, sign-adjusted)
  - ForecastCost (using forecast prices)
  - ForecastPnL (TotalTradeValue - ForecastCost)

- **Output:**  
  One CSV file per Book is generated in the `output/` directory, with columns:
  - Month, LocationName, PeakOffPeak, TotalVolume, TotalTradeValue, ForecastCost, ForecastPnL

- **Error Handling:**  
  Any schema or value errors in the input data are reported and halt execution, ensuring data integrity.

## Challenge Reference

See `CHALLENGE.md` for the original challenge specification and requirements.
