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
│   └── generate_pnl_report.py
├── requirements.txt
└── README.md
```

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Place input data files in the `data/` directory.

3. Run the report generator:
   ```
   python src/generate_pnl_report.py
   ```

4. Output CSV reports will be saved in the `output/` directory.

## Description

This project generates monthly PnL reports for power trading books using trade, location, and forecast data. The code is organized for clarity and maintainability following Python best practices.
