# Data Engineer Coding Challenge
## Challenge Specification
You have been tasked to create a forecasted Profit & Loss (PnL) report for calendar year 2025
for the power trading desk based upon the current trading position and forecasted market
prices. 

### Source Data
The trading data team has provided trade position data (each row is for a one hour period) for all trades that cover the reporting
period along with additional reference data to help generate the report. The data is provided as a DuckDB (https://duckdb.org/) 
database file. The individual tables are also supplied as separate parquet files if DuckDB cannot be used.

The trade volume data is given in the `volumes` table (`power_volume_data.parquet` file) and the schema is:

| Column | Type | Description |
| ------ | ---- | ----------- |
| DealNumber | integer | The unique deal number for the trade |
| BuySell | text | Buy/Sell flag |
| Date | date | The date in the locations time zone the period is for |
| Period | int | The one hour period since midnight in the locations time zone (1 based) |
| Book | text | The trading book that holds the trade |
| LocationId | int | The ID of the physical location of the trade. Joins to location table |
| Volume | decimal | The power volume (MWh) for the period. Always positive but the direction is determined by the Buy/Sell flag |
| Price | decimal | The fixed price for the trade in Euro/MWh |

Notes:
* A single trade will have multiple rows as it will span one or more days
* The Buy/Sell flag will determine the direction of power volume and the direction of cash flow. A `Buy` will mean +ve volume and -ve cash flow. `Sell` -ve volume and +ve cash flow.
* The Date for the row is the local date for the period for the trades location/time zone (see below for location description)
* Period is 1 based one hour offset from midnight for the Date. For example if Date is 2025-01-01 and period is 1, the period is the hour starting 2025-01-01 00:00. Period 2 is is the hour starting 2025-01-01 01:00 etc.
* Each deal has a single fixed price but the price has been given for each period row to support reporting

The location reference data is given in the `locations` table (`location_data.parquet` file) and the schema is:

| Column | Type | Description |
| ------ | ---- | ----------- |
| LocationId | integer | Primary key for the location |
| LocationName | text | The name of the location |
| TimeZone | text | The IANA name of the time zone for the location |

Notes:
* While the IANA time zone name is given, it is acceptable to assume Central European Time (CET) for all mainland European locations
and Western European Time (WET) for Europe/London if this makes the report development easier. This not a requirement though.

In addition to the data supplied by the trading data team, the price forecasting team have supplied a price forecast file for prices 
for calendar year 2025. Prices are given per month for peak and off peak periods. The schema for the data is:

| Column | Type | Description |
| ------ | ---- | ----------- |
| Month | date | The first date of the month the prices are applicable for in ISO format `YYYY-MM-DD` |
| OffPeakPrice | decimal | The off peak periods price in Euro/MWh |
| PeakPrice | decimal | The peak periods price in Euro/MWh |

Notes:
* The time zone for the prices is Europe/Zurich (CET) and therefore data must be converted to this time zone to join
* Off peak prices apply to all one hour periods from 20:00 (8pm) until 08:00 (8am) the following day (Europe/Zurich time zone)
* Peak prices apply to all one hour periods from 08:00 (8am) until 20:00 (8pm) (Europe/Zurich time zone)
* The end times for the peak/off peak windows are exclusive. I.e the one hour period beginning at 08:00 is in the peak window, not the off peak.

### Report Requirements
A separate CSV file for each book should be generated. The CSV file should contain records with the following structure:

| Column | Type | Description |
| ------ | ---- | ----------- |
| Month | date | The first date of the month in ISO format `YYYY-MM-DD` |
| LocationName | text | The name of the location (taken from locations table above) |
| PeakOffPeak | text | Peak/Off Peak flag. `Peak` or `OffPeak` |
| TotalVolume | decimal | The sum of volume (MWh) (considering Buy/Sell sign adjustment) for all periods in the Month/Peak/Off Peak band |
| TotalTradeValue | decimal | The sum of the values (Euro) (see below for definition) for all trades in the applicable periods |
| ForecastCost | decimal | The forecasted cost (Euro) of the trades given the forecasted prices |
| ForecastPnL | decimal | The expected PnL (TotalTradeValue - ForecastCost) (Euro) |

Notes:
* Output file name should indicate the Book that it relates to and the time that the report was run
* All data should be aggregated so that there is a single row for each Month, Location Name, Peak/Off Peak combination.
* As with the price forecast data, the Month date is required to be in Europe/Zurich time zone
* The peak/off peak definition is the same as for the price forecast file
* `TotalVolume` is the sum of all volumes that fall in the Book/Location/Month/Peak or OffPeak window. The sign of the volume must be adjusted based upon the Buy/Sell flag as described above.
* A trades value is defined as the sum of price * volume for each period of the trade. The resulting cash flow should be sign adjusted as defined above.
* Cost is the trade value if market prices are used. The difference between the value of a trade using the trades price and the market price determines the PnL.
* Periods that fall outside of the calendar year 2025 in the Europe/Zurich time zone should be discarded

Example data:
Using the following pseudo trade volume data:

| BuySell | Date | Period | Book | Location | Volume | Price |
| ------  | ---- | ------ | ---- | -------- | ------ | ----- |
| Sell    | 2025-01-01 | 1 | Spec | Spain | 100 | 50 |
| Sell    | 2025-01-01 | 2 | Spec | Spain | 110 | 50 |

And price forecast data:

| Month | OffPeakPrice | PeakPrice |
| ----- | ------------ | --------- |
| 2025-01-01 | 40 | 60 |

The times for the two period in Europe/Zurich (CET) are 2025-01-01 00:00 and 2025-01-01 01:00. Both these times fall within the Off Peak band.
As this is a Sell trade, the total trade volume is **-100 + -110 = -210 MWh**. The trade value is **(100 * 50) + (110 * 50) = 10,500 Euro**.
The forecast cost using the forecast price is **210 * 40 = 8,400 Euro**. This gives a forecast PnL of **10,500 - 8,400 = 2,100 Euro**.

This should result in an output row in the Spec book CSV file:

| Month | LocationName | PeakOffPeak | TotalVolume | TotalTradeValue | ForecastCost | ForecastPnL |
| ----- | ------------ | ----------- | ----------- | --------------- | ------------ | ----------- |
| 2025-01-01 | Spain | OffPeak | -210 | 10500 | 8400 | 2100 |

### Additional Requirements
* While this challenge can potentially be implemented using many different approaches, we require it to be implemented using python. 
It is allowable to push a `SELECT` query down to DuckDB/Parquet with joins and aggregations, but don't build a data pipeline (There should be no `INSERT`s). 
But feel free to bring other implementation ideas to the subsequent interview.
* The code for you solution should be submitted along with the generated report files. Please submit in an accesible source code repository (Github/Bitbucket for example) or as cloud based file share (OneDrive/Google Drive for example).
* Your solution should show how you would approach this requirement in an enterprise environment but does not need to be "production" ready.
