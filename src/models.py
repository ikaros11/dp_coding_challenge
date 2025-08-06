from dataclasses import dataclass
import pandas as pd

class ValidationError(Exception):
    pass

def _is_int_or_float_whole(series):
    # Accept int or float with all values whole numbers
    return pd.api.types.is_integer_dtype(series) or (
        pd.api.types.is_float_dtype(series) and (series.dropna() % 1 == 0).all()
    )

@dataclass
class VolumeRecord:
    DealNumber: int
    BuySell: str
    Date: pd.Timestamp
    Period: int
    Book: str
    LocationId: int
    Volume: float
    Price: float

    @staticmethod
    def validate_df(df: pd.DataFrame) -> None:
        required_types = {
            "DealNumber": "int",
            "BuySell": "object",
            "Date": "datetime64[ns]",
            "Period": "int",
            "Book": "object",
            "LocationId": "int",
            "Volume": "float",
            "Price": "float"
        }
        for col, typ in required_types.items():
            if col not in df.columns:
                raise ValidationError(f"Missing column in volumes: {col}")
            # Coerce types where possible
            if typ == "int":
                if not _is_int_or_float_whole(df[col]):
                    try:
                        df[col] = df[col].astype(int)
                    except Exception:
                        raise ValidationError(f"Column {col} must be integer or whole float type")
            if typ == "float":
                if not pd.api.types.is_float_dtype(df[col]):
                    try:
                        df[col] = df[col].astype(float)
                    except Exception:
                        raise ValidationError(f"Column {col} must be float type")
            if typ == "object" and not pd.api.types.is_object_dtype(df[col]):
                df[col] = df[col].astype(str)
            if typ.startswith("datetime") and not pd.api.types.is_datetime64_any_dtype(df[col]):
                try:
                    df[col] = pd.to_datetime(df[col])
                except Exception:
                    raise ValidationError(f"Column {col} must be datetime type")
        if not df["Period"].dropna().astype(int).between(1, 25).all():
            raise ValidationError("Period must be between 1 and 25")
        if not df["Volume"].ge(0).all():
            raise ValidationError("Volume must be non-negative")
        if not df["Price"].ge(0).all():
            raise ValidationError("Price must be non-negative")
        if not df["BuySell"].isin(["Buy", "Sell"]).all():
            raise ValidationError("BuySell must be 'Buy' or 'Sell'")

@dataclass
class LocationRecord:
    LocationId: int
    LocationName: str
    TimeZone: str

    @staticmethod
    def validate_df(df: pd.DataFrame) -> None:
        required_types = {
            "LocationId": "int",
            "LocationName": "object",
            "TimeZone": "object"
        }
        for col, typ in required_types.items():
            if col not in df.columns:
                raise ValidationError(f"Missing column in locations: {col}")
            if typ == "int" and not _is_int_or_float_whole(df[col]):
                raise ValidationError(f"Column {col} in locations must be integer or whole float type")
            if typ == "object" and not pd.api.types.is_object_dtype(df[col]):
                raise ValidationError(f"Column {col} in locations must be string type")

@dataclass
class ForecastRecord:
    Month: pd.Timestamp
    OffPeakPrice: float
    PeakPrice: float

    @staticmethod
    def validate_df(df: pd.DataFrame) -> None:
        required_types = {
            "Month": "datetime64[ns]",
            "OffPeakPrice": "float",
            "PeakPrice": "float"
        }
        for col, typ in required_types.items():
            if col not in df.columns:
                raise ValidationError(f"Missing column in forecast: {col}")
            if typ == "float" and not pd.api.types.is_float_dtype(df[col]):
                raise ValidationError(f"Column {col} in forecast must be float type")
            if typ.startswith("datetime") and not pd.api.types.is_datetime64_any_dtype(df[col]):
                raise ValidationError(f"Column {col} in forecast must be datetime type")
        if not df["OffPeakPrice"].ge(0).all():
            raise ValidationError("OffPeakPrice must be non-negative")
        if not df["PeakPrice"].ge(0).all():
            raise ValidationError("PeakPrice must be non-negative")
