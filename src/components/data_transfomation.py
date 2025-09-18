import sys
import pandas as pd
import numpy as np
from dataclasses import dataclass
from sklearn.preprocessing import StandardScaler

from src.exception import CustomException
from src.logger import logging
from src.utils import save_object


# ================================
# Config
# ================================
@dataclass
class TransformationConfig:
    preprocessor_path: str = "artifacts/preprocessor.pkl"
    calibration_params: dict = None
    input_timezone: str = "UTC"  # assume raw timestamps are in UTC


# ================================
# Data Transformation Component
# ================================
class DataTransformation:
    def __init__(self, cfg: TransformationConfig = TransformationConfig()):
        self.cfg = cfg
        if self.cfg.calibration_params is None:
            # Default calibration parameters
            self.cfg.calibration_params = {
                "temperature": {"multiplier": 1.02, "offset": -0.3},
                "humidity": {"multiplier": 0.99, "offset": 0.5},
                "soil_moisture": {"multiplier": 1.0, "offset": 0.0},
                "light": {"multiplier": 1.0, "offset": 0.0},
            }

    # --------------------------------
    # Step 1: Clean data
    # --------------------------------
    def drop_duplicates_and_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        before = len(df)
        df.drop_duplicates(inplace=True)
        df.dropna(
            subset=["sensor_id", "timestamp", "reading_type", "value", "battery_level"],
            inplace=True
        )
        after = len(df)
        logging.info(f"Dropped {before - after} duplicate/missing rows")
        return df

    # --------------------------------
    # Step 2: Outlier removal (z-score)
    # --------------------------------
    def remove_outliers_zscore(self, df: pd.DataFrame, col="value", z_thresh=3.0) -> pd.DataFrame:
        df = df.copy()
        if df[col].std() == 0 or pd.isna(df[col].std()):
            return df
        z = (df[col] - df[col].mean()) / df[col].std()
        kept = df[np.abs(z) <= z_thresh].copy()
        logging.info(f"Removed {len(df) - len(kept)} outliers by z-score > {z_thresh}")
        return kept

    # --------------------------------
    # Step 3: Calibration
    # --------------------------------
    def apply_calibration(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        def _calibrate(row):
            p = self.cfg.calibration_params.get(
                row["reading_type"], {"multiplier": 1.0, "offset": 0.0}
            )
            return row["value"] * p["multiplier"] + p["offset"]

        df["calibrated_value"] = df.apply(_calibrate, axis=1)
        return df

    # --------------------------------
    # Step 4: Derived fields
    # --------------------------------
    def add_derived(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df["date"] = df["timestamp"].dt.date

        # Daily average per sensor & type
        df["daily_avg"] = df.groupby(
            ["sensor_id", "reading_type", "date"]
        )["calibrated_value"].transform("mean")

        # 7-record rolling average
        df.sort_values(["sensor_id", "reading_type", "timestamp"], inplace=True)
        df["rolling_7"] = (
            df.groupby(["sensor_id", "reading_type"])["calibrated_value"]
            .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
        )

        # Expected ranges per type
        ranges = {
            "temperature": (-40.0, 85.0),
            "humidity": (0.0, 100.0),
            "soil_moisture": (0.0, 100.0),
            "light": (0.0, 200000.0),
        }

        def _is_anom(row):
            lo, hi = ranges.get(row["reading_type"], (float("-inf"), float("inf")))
            return not (lo <= row["calibrated_value"] <= hi)

        df["anomalous_reading"] = df.apply(_is_anom, axis=1)
        return df

    # --------------------------------
    # Step 5: Timezone & ISO formatting
    # --------------------------------
    def tz_convert_and_iso(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        ts = pd.to_datetime(df["timestamp"], errors="coerce")

        try:
            ts = ts.dt.tz_localize(self.cfg.input_timezone).dt.tz_convert("Asia/Kolkata")
        except TypeError:
            ts = ts.dt.tz_convert("Asia/Kolkata")

        df["timestamp"] = ts
        df["timestamp_iso"] = ts.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        return df

    # --------------------------------
    # Step 6: Scaling numeric columns
    # --------------------------------
    def scale_numeric(self, df: pd.DataFrame, numeric_cols=None) -> pd.DataFrame:
        if numeric_cols is None:
            numeric_cols = ["calibrated_value", "battery_level", "daily_avg", "rolling_7"]

        df = df.copy()
        scaler = StandardScaler()
        df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

        scaled = scaler.fit_transform(df[numeric_cols])
        scaled_cols = [col + "_scaled" for col in numeric_cols]
        df[scaled_cols] = scaled

        # Save preprocessor
        save_object(self.cfg.preprocessor_path, scaler)
        logging.info(f"Scaler saved to {self.cfg.preprocessor_path}")
        return df

    # --------------------------------
    # Full Transformation Pipeline
    # --------------------------------
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df = self.drop_duplicates_and_missing(df)
            df = self.remove_outliers_zscore(df, col="value", z_thresh=3.0)
            df = self.apply_calibration(df)
            df = self.add_derived(df)
            df = self.tz_convert_and_iso(df)
            df = self.scale_numeric(df)

            logging.info(f"Transformation complete. Final shape: {df.shape}")
            return df
        except Exception as e:
            logging.error("Error during transformation", exc_info=True)
            raise CustomException(e, sys)
