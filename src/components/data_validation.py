import os
import sys
import duckdb
import pandas as pd
from dataclasses import dataclass
from src.exception import CustomException
from src.logger import logging


@dataclass
class DataValidationConfig:
    report_file_path: str = os.path.join("artifacts", "data_quality_report.csv")
    processed_data_dir: str = os.path.join("data", "processed")


class DataValidation:
    def __init__(self):
        self.config = DataValidationConfig()

    def run_data_quality_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Run data quality checks using DuckDB SQL
        """
        try:
            logging.info("Starting data quality validation using DuckDB")

            con = duckdb.connect()

            # Register dataframe as a DuckDB table
            con.register("sensor_data", df)

            # Validate types & schema
            type_check = con.execute("""
                SELECT 
                    COUNT(*) AS total_records,
                    SUM(CASE WHEN typeof(value) != 'DOUBLE' THEN 1 ELSE 0 END) AS invalid_value_type,
                    SUM(CASE WHEN TRY_CAST(timestamp AS TIMESTAMP) IS NULL THEN 1 ELSE 0 END) AS invalid_timestamps
                FROM sensor_data
            """).df()

            # Expected ranges per reading_type (example: temperature  -40 to 85, humidity 0–100)
            range_check = con.execute("""
                SELECT 
                    reading_type,
                    COUNT(*) AS total,
                    SUM(CASE 
                        WHEN reading_type = 'temperature' AND (value < -40 OR value > 85) THEN 1
                        WHEN reading_type = 'humidity' AND (value < 0 OR value > 100) THEN 1
                        ELSE 0 END
                    ) AS out_of_range
                FROM sensor_data
                GROUP BY reading_type
            """).df()

            # Missing values per column
            missing_check = con.execute("""
                SELECT 
                    'missing_values' AS check_type,
                    SUM(CASE WHEN sensor_id IS NULL THEN 1 ELSE 0 END) AS missing_sensor_id,
                    SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS missing_timestamp,
                    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS missing_value
                FROM sensor_data
            """).df()

            # % anomalous readings
            anomaly_pct = con.execute("""
                SELECT 
                    COUNT(*) AS total,
                    SUM(CASE WHEN value IS NULL OR value < -1000 OR value > 1000 THEN 1 ELSE 0 END) AS anomalies
                FROM sensor_data
            """).df()
            anomaly_pct["anomaly_pct"] = anomaly_pct["anomalies"] / anomaly_pct["total"] * 100

            # Gaps in hourly data (per sensor)
            coverage_gaps = con.execute("""
                WITH times AS (
                    SELECT MIN(timestamp)::DATE AS start_date, MAX(timestamp)::DATE AS end_date FROM sensor_data
                ),
                expected AS (
                    SELECT generate_series(start_date, end_date, interval 1 hour) AS ts
                    FROM times
                )
                SELECT s.sensor_id, COUNT(*) AS missing_hours
                FROM (SELECT DISTINCT sensor_id FROM sensor_data) s
                CROSS JOIN expected e
                LEFT JOIN sensor_data d 
                    ON s.sensor_id = d.sensor_id AND DATE_TRUNC('hour', d.timestamp) = e.ts
                WHERE d.sensor_id IS NULL
                GROUP BY s.sensor_id
            """).df()

            # Combine results into one report
            report = pd.concat([type_check, range_check, missing_check, anomaly_pct, coverage_gaps], axis=0)

            os.makedirs(os.path.dirname(self.config.report_file_path), exist_ok=True)
            report.to_csv(self.config.report_file_path, index=False)

            logging.info(f"Data quality report saved at {self.config.report_file_path}")

            return report

        except Exception as e:
            raise CustomException(e, sys)

    def save_processed_data(self, df: pd.DataFrame):
        """
        Save cleaned data to partitioned Parquet files
        """
        try:
            logging.info("Saving processed data in partitioned parquet format")

            os.makedirs(self.config.processed_data_dir, exist_ok=True)

            # Add partitioning columns
            df["date"] = pd.to_datetime(df["timestamp"]).dt.date.astype(str)

            # Write to Parquet, partitioned by date and sensor_id
            for date, g1 in df.groupby("date"):
                for sensor, g2 in g1.groupby("sensor_id"):
                    folder = os.path.join(self.config.processed_data_dir, f"date={date}", f"sensor_id={sensor}")
                    os.makedirs(folder, exist_ok=True)
                    file_path = os.path.join(folder, "data.parquet")
                    g2.to_parquet(file_path, index=False, compression="snappy")

            logging.info(f"Processed data saved under {self.config.processed_data_dir}")

        except Exception as e:
            raise CustomException(e, sys)
