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
        Run data quality checks using DuckDB SQL and return clean pivoted report.
        """
        try:
            logging.info("Starting data quality validation using DuckDB")
            con = duckdb.connect()
            con.register("sensor_data", df)

            results = []

            # -------------------------------
            # 1. Type & schema validation
            # -------------------------------
            type_check = con.execute("""
                SELECT 
                    COUNT(*) AS total_records,
                    SUM(CASE WHEN typeof(value) != 'DOUBLE' THEN 1 ELSE 0 END) AS invalid_value_type,
                    SUM(CASE WHEN TRY_CAST(timestamp AS TIMESTAMP) IS NULL THEN 1 ELSE 0 END) AS invalid_timestamps
                FROM sensor_data
            """).fetchdf().iloc[0].to_dict()
            results.append({"check_type": "type_check", **type_check})

            # -------------------------------
            # 2. Range validation
            # -------------------------------
            range_check = con.execute("""
                SELECT 
                    reading_type,
                    SUM(CASE 
                        WHEN reading_type = 'temperature' AND (value < -40 OR value > 85) THEN 1
                        WHEN reading_type = 'humidity' AND (value < 0 OR value > 100) THEN 1
                        ELSE 0 END
                    ) AS out_of_range
                FROM sensor_data
                GROUP BY reading_type
            """).fetchdf()
            for _, row in range_check.iterrows():
                results.append({
                    "check_type": f"range_check_{row['reading_type']}",
                    "out_of_range": row["out_of_range"]
                })

            # -------------------------------
            # 3. Missing values
            # -------------------------------
            missing_check = con.execute("""
                SELECT 
                    SUM(CASE WHEN sensor_id IS NULL THEN 1 ELSE 0 END) AS missing_sensor_id,
                    SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS missing_timestamp,
                    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS missing_value
                FROM sensor_data
            """).fetchdf().iloc[0].to_dict()
            results.append({"check_type": "missing_check", **missing_check})

            # -------------------------------
            # 4. Anomaly percentage
            # -------------------------------
            anomaly_pct = con.execute("""
                SELECT 
                    COUNT(*) AS total,
                    SUM(CASE WHEN value IS NULL OR value < -1000 OR value > 1000 THEN 1 ELSE 0 END) AS anomalies
                FROM sensor_data
            """).fetchdf().iloc[0].to_dict()
            anomaly_pct["anomaly_pct"] = (
                (anomaly_pct["anomalies"] / anomaly_pct["total"]) * 100
                if anomaly_pct["total"] > 0 else 0
            )
            results.append({"check_type": "anomaly_pct", **anomaly_pct})

            # -------------------------------
            # 5. Coverage gaps
            # -------------------------------
            coverage_gaps = con.execute("""
                WITH times AS (
                    SELECT MIN(timestamp)::DATE AS start_date, MAX(timestamp)::DATE AS end_date 
                    FROM sensor_data
                ),
                expected AS (
                    SELECT ts::TIMESTAMP AS ts
                    FROM times, 
                    LATERAL generate_series(start_date, end_date, INTERVAL 1 hour) AS t(ts)
                )
                SELECT s.sensor_id, COUNT(*) AS missing_hours
                FROM (SELECT DISTINCT sensor_id FROM sensor_data) s
                CROSS JOIN expected e
                LEFT JOIN sensor_data d 
                    ON s.sensor_id = d.sensor_id AND DATE_TRUNC('hour', d.timestamp) = e.ts
                WHERE d.sensor_id IS NULL
                GROUP BY s.sensor_id
            """).fetchdf()
            total_gaps = int(coverage_gaps["missing_hours"].sum()) if not coverage_gaps.empty else 0
            results.append({"check_type": "coverage_gaps", "total_gaps": total_gaps})

            # -------------------------------
            # Final Report
            # -------------------------------
            report = pd.DataFrame(results)

            os.makedirs(os.path.dirname(self.config.report_file_path), exist_ok=True)
            report.to_csv(self.config.report_file_path, index=False)
            logging.info(f"Data quality report saved at {self.config.report_file_path}")

            return report

        except Exception as e:
            raise CustomException(e, sys)

    def save_processed_data(self, df: pd.DataFrame):
        """
        Save cleaned data to partitioned Parquet files (partitioned by date & sensor_id).
        """
        try:
            logging.info("Saving processed data in partitioned parquet format")

            os.makedirs(self.config.processed_data_dir, exist_ok=True)

            df["date"] = pd.to_datetime(df["timestamp"], errors="coerce").dt.date.astype(str)

            # ✅ Write Parquet with partitioning
            df.to_parquet(
                self.config.processed_data_dir,
                partition_cols=["date", "sensor_id"],
                index=False,
                compression="snappy"
            )

            logging.info(f"Processed data saved under {self.config.processed_data_dir}")

        except Exception as e:
            raise CustomException(e, sys)


if __name__ == "__main__":
    try:
        # Example: load the sample parquet
        file_path = r"D:/sudesh/aerospace endto end/notebook/data/sample_sensor_data.parquet"
        df = pd.read_parquet(file_path)

        validator = DataValidation()
        report = validator.run_data_quality_checks(df)
        print("Validation Report:\n", report)

        # Save cleaned/processed parquet
        validator.save_processed_data(df)

    except Exception as e:
        raise CustomException(e, sys)
