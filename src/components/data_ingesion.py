import os
import sys
import pandas as pd
import duckdb
from dataclasses import dataclass
from sklearn.model_selection import train_test_split

from src.exception import CustomException
from src.logger import logging


# ================================
# 1. Config
# ================================
@dataclass
class DataIngestionConfig:
    raw_data_path: str = os.path.join('artifacts', "raw_data.parquet")
    train_data_path: str = os.path.join('artifacts', "train.parquet")
    test_data_path: str = os.path.join('artifacts', "test.parquet")


# ================================
# 2. Ingestion Component
# ================================
class DataIngestion:
    def __init__(self):
        self.config = DataIngestionConfig()

    def initiate_data_ingestion(self, file_path=r"D:/sudesh/aerospace endto end/notebook/data/sample_sensor_data.parquet"):
        logging.info("🚀 Starting sensor data ingestion...")

        try:
            # Step 1: Read parquet
            logging.info(f"Reading parquet file from {file_path}")
            df = pd.read_parquet(file_path)
            logging.info(f"✅ Loaded dataset with shape {df.shape}")

            # Step 2: Schema validation with DuckDB
            con = duckdb.connect(database=':memory:')
            con.register("df", df)
            schema = con.execute("DESCRIBE df").df()
            logging.info(f"Schema:\n{schema}")

            # Step 3: Data quality checks
            validation = con.execute("""
                SELECT 
                    SUM(sensor_id IS NULL) AS missing_sensor_id,
                    SUM(timestamp IS NULL) AS missing_timestamp,
                    SUM(reading_type IS NULL) AS missing_reading_type,
                    SUM(value IS NULL) AS missing_value,
                    SUM(battery_level < 0 OR battery_level > 100) AS invalid_battery,
                    COUNT(*) AS total_records
                FROM df
            """).df()
            logging.info(f"Validation Results:\n{validation}")

            # Step 4: Save raw parquet
            os.makedirs(os.path.dirname(self.config.raw_data_path), exist_ok=True)
            df.to_parquet(self.config.raw_data_path, index=False, compression="snappy")
            logging.info(f"Raw data saved at {self.config.raw_data_path}")

            # Step 5: Train/Test split
            train_set, test_set = train_test_split(df, test_size=0.2, random_state=42)
            train_set.to_parquet(self.config.train_data_path, index=False, compression="snappy")
            test_set.to_parquet(self.config.test_data_path, index=False, compression="snappy")

            logging.info("✅ Train/Test data saved successfully.")

            return (
                self.config.train_data_path,
                self.config.test_data_path
            )

        except Exception as e:
            raise CustomException(e, sys)


# ================================
# 3. Run Ingestion
# ================================
if __name__ == "__main__":
    obj = DataIngestion()
    train_data, test_data = obj.initiate_data_ingestion()
    logging.info(f"🏁 Ingestion complete. Train: {train_data}, Test: {test_data}")
