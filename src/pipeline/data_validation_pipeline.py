import sys
import pandas as pd
from src.exception import CustomException
from src.components.data_validation import DataValidation


class ValidationPipeline:
    """
    Runs the data validation checks on incoming sensor data.
    """

    def __init__(self):
        self.validator = DataValidation()

    def validate(self, features: pd.DataFrame):
        """
        Validate the provided sensor dataset.

        Args:
            features (pd.DataFrame): Input dataframe containing sensor data.

        Returns:
            dict: Validation report (summary of issues, anomalies, missing values etc.)
            pd.DataFrame: Any detected time gaps or anomaly details.
        """
        try:
            report, time_gaps = self.validator.validate(features)
            return report, time_gaps

        except Exception as e:
            raise CustomException(e, sys)


class CustomSensorData:
    """
    Helper class to create a DataFrame from raw sensor data inputs.
    """

    def __init__(
        self,
        sensor_id: str,
        timestamp: str,
        reading_type: str,
        value: float,
        battery_level: float,
    ):
        self.sensor_id = sensor_id
        self.timestamp = timestamp
        self.reading_type = reading_type
        self.value = value
        self.battery_level = battery_level

    def get_data_as_data_frame(self):
        """
        Convert input sensor data into a pandas DataFrame.
        """
        try:
            custom_data_input_dict = {
                "sensor_id": [self.sensor_id],
                "timestamp": [self.timestamp],
                "reading_type": [self.reading_type],
                "value": [self.value],
                "battery_level": [self.battery_level],
            }

            return pd.DataFrame(custom_data_input_dict)

        except Exception as e:
            raise CustomException(e, sys)
