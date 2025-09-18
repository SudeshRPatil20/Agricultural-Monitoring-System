# 🌾 Agricultural Sensor Data Pipeline

## 📌 Overview
This project implements a **production-grade data pipeline** for agricultural sensor data as part of the **SDE-2 Data Engineering Assignment**.  
The pipeline ingests, transforms, validates, and stores **sensor readings** (soil moisture, temperature, humidity, light intensity, battery levels) into a **clean, enriched, and query-optimized dataset** for downstream analytics.



## 📂 Project Structure
.
├── artifacts/ # Stores generated reports (e.g., data_quality_report.csv)
├── data/
│ ├── raw/ # Raw input parquet files (daily sensor data)
│ ├── processed/ # Processed + partitioned parquet files
├── notebook/ # Jupyter notebooks for experiments/debugging
├── src/ # Core pipeline code
│ ├── init.py
│ ├── components/ # Modular pipeline components
│ │ ├── data_ingestion.py
│ │ ├── data_transformation.py
│ │ ├── data_validation.py
│ │ └── data_loading.py
│ ├── logger.py # Logging utility
│ └── exception.py # Custom exception handling
├── requirements.txt # Python dependencies
├── setup.py # Package setup script
├── Dockerfile # Container setup for reproducibility
└── README.md # Project documentation

markdown
Copy code


## ⚙️ Features

### 🔹 Data Ingestion
- Reads **daily parquet files** from `data/raw/`.
- Supports **incremental loading** using file naming & timestamp checkpoints.
- Uses **DuckDB** for:
  - Schema inspection
  - Validation queries (e.g., missing columns, data ranges)
  - Ingestion statistics logging
- Handles:
  - Corrupt/unreadable files
  - Schema mismatches
  - Missing or invalid values

### 🔹 Data Transformation
- Cleans data:
  - Drops duplicates
  - Handles missing values
  - Detects & corrects outliers (z-score > 3)
- Enriches with derived fields:
  - Daily average per sensor & reading type
  - 7-day rolling average
  - `anomalous_reading = true/false`
  - Normalization (using calibration parameters)
- Timestamp standardization:
  - ISO 8601 format
  - Adjusted to **UTC+5:30**

### 🔹 Data Validation (DuckDB)
- Type & schema validation
- Value range checks per reading type
- Gap detection using `generate_series` (hourly coverage check)
- Profiling:
  - % missing values
  - % anomalies
  - Coverage gaps
- Exports results as:  
  📄 `artifacts/data_quality_report.csv`

### 🔹 Data Loading & Storage
- Stores cleaned data in **partitioned parquet format** under `data/processed/`
- Optimized for analytics:
  - Partitioned by `date` & `sensor_id`
  - Columnar format + compression (`snappy`)

---

## 🐳 Docker Setup
Build and run the pipeline inside a container:


# Build the image
docker build -t agri-pipeline .

# Run the container
docker run -it --rm -v $(pwd):/app agri-pipeline
▶️ Running the Pipeline
Local (Python)
bash
Copy code
# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python src/components/data_ingestion.py
python src/components/data_transformation.py
python src/components/data_validation.py
python src/components/data_loading.py
Docker
bash
Copy code
docker build -t agri-pipeline .
docker run -it --rm agri-pipeline
📝 Example Output
Processed Data: Stored at data/processed/

Validation Report: Generated at artifacts/data_quality_report.csv

📊 Sample Queries (DuckDB)
sql
Copy code
-- Inspect anomalies
SELECT * FROM read_parquet('data/processed/*/*.parquet')
WHERE anomalous_reading = TRUE;

-- Daily average temperature
SELECT date, sensor_id, AVG(value) AS avg_temp
FROM read_parquet('data/processed/*/*.parquet')
WHERE reading_type = 'temperature'
GROUP BY date, sensor_id;
🧪 Testing
Unit tests cover core logic:

Ingestion validation

Transformation (outlier detection, rolling average, normalization)

Validation (range checks, missing values, gaps)

Run tests:

bash
Copy code
pytest tests/
📦 Deliverables
✅ Modular, production-grade data pipeline
✅ Unit tests + exception handling
✅ Dockerized setup for reproducibility
✅ Example raw + processed data
✅ Data quality report