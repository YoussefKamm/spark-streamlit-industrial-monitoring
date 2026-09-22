# Industrial Machine Monitoring Dashboard  
### Apache Spark · PySpark · Streamlit

A hands-on data engineering and visualization project for processing industrial machine data with **Apache Spark** and presenting analytical insights through an interactive **Streamlit dashboard**.

The project demonstrates how Spark can be used as the data-processing layer for cleaning, joining, filtering, and aggregating sensor and maintenance data, while Streamlit provides the interactive application layer.

> This project was developed locally in VS Code as a practical exercise to strengthen my understanding of Apache Spark, PySpark, and Streamlit.

---

## Project Overview

Industrial machines continuously generate sensor measurements such as temperature, pressure, vibration, and energy consumption.

The objective of this project is to build a small industrial monitoring application capable of:

- processing machine and sensor data with PySpark,
- cleaning and enriching multiple datasets,
- calculating operational KPIs,
- identifying warnings and critical sensor events,
- analyzing machine health and maintenance activity,
- and presenting the results through an interactive Streamlit dashboard.

The overall architecture is:

```text
Raw CSV Data
     │
     ▼
Apache Spark / PySpark
     │
     ├── Data Cleaning
     ├── Filtering
     ├── Joins
     ├── Aggregations
     └── KPI Calculation
     │
     ▼
Small Analytical Results
     │
     ▼
Pandas
     │
     ▼
Streamlit Dashboard
```

Spark performs the data processing, while Streamlit handles user interaction and visualization.

---

## Dataset

The project uses three synthetic datasets designed for Spark and Streamlit practice.

### `machines.csv`

Contains metadata for **24 industrial machines**.

Example information:

```text
machine_id
factory
production_line
model
install_date
criticality
```

---

### `sensor_readings.csv`

Contains **5,040 sensor readings** collected over time.

Example information:

```text
timestamp
machine_id
temperature_c
pressure_bar
vibration_mm_s
energy_kwh
status
```

The dataset intentionally contains:

- warning and critical measurements,
- anomalous sensor values,
- and a small number of missing values,

in order to practice data preprocessing and monitoring logic.

---

### `maintenance_events.csv`

Contains **124 maintenance events**.

Example information:

```text
machine_id
event_date
maintenance_type
downtime_minutes
maintenance_cost
technician
issue
```

---

## Data Relationships

The datasets are connected through `machine_id`.

```text
sensor_readings.machine_id
            │
            ▼
     machines.machine_id

maintenance_events.machine_id
            │
            ▼
     machines.machine_id
```

For example, sensor measurements are enriched with machine metadata using a Spark join:

```python
sensor_enriched = sensor_df.join(
    machines_df,
    on="machine_id",
    how="left"
)
```

This allows sensor measurements to be analyzed using additional dimensions such as:

```text
Factory
Production line
Machine model
Criticality
```

---

## Dashboard Features

The Streamlit application provides an interactive monitoring dashboard with several filtering dimensions.

### Filters

Users can filter the data by:

```text
Factory
Production line
Machine model
Machine
Sensor status
Date range
```

### KPIs

The dashboard calculates indicators including:

```text
Number of machines
Total sensor readings
Warning events
Critical events
Alert rate
Average temperature
Average vibration
Maintenance downtime
Maintenance cost
```

### Visualizations

The application includes visualizations for:

```text
Machine health distribution
Temperature trends
Vibration trends
Temperature by machine
Vibration by machine
Warnings and critical events
Sensor status distribution
Energy consumption
Maintenance cost by type
Downtime by factory
Maintenance history
```

---

## Application Architecture

The project is divided into three main Python modules.

```text
spark-streamlit-industrial-monitoring/
│
├── app.py
│
├── data_processing.py
├── spark_utils.py
│
├── data/
│   ├── machines.csv
│   ├── sensor_readings.csv
│   └── maintenance_events.csv
│
├── requirements.txt
├── .gitignore
└── README.md
```

### `spark_utils.py`

Responsible for creating and reusing the Spark session.

```python
@st.cache_resource
def get_spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("AdvancedSparkDashboard")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark
```

The Spark session is cached because Streamlit reruns the application whenever the user interacts with the dashboard.

Creating a new Spark session for every interaction would be unnecessary and expensive.

---

### `data_processing.py`

Contains the backend data-processing logic.

Its responsibilities include:

```text
Loading datasets
Cleaning sensor data
Cleaning machine metadata
Cleaning maintenance data
Joining datasets
Applying dashboard filters
Calculating KPIs
Preparing chart datasets
Computing machine health indicators
```

The majority of data manipulation is performed using Spark DataFrames.

Example:

```python
machine_performance = (
    sensor_df
    .groupBy("machine_id")
    .agg(
        avg("temperature_c").alias("average_temperature"),
        avg("vibration_mm_s").alias("average_vibration")
    )
)
```

---

### `app.py`

Contains the Streamlit user interface.

It is responsible for:

```text
Dashboard layout
Sidebar filters
KPIs
Tabs
Charts
Tables
User interaction
```

When the user changes a filter, Streamlit reruns the application and sends the selected values to the Spark processing functions.

For example:

```text
User selects:
Factory = Stuttgart

        ↓

Streamlit reruns app.py

        ↓

Selected factory is passed to
data_processing.py

        ↓

Spark filters and aggregates
the corresponding data

        ↓

Small analytical result

        ↓

Streamlit updates the dashboard
```

---

## Spark → Pandas → Streamlit

One important design principle in this project is to avoid moving large raw datasets directly into Pandas.

Instead:

```text
Large Spark DataFrame
        │
        ▼
      Filter
        │
        ▼
       Join
        │
        ▼
     GroupBy
        │
        ▼
   Aggregation
        │
        ▼
Small Spark DataFrame
        │
        ▼
    toPandas()
        │
        ▼
    Streamlit
```

For example:

```python
result_df = (
    sensor_df
    .groupBy("machine_id")
    .agg(
        avg("temperature_c")
        .alias("average_temperature")
    )
)

pandas_df = result_df.toPandas()
```

Only the reduced analytical result is converted to Pandas for visualization.

---

## Machine Health Indicator

The dashboard includes a simple rule-based machine health indicator.

Sensor information such as:

```text
Warning events
Critical events
Temperature
Vibration
Alert rate
```

is aggregated by machine and used to derive a health status.

This functionality was created for learning and demonstration purposes.

It is **not a trained machine-learning model** and should not be interpreted as a production predictive-maintenance system.

---

## Development Workflow

The project was developed incrementally using the following workflow:

```text
1. Understand the datasets
        ↓
2. Create the SparkSession
        ↓
3. Load data using PySpark
        ↓
4. Inspect schemas and data quality
        ↓
5. Clean and preprocess data
        ↓
6. Join the datasets
        ↓
7. Validate Spark transformations
        ↓
8. Build aggregations and KPIs
        ↓
9. Verify analytical results
        ↓
10. Build the Streamlit interface
        ↓
11. Add interactive filters
        ↓
12. Add charts and KPIs
        ↓
13. Add caching and application structure
```

---

## Spark Concepts Practiced

This project was used to practice several Spark concepts, including:

```text
SparkSession
Spark DataFrames
PySpark DataFrame API
Transformations
Actions
Lazy evaluation
Filtering
groupBy
Aggregations
Joins
Partitions
Shuffle operations
Batch processing
Spark SQL functions
Spark / Pandas interoperability
```

---

## Streamlit Concepts Practiced

The application also covers:

```text
Interactive dashboards
Sidebar filters
Select boxes
Date filters
KPIs
Tables
Bar charts
Line charts
Tabs
Resource caching
Data caching
Application reruns
Spark integration
```

---

## Installation

### Prerequisites

The project requires:

```text
Python 3
Java 17
Apache Spark / PySpark
```

Verify Java:

```bash
java -version
```

Verify Python:

```bash
python --version
```

---

### Clone the repository

```bash
git clone https://github.com/YoussefKamm/spark-streamlit-industrial-monitoring.git
```

Enter the project directory:

```bash
cd spark-streamlit-industrial-monitoring
```

---

### Create a virtual environment

```bash
python -m venv .venv
```

On Windows:

```bash
.venv\Scripts\activate
```

---

### Install dependencies

```bash
pip install -r requirements.txt
```

Current Python dependencies:

```text
pyspark
streamlit
pandas
numpy
```

---

## Running the Application

Start the Streamlit dashboard with:

```bash
streamlit run app.py
```

The application will normally be available at:

```text
http://localhost:8501
```

---

## Technologies

| Technology | Usage |
|---|---|
| Python | Application development |
| Apache Spark | Distributed data processing |
| PySpark | Python interface for Spark |
| Streamlit | Interactive dashboard |
| Pandas | Small analytical results for visualization |
| NumPy | Numerical utilities |
| Git / GitHub | Version control and project sharing |

---

## Key Learning Outcomes

Through this project, I practiced how to:

- structure a Spark-based Python application,
- process and clean data using PySpark,
- enrich datasets using joins,
- create analytical aggregations and KPIs,
- understand Spark transformations, actions, partitions, and shuffle operations,
- separate data-processing logic from UI logic,
- integrate Spark with Streamlit,
- safely convert reduced Spark results to Pandas,
- and build an interactive industrial monitoring dashboard.

---

## Possible Future Improvements

Possible extensions of the project include:

```text
Spark Structured Streaming
Real-time sensor ingestion
Advanced anomaly detection
Predictive maintenance
Machine-learning models
Database integration
Cloud object storage
Docker containerization
Automated tests
CI/CD
Cloud deployment
```

---

## Author

**Youssef Kammoun**

AI / Data Engineer

Hands-on project focused on **Apache Spark, PySpark, and Streamlit** for industrial data processing and monitoring.