Advanced Spark + Streamlit practice dataset

Files:
1. machines.csv
   - 24 machines
   - machine metadata: factory, line, model, install date, criticality

2. sensor_readings.csv
   - 5040 sensor readings
   - timestamp, temperature, pressure, vibration, energy, status
   - includes deliberate anomalies and a few missing values for preprocessing practice

3. maintenance_events.csv
   - 124 maintenance events
   - type, downtime, cost, technician, issue

Suggested joins:
sensor_readings.machine_id = machines.machine_id
maintenance_events.machine_id = machines.machine_id

Suggested dashboard:
- KPIs: machines, readings, warning/critical events, avg temperature, avg vibration,
  total downtime, maintenance cost
- Filters: factory, production line, machine, model, date range, status
- Charts: temperature trend, warnings by machine, average vibration by machine,
  downtime by factory, maintenance cost by type, status distribution





1. Understand the data
        ↓
2. Create SparkSession
        ↓
3. Read data
        ↓
4. Inspect schema
        ↓
5. Clean data
        ↓
6. Join datasets
        ↓
7. Test processing
        ↓
8. Create aggregations/KPIs
        ↓
9. Verify results
        ↓
10. Build Streamlit UI
        ↓
11. Add filters
        ↓
12. Add charts
        ↓
13. Add caching/error handling