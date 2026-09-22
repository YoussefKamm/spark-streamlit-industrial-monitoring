from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    avg,
    count,
    sum as spark_sum,
    lit,
    when,
    greatest,
    round as spark_round
)

from spark_utils import get_spark


# =========================================================
# LOAD DATA
# =========================================================

def load_data():

    spark = get_spark()

    sensors_df = spark.read.csv(
        "data/sensor_readings.csv",
        header=True,
        inferSchema=True
    )

    machines_df = spark.read.csv(
        "data/machines.csv",
        header=True,
        inferSchema=True
    )

    maintenance_df = spark.read.csv(
        "data/maintenance_events.csv",
        header=True,
        inferSchema=True
    )

    return (
        sensors_df,
        machines_df,
        maintenance_df
    )


# =========================================================
# CLEAN SENSOR DATA
# =========================================================

def clean_sensors(sensors_df):

    clean_df = (
        sensors_df

        # Convert timestamp column to real Spark timestamp
        .withColumn(
            "timestamp",
            to_timestamp(
                col("timestamp")
            )
        )

        # Create a separate date column
        .withColumn(
            "date",
            to_date(
                col("timestamp")
            )
        )

        # Remove rows with missing important values
        .dropna(
            subset=[
                "machine_id",
                "timestamp",
                "temperature_c",
                "pressure_bar",
                "vibration_mm_s",
                "energy_kwh"
            ]
        )
    )

    return clean_df


# =========================================================
# CLEAN MACHINE DATA
# =========================================================

def clean_machines(machines_df):

    clean_df = (
        machines_df

        # Convert installation date to Spark date
        .withColumn(
            "install_date",
            to_date(
                col("install_date")
            )
        )

        # Remove rows with missing important machine information
        .dropna(
            subset=[
                "machine_id",
                "factory",
                "production_line",
                "model"
            ]
        )
    )

    return clean_df


# =========================================================
# CLEAN MAINTENANCE DATA
# =========================================================

def clean_maintenance(maintenance_df):

    clean_df = (
        maintenance_df

        # Convert maintenance date to Spark date
        .withColumn(
            "event_date",
            to_date(
                col("event_date")
            )
        )

        # Remove rows with missing important maintenance information
        .dropna(
            subset=[
                "event_id",
                "machine_id",
                "event_date",
                "maintenance_type",
                "downtime_minutes",
                "cost_eur"
            ]
        )
    )

    return clean_df


# =========================================================
# PREPARE / JOIN DATA
# =========================================================

def prepare_data():

    # Load raw datasets
    sensors_df, machines_df, maintenance_df = load_data()

    # Clean datasets
    sensors_df = clean_sensors(
        sensors_df
    )

    machines_df = clean_machines(
        machines_df
    )

    maintenance_df = clean_maintenance(
        maintenance_df
    )


    # -----------------------------------------------------
    # Sensor readings + machine metadata
    # -----------------------------------------------------

    sensor_enriched_df = sensors_df.join(
        machines_df,
        on="machine_id",
        how="left"
    )


    # -----------------------------------------------------
    # Maintenance events + machine metadata
    # -----------------------------------------------------

    maintenance_enriched_df = maintenance_df.join(
        machines_df,
        on="machine_id",
        how="left"
    )


    return (
        sensor_enriched_df,
        maintenance_enriched_df,
        machines_df
    )


# =========================================================
# FILTER DATA
# =========================================================

def filter_data(
    sensor_df,
    maintenance_df,
    machines_df,
    factory="All",
    production_line="All",
    model="All",
    machine_id="All",
    status="All",
    start_date=None,
    end_date=None
):

    # -----------------------------------------------------
    # Factory / line / model / machine filters
    # -----------------------------------------------------

    filters = [
        ("factory", factory),
        ("production_line", production_line),
        ("model", model),
        ("machine_id", machine_id)
    ]


    for column_name, value in filters:

        if value != "All":

            sensor_df = sensor_df.filter(
                col(column_name) == value
            )

            maintenance_df = maintenance_df.filter(
                col(column_name) == value
            )

            machines_df = machines_df.filter(
                col(column_name) == value
            )


    # -----------------------------------------------------
    # Sensor status filter
    # -----------------------------------------------------

    if status != "All":

        sensor_df = sensor_df.filter(
            col("status") == status
        )


    # -----------------------------------------------------
    # Date range filter
    # -----------------------------------------------------

    if (
        start_date is not None
        and
        end_date is not None
    ):

        sensor_df = sensor_df.filter(
            (
                col("date")
                >=
                lit(
                    str(start_date)
                ).cast("date")
            )
            &
            (
                col("date")
                <=
                lit(
                    str(end_date)
                ).cast("date")
            )
        )


        maintenance_df = maintenance_df.filter(
            (
                col("event_date")
                >=
                lit(
                    str(start_date)
                ).cast("date")
            )
            &
            (
                col("event_date")
                <=
                lit(
                    str(end_date)
                ).cast("date")
            )
        )


    return (
        sensor_df,
        maintenance_df,
        machines_df
    )


# =========================================================
# KPI CALCULATION
# =========================================================

def calculate_kpis(
    sensor_enriched_df,
    maintenance_enriched_df,
    machines_df
):

    # -----------------------------------------------------
    # Total machines represented in current sensor data
    # -----------------------------------------------------

    total_machines = (
        sensor_enriched_df
        .select(
            "machine_id"
        )
        .distinct()
        .count()
    )


    # -----------------------------------------------------
    # Total sensor readings
    # -----------------------------------------------------

    total_readings = (
        sensor_enriched_df
        .count()
    )


    # -----------------------------------------------------
    # Warning events
    # -----------------------------------------------------

    warning_events = (
        sensor_enriched_df
        .filter(
            col("status") == "WARNING"
        )
        .count()
    )


    # -----------------------------------------------------
    # Critical events
    # -----------------------------------------------------

    critical_events = (
        sensor_enriched_df
        .filter(
            col("status") == "CRITICAL"
        )
        .count()
    )


    # -----------------------------------------------------
    # Average temperature
    # -----------------------------------------------------

    average_temperature = (
        sensor_enriched_df

        .agg(
            avg(
                "temperature_c"
            ).alias(
                "avg_temperature"
            )
        )

        .collect()[0][
            "avg_temperature"
        ]
    )


    # -----------------------------------------------------
    # Average vibration
    # -----------------------------------------------------

    average_vibration = (
        sensor_enriched_df

        .agg(
            avg(
                "vibration_mm_s"
            ).alias(
                "avg_vibration"
            )
        )

        .collect()[0][
            "avg_vibration"
        ]
    )


    # -----------------------------------------------------
    # Total maintenance downtime
    # -----------------------------------------------------

    total_downtime = (
        maintenance_enriched_df

        .agg(
            spark_sum(
                "downtime_minutes"
            ).alias(
                "total_downtime"
            )
        )

        .collect()[0][
            "total_downtime"
        ]
    )


    # If no maintenance rows are found
    if total_downtime is None:

        total_downtime = 0


    # -----------------------------------------------------
    # Total maintenance cost
    # -----------------------------------------------------

    total_maintenance_cost = (
        maintenance_enriched_df

        .agg(
            spark_sum(
                "cost_eur"
            ).alias(
                "total_cost"
            )
        )

        .collect()[0][
            "total_cost"
        ]
    )


    # If no maintenance rows are found
    if total_maintenance_cost is None:

        total_maintenance_cost = 0


    # -----------------------------------------------------
    # Return all KPI values
    # -----------------------------------------------------

    return {

        "total_machines":
            total_machines,

        "total_readings":
            total_readings,

        "warning_events":
            warning_events,

        "critical_events":
            critical_events,

        "average_temperature":
            average_temperature,

        "average_vibration":
            average_vibration,

        "total_downtime":
            total_downtime,

        "total_maintenance_cost":
            total_maintenance_cost
    }






# =========================================================
# BUILD DATA FOR DASHBOARD CHARTS
# =========================================================

def build_chart_data(
    sensor_df,
    maintenance_df
):

    # -----------------------------------------------------
    # 1. Temperature / vibration / energy trend by day
    # -----------------------------------------------------

    daily_trend_df = (
        sensor_df
        .groupBy("date")
        .agg(
            avg("temperature_c")
            .alias("average_temperature"),

            avg("vibration_mm_s")
            .alias("average_vibration"),

            spark_sum("energy_kwh")
            .alias("total_energy")
        )
        .orderBy("date")
    )


    # -----------------------------------------------------
    # 2. Machine performance
    # -----------------------------------------------------

    machine_performance_df = (
        sensor_df
        .groupBy("machine_id")
        .agg(
            avg("temperature_c")
            .alias("average_temperature"),

            avg("vibration_mm_s")
            .alias("average_vibration"),

            avg("pressure_bar")
            .alias("average_pressure"),

            count("*")
            .alias("number_of_readings")
        )
        .orderBy(
            col("average_temperature").desc()
        )
    )


    # -----------------------------------------------------
    # 3. Warning / critical events by machine
    # -----------------------------------------------------

    alerts_df = (
        sensor_df

        .filter(
            col("status").isin(
                "WARNING",
                "CRITICAL"
            )
        )

        .groupBy(
            "machine_id",
            "status"
        )

        .count()

        .groupBy("machine_id")

        .pivot(
            "status",
            [
                "WARNING",
                "CRITICAL"
            ]
        )

        .sum("count")

        .fillna(0)

        .orderBy(
            col("CRITICAL").desc(),
            col("WARNING").desc()
        )
    )


    # -----------------------------------------------------
    # 4. Status distribution
    # -----------------------------------------------------

    status_distribution_df = (
        sensor_df
        .groupBy("status")
        .count()
        .orderBy(
            col("count").desc()
        )
    )


    # -----------------------------------------------------
    # 5. Maintenance by type
    # -----------------------------------------------------

    maintenance_type_df = (
        maintenance_df
        .groupBy("maintenance_type")
        .agg(
            spark_sum("cost_eur")
            .alias("maintenance_cost"),

            spark_sum("downtime_minutes")
            .alias("downtime_minutes"),

            count("*")
            .alias("maintenance_events")
        )
        .orderBy(
            col("maintenance_cost").desc()
        )
    )


    # -----------------------------------------------------
    # 6. Downtime by factory
    # -----------------------------------------------------

    factory_downtime_df = (
        maintenance_df
        .groupBy("factory")
        .agg(
            spark_sum("downtime_minutes")
            .alias("downtime_minutes"),

            spark_sum("cost_eur")
            .alias("maintenance_cost")
        )
        .orderBy(
            col("downtime_minutes").desc()
        )
    )


    # -----------------------------------------------------
    # Convert SMALL aggregated results to Pandas
    # -----------------------------------------------------

    return {

        "daily_trend":
            daily_trend_df.toPandas(),

        "machine_performance":
            machine_performance_df.toPandas(),

        "alerts":
            alerts_df.toPandas(),

        "status_distribution":
            status_distribution_df.toPandas(),

        "maintenance_type":
            maintenance_type_df.toPandas(),

        "factory_downtime":
            factory_downtime_df.toPandas()
    }



# =========================================================
# MACHINE HEALTH ANALYSIS
# =========================================================

def build_machine_health(sensor_df):

    machine_health_df = (
        sensor_df

        .groupBy(
            "machine_id",
            "factory",
            "production_line",
            "model",
            "criticality"
        )

        .agg(

            count("*")
            .alias("readings"),

            spark_sum(
                when(
                    col("status") == "WARNING",
                    1
                ).otherwise(0)
            )
            .alias("warnings"),

            spark_sum(
                when(
                    col("status") == "CRITICAL",
                    1
                ).otherwise(0)
            )
            .alias("criticals"),

            avg("temperature_c")
            .alias("average_temperature"),

            avg("vibration_mm_s")
            .alias("average_vibration"),

            avg("pressure_bar")
            .alias("average_pressure")
        )

        # ---------------------------------------------
        # Alert rate
        # ---------------------------------------------

        .withColumn(
            "alert_rate_pct",

            spark_round(
                (
                    (
                        col("warnings")
                        +
                        col("criticals")
                    )
                    /
                    col("readings")
                )
                * 100,
                1
            )
        )

        # ---------------------------------------------
        # Health score
        #
        # Warning  = penalty of 1
        # Critical = penalty of 3
        # ---------------------------------------------

        .withColumn(
            "health_score",

            spark_round(

                greatest(

                    lit(0.0),

                    lit(100.0)
                    -
                    (
                        (
                            col("warnings")
                            +
                            (3 * col("criticals"))
                        )
                        /
                        col("readings")
                    )
                    * 100
                ),

                1
            )
        )

        # ---------------------------------------------
        # Health category
        # ---------------------------------------------

        .withColumn(
            "health_status",

            when(
                col("health_score") >= 90,
                "Healthy"
            )

            .when(
                col("health_score") >= 75,
                "Watch"
            )

            .otherwise(
                "At Risk"
            )
        )

        .orderBy(
            col("health_score").asc()
        )
    )

    return machine_health_df.toPandas()