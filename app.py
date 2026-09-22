import streamlit as st

from pyspark.sql.functions import (
    col,
    min as spark_min,
    max as spark_max
)

from data_processing import (
    prepare_data,
    filter_data,
    calculate_kpis,
    build_chart_data,
    build_machine_health
)


# =========================================================
# PAGE CONFIGURATION
# =========================================================

st.set_page_config(
    page_title="Industrial Monitoring Dashboard",
    page_icon="🏭",
    layout="wide"
)


st.title("🏭 Industrial Machine Monitoring")

st.caption(
    "Spark + Streamlit monitoring, health and maintenance analytics"
)


# =========================================================
# LOAD AND PREPARE DATA
# =========================================================

@st.cache_resource
def get_prepared_data():

    return prepare_data()


sensor_df, maintenance_df, machines_df = (
    get_prepared_data()
)


# =========================================================
# MACHINE METADATA
# Used for sidebar dependent filters
# =========================================================

machine_metadata = (
    machines_df
    .select(
        "machine_id",
        "factory",
        "production_line",
        "model"
    )
    .dropDuplicates()
    .toPandas()
)


# =========================================================
# DATE BOUNDS
# =========================================================

date_bounds = (
    sensor_df
    .agg(
        spark_min("date").alias("min_date"),
        spark_max("date").alias("max_date")
    )
    .collect()[0]
)


min_date = date_bounds["min_date"]
max_date = date_bounds["max_date"]


# =========================================================
# SIDEBAR
# =========================================================

st.sidebar.title("🔎 Filters")


# =========================================================
# FACTORY FILTER
# =========================================================

factory_options = (
    ["All"]
    +
    sorted(
        machine_metadata["factory"]
        .dropna()
        .unique()
        .tolist()
    )
)


factory = st.sidebar.selectbox(
    "Factory",
    factory_options
)


# Start with all machine metadata
filtered_metadata = machine_metadata.copy()


if factory != "All":

    filtered_metadata = filtered_metadata[
        filtered_metadata["factory"] == factory
    ]


# =========================================================
# PRODUCTION LINE FILTER
# =========================================================

line_options = (
    ["All"]
    +
    sorted(
        filtered_metadata["production_line"]
        .dropna()
        .unique()
        .tolist()
    )
)


production_line = st.sidebar.selectbox(
    "Production Line",
    line_options
)


if production_line != "All":

    filtered_metadata = filtered_metadata[
        filtered_metadata["production_line"]
        == production_line
    ]


# =========================================================
# MODEL FILTER
# =========================================================

model_options = (
    ["All"]
    +
    sorted(
        filtered_metadata["model"]
        .dropna()
        .unique()
        .tolist()
    )
)


model = st.sidebar.selectbox(
    "Machine Model",
    model_options
)


if model != "All":

    filtered_metadata = filtered_metadata[
        filtered_metadata["model"] == model
    ]


# =========================================================
# MACHINE FILTER
# =========================================================

machine_options = (
    ["All"]
    +
    sorted(
        filtered_metadata["machine_id"]
        .dropna()
        .unique()
        .tolist()
    )
)


machine_id = st.sidebar.selectbox(
    "Machine",
    machine_options
)


# =========================================================
# STATUS FILTER
# =========================================================

status = st.sidebar.selectbox(
    "Sensor Status",
    [
        "All",
        "OK",
        "WARNING",
        "CRITICAL"
    ]
)


# =========================================================
# DATE RANGE
# =========================================================

date_range = st.sidebar.date_input(
    "Date Range",
    value=(
        min_date,
        max_date
    ),
    min_value=min_date,
    max_value=max_date
)


if (
    isinstance(date_range, (list, tuple))
    and len(date_range) == 2
):

    start_date = date_range[0]
    end_date = date_range[1]

else:

    start_date = min_date
    end_date = max_date


# =========================================================
# APPLY NORMAL FILTERS
# =========================================================

(
    filtered_sensor_df,
    filtered_maintenance_df,
    filtered_machines_df
) = filter_data(

    sensor_df,
    maintenance_df,
    machines_df,

    factory=factory,
    production_line=production_line,
    model=model,
    machine_id=machine_id,
    status=status,

    start_date=start_date,
    end_date=end_date
)


# =========================================================
# HEALTH DATA
#
# Important:
# health calculation ignores the status filter.
#
# Otherwise selecting only CRITICAL would make all selected
# machines appear unhealthy.
# =========================================================

(
    health_sensor_df,
    _,
    _
) = filter_data(

    sensor_df,
    maintenance_df,
    machines_df,

    factory=factory,
    production_line=production_line,
    model=model,
    machine_id=machine_id,

    status="All",

    start_date=start_date,
    end_date=end_date
)


# =========================================================
# KPI CALCULATION
# =========================================================

kpis = calculate_kpis(
    filtered_sensor_df,
    filtered_maintenance_df,
    filtered_machines_df
)


# =========================================================
# CHART DATA
# =========================================================

chart_data = build_chart_data(
    filtered_sensor_df,
    filtered_maintenance_df
)


# =========================================================
# MACHINE HEALTH
# =========================================================

machine_health = build_machine_health(
    health_sensor_df
)


# =========================================================
# ALERT RATE
# =========================================================

if kpis["total_readings"] > 0:

    alert_rate = (
        (
            kpis["warning_events"]
            +
            kpis["critical_events"]
        )
        /
        kpis["total_readings"]
    ) * 100

else:

    alert_rate = 0


# =========================================================
# FORMAT HELPERS
# =========================================================

def format_decimal(
    value,
    decimals=1,
    suffix=""
):

    if value is None:

        return "—"

    return f"{value:,.{decimals}f}{suffix}"


# =========================================================
# MAIN KPI SECTION
# =========================================================

st.subheader("📊 Key Performance Indicators")


# ---------------------------------------------------------
# KPI ROW 1
# ---------------------------------------------------------

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)


with kpi1:

    st.metric(
        "Machines",
        f"{kpis['total_machines']:,}"
    )


with kpi2:

    st.metric(
        "Sensor Readings",
        f"{kpis['total_readings']:,}"
    )


with kpi3:

    st.metric(
        "Warnings",
        f"{kpis['warning_events']:,}"
    )


with kpi4:

    st.metric(
        "Critical Events",
        f"{kpis['critical_events']:,}"
    )


with kpi5:

    st.metric(
        "Alert Rate",
        f"{alert_rate:.1f}%"
    )


# ---------------------------------------------------------
# KPI ROW 2
# ---------------------------------------------------------

kpi6, kpi7, kpi8, kpi9 = st.columns(4)


with kpi6:

    st.metric(
        "Average Temperature",
        format_decimal(
            kpis["average_temperature"],
            1,
            " °C"
        )
    )


with kpi7:

    st.metric(
        "Average Vibration",
        format_decimal(
            kpis["average_vibration"],
            2,
            " mm/s"
        )
    )


with kpi8:

    downtime_hours = (
        kpis["total_downtime"] / 60
    )

    st.metric(
        "Maintenance Downtime",
        f"{downtime_hours:,.1f} h"
    )


with kpi9:

    st.metric(
        "Maintenance Cost",
        f"€{kpis['total_maintenance_cost']:,.0f}"
    )


st.divider()


# =========================================================
# TABS
# =========================================================

(
    overview_tab,
    sensor_tab,
    maintenance_tab,
    machine_tab
) = st.tabs(
    [
        "📈 Overview",
        "🌡️ Sensors",
        "🔧 Maintenance",
        "🏭 Machines"
    ]
)


# =========================================================
# OVERVIEW TAB
# =========================================================

with overview_tab:

    # =====================================================
    # MACHINE HEALTH
    # =====================================================

    st.subheader(
        "❤️ Machine Health Overview"
    )


    if not machine_health.empty:

        # -------------------------------------------------
        # Health status counts
        # -------------------------------------------------

        healthy_count = (
            machine_health[
                machine_health["health_status"]
                == "Healthy"
            ]
            .shape[0]
        )


        watch_count = (
            machine_health[
                machine_health["health_status"]
                == "Watch"
            ]
            .shape[0]
        )


        at_risk_count = (
            machine_health[
                machine_health["health_status"]
                == "At Risk"
            ]
            .shape[0]
        )


        health1, health2, health3 = st.columns(3)


        with health1:

            st.metric(
                "Healthy Machines",
                healthy_count
            )


        with health2:

            st.metric(
                "Machines to Watch",
                watch_count
            )


        with health3:

            st.metric(
                "At Risk Machines",
                at_risk_count
            )


        # =================================================
        # TOP RISK MACHINES
        # =================================================

        st.subheader(
            "⚠️ Top 5 Machines Requiring Attention"
        )


        top_risk = (
            machine_health
            .sort_values(
                "health_score",
                ascending=True
            )
            .head(5)
        )


        st.dataframe(

            top_risk[
                [
                    "machine_id",
                    "factory",
                    "production_line",
                    "model",
                    "health_score",
                    "alert_rate_pct",
                    "warnings",
                    "criticals"
                ]
            ],

            use_container_width=True,

            hide_index=True,

            column_config={

                "machine_id":
                    "Machine",

                "factory":
                    "Factory",

                "production_line":
                    "Line",

                "model":
                    "Model",

                "health_score":
                    st.column_config.ProgressColumn(
                        "Health Score",
                        min_value=0,
                        max_value=100,
                        format="%.1f"
                    ),

                "alert_rate_pct":
                    st.column_config.NumberColumn(
                        "Alert Rate",
                        format="%.1f%%"
                    ),

                "warnings":
                    "Warnings",

                "criticals":
                    "Critical"
            }
        )


        # =================================================
        # HEALTH DISTRIBUTION
        # =================================================

        st.subheader(
            "📊 Machine Health Distribution"
        )


        health_distribution = (
            machine_health[
                "health_status"
            ]
            .value_counts()
            .reset_index()
        )


        health_distribution.columns = [
            "health_status",
            "machines"
        ]


        st.bar_chart(
            health_distribution
            .set_index(
                "health_status"
            )[
                ["machines"]
            ],
            use_container_width=True
        )


    else:

        st.info(
            "No machine health data available for the selected filters."
        )


    st.divider()


    # =====================================================
    # DAILY TREND DATA
    # =====================================================

    daily_trend = chart_data[
        "daily_trend"
    ]


    # =====================================================
    # TEMPERATURE TREND
    # =====================================================

    st.subheader(
        "🌡️ Average Temperature Over Time"
    )


    if not daily_trend.empty:

        temperature_chart = (
            daily_trend
            .set_index("date")[
                ["average_temperature"]
            ]
        )


        st.line_chart(
            temperature_chart,
            use_container_width=True
        )


    else:

        st.info(
            "No temperature data available."
        )


    st.divider()


    # =====================================================
    # MACHINE PERFORMANCE CHARTS
    # =====================================================

    machine_performance = chart_data[
        "machine_performance"
    ]


    performance_col1, performance_col2 = (
        st.columns(2)
    )


    # -----------------------------------------------------
    # Temperature by machine
    # -----------------------------------------------------

    with performance_col1:

        st.subheader(
            "🔥 Average Temperature by Machine"
        )


        if not machine_performance.empty:

            machine_temperature = (
                machine_performance
                .set_index("machine_id")[
                    ["average_temperature"]
                ]
            )


            st.bar_chart(
                machine_temperature,
                use_container_width=True
            )


        else:

            st.info(
                "No machine temperature data."
            )


    # -----------------------------------------------------
    # Vibration by machine
    # -----------------------------------------------------

    with performance_col2:

        st.subheader(
            "📳 Average Vibration by Machine"
        )


        if not machine_performance.empty:

            machine_vibration = (
                machine_performance
                .set_index("machine_id")[
                    ["average_vibration"]
                ]
            )


            st.bar_chart(
                machine_vibration,
                use_container_width=True
            )


        else:

            st.info(
                "No machine vibration data."
            )


    st.divider()


    # =====================================================
    # ALERTS BY MACHINE
    # =====================================================

    st.subheader(
        "🚨 Alerts by Machine"
    )


    alerts = chart_data[
        "alerts"
    ]


    if not alerts.empty:

        alerts_chart = (
            alerts
            .set_index(
                "machine_id"
            )
        )


        st.bar_chart(
            alerts_chart[
                [
                    "WARNING",
                    "CRITICAL"
                ]
            ],
            use_container_width=True
        )


    else:

        st.success(
            "No warning or critical events for the selected filters."
        )


    st.divider()


    # =====================================================
    # STATUS + ENERGY
    # =====================================================

    overview_bottom1, overview_bottom2 = (
        st.columns(2)
    )


    # -----------------------------------------------------
    # Status distribution
    # -----------------------------------------------------

    with overview_bottom1:

        st.subheader(
            "📊 Sensor Status Distribution"
        )


        status_distribution = chart_data[
            "status_distribution"
        ]


        if not status_distribution.empty:

            status_chart = (
                status_distribution
                .set_index("status")[
                    ["count"]
                ]
            )


            st.bar_chart(
                status_chart,
                use_container_width=True
            )


        else:

            st.info(
                "No status data available."
            )


    # -----------------------------------------------------
    # Energy consumption
    # -----------------------------------------------------

    with overview_bottom2:

        st.subheader(
            "⚡ Daily Energy Consumption"
        )


        if not daily_trend.empty:

            energy_chart = (
                daily_trend
                .set_index("date")[
                    ["total_energy"]
                ]
            )


            st.line_chart(
                energy_chart,
                use_container_width=True
            )


        else:

            st.info(
                "No energy data available."
            )


# =========================================================
# SENSOR TAB
# =========================================================

with sensor_tab:

    st.subheader(
        "🌡️ Sensor Analytics"
    )


    daily_trend = chart_data[
        "daily_trend"
    ]


    sensor_col1, sensor_col2 = (
        st.columns(2)
    )


    # -----------------------------------------------------
    # Temperature trend
    # -----------------------------------------------------

    with sensor_col1:

        st.subheader(
            "Temperature Trend"
        )


        if not daily_trend.empty:

            st.line_chart(
                daily_trend
                .set_index("date")[
                    ["average_temperature"]
                ],
                use_container_width=True
            )


        else:

            st.info(
                "No temperature data available."
            )


    # -----------------------------------------------------
    # Vibration trend
    # -----------------------------------------------------

    with sensor_col2:

        st.subheader(
            "Vibration Trend"
        )


        if not daily_trend.empty:

            st.line_chart(
                daily_trend
                .set_index("date")[
                    ["average_vibration"]
                ],
                use_container_width=True
            )


        else:

            st.info(
                "No vibration data available."
            )


    st.divider()


    # =====================================================
    # SENSOR TABLE
    # =====================================================

    st.subheader(
        "Latest Sensor Readings"
    )


    sensor_preview = (
        filtered_sensor_df
        .orderBy(
            col("timestamp").desc()
        )
        .limit(500)
        .toPandas()
    )


    if not sensor_preview.empty:

        st.dataframe(
            sensor_preview,
            use_container_width=True,
            hide_index=True
        )


    else:

        st.info(
            "No sensor records match the selected filters."
        )


# =========================================================
# MAINTENANCE TAB
# =========================================================

with maintenance_tab:

    st.subheader(
        "🔧 Maintenance Analytics"
    )


    maintenance_col1, maintenance_col2 = (
        st.columns(2)
    )


    # -----------------------------------------------------
    # Maintenance Cost
    # -----------------------------------------------------

    with maintenance_col1:

        st.subheader(
            "💰 Maintenance Cost by Type"
        )


        maintenance_type = chart_data[
            "maintenance_type"
        ]


        if not maintenance_type.empty:

            cost_chart = (
                maintenance_type
                .set_index(
                    "maintenance_type"
                )[
                    ["maintenance_cost"]
                ]
            )


            st.bar_chart(
                cost_chart,
                use_container_width=True
            )


        else:

            st.info(
                "No maintenance data available."
            )


    # -----------------------------------------------------
    # Factory downtime
    # -----------------------------------------------------

    with maintenance_col2:

        st.subheader(
            "⏱️ Downtime by Factory"
        )


        factory_downtime = chart_data[
            "factory_downtime"
        ]


        if not factory_downtime.empty:

            downtime_chart = (
                factory_downtime
                .set_index("factory")[
                    ["downtime_minutes"]
                ]
            )


            st.bar_chart(
                downtime_chart,
                use_container_width=True
            )


        else:

            st.info(
                "No downtime data available."
            )


    st.divider()


    # =====================================================
    # MAINTENANCE TABLE
    # =====================================================

    st.subheader(
        "Maintenance Event History"
    )


    maintenance_preview = (
        filtered_maintenance_df
        .orderBy(
            col("event_date").desc()
        )
        .limit(500)
        .toPandas()
    )


    if not maintenance_preview.empty:

        st.dataframe(
            maintenance_preview,
            use_container_width=True,
            hide_index=True
        )


    else:

        st.info(
            "No maintenance events match the selected filters."
        )


# =========================================================
# MACHINE TAB
# =========================================================

with machine_tab:

    st.subheader(
        "🏭 Machine Information"
    )


    # =====================================================
    # MACHINE HEALTH TABLE
    # =====================================================

    st.subheader(
        "Machine Health Details"
    )


    if not machine_health.empty:

        health_table = (
            machine_health
            .sort_values(
                "health_score",
                ascending=True
            )
        )


        st.dataframe(

            health_table,

            use_container_width=True,

            hide_index=True,

            column_config={

                "machine_id":
                    "Machine",

                "factory":
                    "Factory",

                "production_line":
                    "Production Line",

                "model":
                    "Model",

                "criticality":
                    "Criticality",

                "readings":
                    "Readings",

                "warnings":
                    "Warnings",

                "criticals":
                    "Critical",

                "alert_rate_pct":
                    st.column_config.NumberColumn(
                        "Alert Rate",
                        format="%.1f%%"
                    ),

                "health_score":
                    st.column_config.ProgressColumn(
                        "Health Score",
                        min_value=0,
                        max_value=100,
                        format="%.1f"
                    ),

                "health_status":
                    "Health Status",

                "average_temperature":
                    st.column_config.NumberColumn(
                        "Avg Temperature",
                        format="%.1f °C"
                    ),

                "average_vibration":
                    st.column_config.NumberColumn(
                        "Avg Vibration",
                        format="%.2f mm/s"
                    ),

                "average_pressure":
                    st.column_config.NumberColumn(
                        "Avg Pressure",
                        format="%.2f bar"
                    )
            }
        )


    else:

        st.info(
            "No health information available."
        )


    st.divider()


    # =====================================================
    # MACHINE MASTER DATA
    # =====================================================

    st.subheader(
        "Machine Master Data"
    )


    machine_preview = (
        filtered_machines_df
        .orderBy(
            "machine_id"
        )
        .toPandas()
    )


    if not machine_preview.empty:

        st.dataframe(
            machine_preview,
            use_container_width=True,
            hide_index=True
        )


    else:

        st.info(
            "No machines match the selected filters."
        )