import streamlit as st
from pyspark.sql import SparkSession


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