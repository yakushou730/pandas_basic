from dotenv import load_dotenv
from pyspark.sql import SparkSession


def create_spark_session():
    """Create a Spark Session"""
    _ = load_dotenv()
    return (
        SparkSession
        .builder
        .appName("SparkApp")
        .master("local[4]")
        .getOrCreate()
    )
