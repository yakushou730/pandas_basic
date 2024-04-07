from dotenv import load_dotenv
from pyspark.sql import SparkSession


def create_spark_session():
    """Create a Spark Session"""
    _ = load_dotenv()
    spark = SparkSession.builder.appName("SparkApp").master("local[5]").getOrCreate()
    return spark


def read_sdf(spark, PATH_BIGDATA):
    """Read the dataset"""
    raw_sdf = spark.read.json(PATH_BIGDATA)
    return raw_sdf


def rename_columns(df, column_map):
    """Rename the columns"""
    for old, new in column_map.items():
        df = df.withColumnRenamed(old, new)

    return df


def select_subset(df, columns):
    """Select the required columns"""
    df = df.select(*columns)
    return df


def repartitioning_and_saving(df, PATH_SNAPSHOT):
    """Repartitioning and saving the snapshot"""
    df = df.repartition('reviewed_year', 'reviewed_month').sortWithinPartitions("asin")
    df.write.mode("overwrite").parquet(PATH_SNAPSHOT)
