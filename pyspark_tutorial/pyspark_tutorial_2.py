from pathlib import Path

import yaml
from dotenv import load_dotenv
from pyspark.sql import SparkSession


def create_spark_session():
    """Create a Spark Session"""
    _ = load_dotenv()
    return (
        SparkSession
        .builder
        .appName("SparkApp")
        .master("local[5]")
        .getOrCreate()
    )


spark = create_spark_session()
print('Session Started')
print('Code Executed Successfully')


def load_latest_parquet_path() -> Path:
    """
        :return: path to latest parquet file storage
    """
    with open('versions.yaml') as f:
        content = yaml.safe_load(f)
        print(content)
        latest = content['latest']
        path = content[latest]['path']
        return path


def read_latest_snapshot(ctx: SparkSession):
    """
    Read parquet data source from latest metadata
    :param ctx: A PySpark session context
    :return: PySpark Dataframe
"""
    path = load_latest_parquet_path()
    df = ctx.read.parquet(str(path))
    return df


main_df = read_latest_snapshot(spark)
main_df.show()
print('Code Executed Successfully')
