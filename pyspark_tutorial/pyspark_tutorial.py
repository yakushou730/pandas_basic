import time

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

spark.conf.set("spark.sql.caseSensitive", "true")
PATH_BIGDATA = 'Toys_and_Games_5.json'
raw_sdf = spark.read.json(PATH_BIGDATA)
raw_sdf.show()
print('Code Executed Successfully')

COL_NAME_MAP = {
    "overall": "overall",
    "verified": "verified",
    "reviewTime": "review_time",
    "reviewerID": "reviewer_id",
    "asin": "asin",
    "reviewerName": "reviewer_name",
    "reviewText": "review_text",
    "summary": "summary",
    "unixReviewTime": "unix_review_time",
    "style": "style",
    "vote": "vote",
    "image": "image"
}

print('___________________________')
print('Columns names before rename:')
i = 1
for col_name in raw_sdf.schema.names:
    print(f'{i}: {col_name}')
    i = i + 1


def rename_columns(df, column_map):
    for old, new in column_map.items():
        df = df.withColumnRenamed(old, new)
    return df


raw_sdf = rename_columns(raw_sdf, COL_NAME_MAP)
print('___________________________')
print('Columns names after rename:')
i = 1
for col_name in raw_sdf.schema.names:
    print(f'{i}: {col_name}')
    i = i + 1

print('___________________________')

print('Code Executed Successfully')

SELECTED_COLUMNS = [
    "reviewer_id",
    "asin",
    "review_text",
    "summary",
    "verified",
    "overall",
    "vote",
    "unix_review_time",
    "review_time",
]

# Alternative method:
# raw_sdf = raw_sdf.select("reviewer_id", "asin", ...)
raw_sdf = raw_sdf.select(*SELECTED_COLUMNS)
print(raw_sdf.show())

print('Code Executed Successfully')


def create_path_snapshot():
    path_fixed = 'data/snapshot/pyspark/snapshot_{}.parquet'
    current_unix_time = int(time.time())
    return path_fixed.format(current_unix_time)


PATH_SNAPSHOT = create_path_snapshot()
raw_sdf = (
    raw_sdf
    .repartition("asin")
    .sortWithinPartitions("unix_review_time")
)
# Write data with partition and sorted
(
    raw_sdf
    .write.partitionBy("asin")
    .mode("overwrite")
    .parquet(PATH_SNAPSHOT)
)

print('Code Executed Successfully')
