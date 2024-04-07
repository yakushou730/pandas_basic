from pathconfig import PATH_TOY_GAME_DATA, create_path_snapshot
from utils.functions import rename_columns
from utils.session import create_spark_session
from utils.tableschema import COL_NAME_MAP, SELECTED_COLUMNS


def execute_pipeline():
    path_snapshot = create_path_snapshot()
    spark = create_spark_session()
    spark.conf.set("spark.sql.caseSensitive", "true")
    raw_sdf = spark.read.json(PATH_TOY_GAME_DATA)
    raw_sdf = rename_columns(raw_sdf, COL_NAME_MAP)
    raw_sdf = raw_sdf.select(*SELECTED_COLUMNS)
    raw_sdf.show(5)
    raw_sdf = raw_sdf.repartition("asin").sortWithinPartitions("unix_review_time")
    raw_sdf.write.partitionBy("asin").mode("overwrite").parquet(str(path_snapshot.resolve()))


execute_pipeline()

print('Code Executed Successfully')
