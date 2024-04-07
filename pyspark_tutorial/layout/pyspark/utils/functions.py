from pyspark.sql import DataFrame as SparkDf


def rename_columns(df: SparkDf, column_map: dict) -> SparkDf:
    """
        :param df: Spark Dataframe to Rename Columns
        :param column_map: Old and New column map
        :return: Spark Dataframe after rename
    """
    for old, new in column_map.items():
        df = df.withColumnRenamed(old, new)
    return df
