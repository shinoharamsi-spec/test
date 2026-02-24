import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Databricksノートブック外で実行する場合はSparkセッションを作成
spark = SparkSession.builder \
    .appName('integrity-tests') \
    .getOrCreate()

# 指定したテーブルがデータベースに存在するか？
def tableExists(tableName, dbName):
    return spark.catalog.tableExists(f"{dbName}.{tableName}")

# 指定したカラムがDataFrameに存在するか？
def columnExists(dataFrame, columnName):
    if columnName in dataFrame.columns:
        return True
    else:
        return False

# 指定したカラムの特定の値を持つ行数は？
def numRowsInColumnForValue(dataFrame, columnName, columnValue):
    df = dataFrame.filter(col(columnName) == columnValue)
    return df.count()