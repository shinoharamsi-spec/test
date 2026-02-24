import pytest
import pyspark
from myfunctions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

tableName    = "diamonds"
dbName       = "default"
columnName   = "clarity"
columnValue  = "SI2"

# Sparkセッションを作成
spark = SparkSession.builder \
    .appName('integrity-tests') \
    .getOrCreate()

# テスト用のフェイクデータを作成（本番データを使わない！）
schema = StructType([
    StructField("_c0",     IntegerType(), True),
    StructField("carat",   FloatType(),   True),
    StructField("cut",     StringType(),  True),
    StructField("color",   StringType(),  True),
    StructField("clarity", StringType(),  True),
    StructField("depth",   FloatType(),   True),
    StructField("table",   IntegerType(), True),
    StructField("price",   IntegerType(), True),
    StructField("x",       FloatType(),   True),
    StructField("y",       FloatType(),   True),
    StructField("z",       FloatType(),   True),
])

data = [
    (1, 0.23, "Ideal",   "E", "SI2", 61.5, 55, 326, 3.95, 3.98, 2.43),
    (2, 0.21, "Premium", "E", "SI1", 59.8, 61, 326, 3.89, 3.84, 2.31)
]

df = spark.createDataFrame(data, schema)

# テスト1: テーブルが存在するか？
def test_tableExists():
    assert tableExists(tableName, dbName) is True

# テスト2: カラムが存在するか？
def test_columnExists():
    assert columnExists(df, columnName) is True

# テスト3: 指定した値を持つ行が1行以上あるか？
def test_numRowsInColumnForValue():
    assert numRowsInColumnForValue(df, columnName, columnValue) > 0