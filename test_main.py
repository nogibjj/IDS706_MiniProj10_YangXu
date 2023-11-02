import main
from pyspark.sql import SparkSession


def test_initialize_spark():
    spark = main.initialize_spark()
    assert isinstance(spark, SparkSession)


def test_load_dataset():
    spark = main.initialize_spark()
    df = main.load_dataset(spark)
    # The flights dataset has 144 rows
    assert df.count() == 144


def test_filter_flights():
    spark = main.initialize_spark()
    df = main.load_dataset(spark)
    filtered = main.filter_flights(df)
    # Since some flights will be filtered out
    assert filtered.count() <= 144
