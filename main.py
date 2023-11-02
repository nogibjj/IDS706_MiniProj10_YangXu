from pyspark.sql import SparkSession
import seaborn as sns


def initialize_spark():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    return spark


def load_dataset(spark):
    flights = sns.load_dataset("flights")
    df = spark.createDataFrame(flights)
    return df


def filter_flights(df):
    return df.filter(df["passengers"] > 100)


def total_passengers_by_year(df):
    df.createOrReplaceTempView("flights_table")
    return spark.sql(
        """SELECT year, SUM(passengers) as total_passengers
        FROM flights_table GROUP BY year ORDER BY year"""
    )


if __name__ == "__main__":
    spark = initialize_spark()
    df = load_dataset(spark)
    df.show()

    filtered_flights = filter_flights(df)
    filtered_flights.show()

    yearly_passengers = total_passengers_by_year(df)
    yearly_passengers.show()
