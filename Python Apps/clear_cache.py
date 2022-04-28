from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName('Detect Language').getOrCreate()

    spark.sql("CLEAR CACHE").collect()

    spark.stop()


if __name__ == "__main__":
    main()
