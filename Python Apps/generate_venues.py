import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min


def loadParquet(spark, file_path) -> DataFrame:
    df = spark.read.parquet(f"hdfs://open1:9000{file_path}")
    return df


def main():
    spark = SparkSession.builder.appName('Generate Venues with works count.').getOrCreate()

    years_number = sys.argv[1]

    venues = loadParquet(spark, "/parquet_files/venues.parquet")
    years_df = loadParquet(spark, f"/parquet_files/{years_number}_years.parquet")
    venues.crossJoin(years_df) \
        .withColumnRenamed("id", "venue_id") \
        .select("venue_id", "year") \
        .createOrReplaceTempView("venues")

    works_host_venues = loadParquet(spark, "/parquet_files/works_host_venues.parquet")
    works_host_venues.select("venue_id", "work_id") \
        .createOrReplaceTempView("works_host_venues")

    works = loadParquet(spark, "/parquet_files/works.parquet")
    works.withColumnRenamed("id", "work_id") \
        .select("work_id", "publication_year") \
        .createOrReplaceTempView("works")

    works_counts = spark.sql("""
    select venues.venue_id as venue_id, venues.year as year, count(distinct works.work_id) as counts
    from venues
    join works_host_venues
        on venues.venue_id = works_host_venues.venue_id
    left join works
        on works.publication_year = venues.year and works.work_id = works_host_venues.work_id
    group by venues.venue_id, venues.year
    order by venues.venue_id, venues.year
    """)
    print((works_counts.count(), len(works_counts.columns)))
    works_counts.show(20000, False)
    works_counts.createOrReplaceTempView("works_counts")

    filtered_venues = spark.sql("""
    select venue_id
    from works_counts
    group by venue_id
    having min(counts) > 36
    """)
    print((filtered_venues.count(), len(filtered_venues.columns)))
    filtered_venues.show(20000, False)
    filtered_with_years = filtered_venues.crossJoin(years_df)
    print((filtered_with_years.count(), len(filtered_with_years.columns)))
    filtered_with_years.show(20000, False)

    filtered_with_years.write.\
        mode("overwrite").\
        parquet(f"hdfs://open1:9000/parquet_files/venues_{years_number}_years.parquet")

    spark.stop()


if __name__ == "__main__":
    main()
