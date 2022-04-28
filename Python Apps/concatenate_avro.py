import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StringType, FloatType, StructField
import cld3


def detectLanguage(title):
    prediction = cld3.get_language(title)
    if prediction is not None:
        return prediction[0], round(prediction[1], 4)
    return "Unknown", 0.0000


def main():
    spark = SparkSession.builder.appName('Get Venues - AVRO').getOrCreate()

    schema = StructType([
        StructField("language", StringType(), False),
        StructField("confidence", FloatType(), False)
    ])
    detect_udf = udf(detectLanguage, schema)

    works = spark.read\
        .format("avro")\
        .load("hdfs://open1:9000/avro_files/works.avro") \
        .select(col("id"), col("publication_year"), col("title"))
    works \
        .withColumn("detection_output", detect_udf(col("title"))) \
        .select(col("id"), col("publication_year"), col("title"), col("detection_output.*")) \
        .createOrReplaceTempView("works_detected")

    spark.sql("""CREATE TEMPORARY VIEW filtered_with_years
            USING avro
            OPTIONS(path \"hdfs://open1:9000/avro_files/venues_50_years.avro\")""")

    spark.sql("""CREATE TEMPORARY VIEW works_host_venues
            USING avro
            OPTIONS(path \"hdfs://open1:9000/avro_files/works_host_venues.avro\")""")

    spark.sql("""CREATE TEMPORARY VIEW works_referenced_works
        USING avro
        OPTIONS(path \"hdfs://open1:9000/avro_files/works_referenced_works.avro\")""")

    final_dataframe = spark.sql("""
    select 
        filtered_with_years.venue_id, 
        filtered_with_years.year, 
        works_detected1.id,
        works_referenced_works.referenced_work_id,
        works_detected2.title,
        works_detected2.language,
        works_detected2.confidence
    from filtered_with_years
    join works_host_venues 
        on filtered_with_years.venue_id = works_host_venues.venue_id
    join works_detected works_detected1
        on works_host_venues.work_id = works_detected1.id and filtered_with_years.year = works_detected1.publication_year
    left join works_referenced_works
        on works_detected1.id = works_referenced_works.work_id
    left join works_detected works_detected2
        on works_referenced_works.referenced_work_id = works_detected2.id
    """)

    print((final_dataframe.count(), len(final_dataframe.columns)))
    final_dataframe.show(1000, False)

    spark.stop()


if __name__ == "__main__":
    main()
