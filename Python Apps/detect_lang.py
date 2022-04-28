from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StringType, FloatType, StructField
import cld3


def loadParquet(spark, file_path) -> DataFrame:
    df = spark.read.parquet(f"hdfs://open1:9000{file_path}")
    return df


def detectLanguage(title):
    prediction = cld3.get_language(title)
    if prediction is not None:
        return prediction[0], round(prediction[1], 4)
    return "Unknown", 0.0000


def main():
    spark = SparkSession.builder.appName('Detect Language').getOrCreate()

    schema = StructType([
        StructField("language", StringType(), False),
        StructField("confidence", FloatType(), False)
    ])

    detect_udf = udf(detectLanguage, schema)

    works = loadParquet(spark, "/parquet_files/works.parquet")
    print((works.count(), len(works.columns)))
    works_detected = works \
        .withColumn("detection_output", detect_udf(col("title"))) \
        .select(col("id"), col("publication_year"), col("title"), col("detection_output.*"))

    print((works_detected.count(), len(works_detected.columns)))

    works_detected.show(100, False)
    works_detected.write \
        .mode("overwrite") \
        .parquet(f"hdfs://open1:9000/parquet_files/works_detected.parquet")

    spark.stop()


if __name__ == "__main__":
    main()
