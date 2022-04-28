import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f


def loadParquet(spark, file_path) -> DataFrame:
    df = spark.read.parquet(f"hdfs://open1:9000{file_path}")
    return df


def loadCSV(spark, file_path):
    df = spark.read \
        .option("header", True) \
        .option("delimiter", ",") \
        .csv(f"hdfs://open1:9000{file_path}")
    return df


def main():
    spark = SparkSession.builder.appName('Merge CSV Authors').getOrCreate()

    authors = loadParquet(spark, "/parquet_files/authors.parquet")
    authors_ids = loadParquet(spark, "/parquet_files/authors_ids.parquet")
    authors_counts_by_year = loadParquet(spark, "/parquet_files/authors_counts_by_year.parquet")

    print((authors.count(), len(authors.columns)))
    print((authors_ids.count(), len(authors_ids.columns)))
    print((authors_counts_by_year.count(), len(authors_counts_by_year.columns)))

    authors_update = loadCSV(spark, "/csv_update/authors_update.csv")
    authors_ids_update = loadCSV(spark, "/csv_update/authors_ids_update.csv")
    authors_counts_by_year_update = loadCSV(spark, "/csv_update/authors_counts_by_year_update.csv")

    cols = ['orcid', 'display_name', 'display_name_alternatives', 'works_count', 'cited_by_count',
            'last_known_institution', 'works_api_url', 'updated_date']
    authors_merged = authors.alias('a') \
        .join(authors_update.alias('b'), "id", how='outer') \
        .select('id', *(f.coalesce('b.' + col, 'a.' + col).alias(col) for col in cols))

    cols = ['openalex', 'orcid', 'scopus', 'twitter', 'wikipedia', 'mag']
    authors_ids_merged = authors_ids.alias('a') \
        .join(authors_ids_update.alias('b'), "author_id", how='outer') \
        .select('author_id', *(f.coalesce('b.' + col, 'a.' + col).alias(col) for col in cols))

    cols = ['works_count', 'cited_by_count']
    authors_counts_by_year_merged = authors_counts_by_year.alias('a') \
        .join(authors_counts_by_year_update.alias('b'), ["author_id", "year"], how='outer') \
        .select('author_id', 'year', *(f.coalesce('b.' + col, 'a.' + col).alias(col) for col in cols))

    print((authors_merged.count(), len(authors_merged.columns)))
    print((authors_ids_merged.count(), len(authors_ids_merged.columns)))
    print((authors_counts_by_year_merged.count(), len(authors_counts_by_year_merged.columns)))

    authors_merged.write.parquet(f"hdfs://open1:9000/parquet_files/authors_updated.parquet")
    authors_ids_merged.write.parquet(f"hdfs://open1:9000/parquet_files/authors_ids_updated.parquet")
    authors_counts_by_year_merged.write.parquet(f"hdfs://open1:9000/parquet_files/authors_counts_by_year_updated.parquet")

    spark.stop()


if __name__ == "__main__":
    main()
