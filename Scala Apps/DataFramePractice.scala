package dataframepractice

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties


object DataFramePractice {
    
    def loadCSVDataFrame(spark: SparkSession, path: String): DataFrame = {
        
        val df = spark.read.format("csv")
            .option("sep", ",")
            .option("quote", "\"")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(path)
        df
    }
    
    case class Movie(genres: String, movieId: Long, title: String)
    
    def main(args: Array[String]): Unit = {
    
        val spark: SparkSession = SparkSession
            .builder()
            .appName("Collaborative Filtering")
            .master("local")
            .getOrCreate()
        
        import spark.implicits._

        val moviesCSV = loadCSVDataFrame(spark, "ml-latest-small/movies.csv")
        val ratingsCSV = loadCSVDataFrame(spark, "ml-latest-small/ratings.csv")
        val moviesJSON = spark.read.json("ml-latest-small/movies.json")

        moviesJSON.printSchema()

        moviesCSV.createOrReplaceTempView("view_moviescsv")
        moviesJSON.createOrReplaceTempView("view_moviesjson")

        spark.sql("SELECT * FROM view_moviescsv WHERE movieId > 500 ORDER BY movieId DESC").show()
        spark.sql("SELECT * FROM view_moviesjson WHERE movieId > 500 ORDER BY movieId DESC").show()

        val grouppedRatingsCSV = ratingsCSV
            .groupBy("movieId")
            .agg(count("rating")
                .as("ratingCount"))
        
        grouppedRatingsCSV.as("df1")
            .join(moviesCSV.as("df2"), col("df1.movieId") === col("df2.movieId"), "inner")
            .select(col("df2.title"), col("df1.ratingCount"))
            .orderBy(col("df1.ratingCount").desc)
            .show()

        val mappedMoviesJSON = moviesJSON.as[Movie]
        mappedMoviesJSON.show()

        val prop = new Properties()
        prop.setProperty("user", "bigdata")
        prop.setProperty("password", "1234")
        prop.setProperty("serverTimezone","UTC")
        
        val jdbcDFMovies = spark.read.jdbc(
            "jdbc:mysql://zecer.wi.zut.edu.pl:3306/bigdata",
            "movies",
            prop
        )
        val jdbcDFRatings = spark.read.jdbc(
            "jdbc:mysql://zecer.wi.zut.edu.pl:3306/bigdata",
            "ratings",
            prop
        )
            
        jdbcDFMovies.show()
        jdbcDFRatings.show()

        spark.stop()
    }
}
