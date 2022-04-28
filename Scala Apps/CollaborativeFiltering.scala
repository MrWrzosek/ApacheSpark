package collaborativefiltering

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object CollaborativeFiltering {
    
    
    def loadCSVDataFrame(spark: SparkSession, path: String): DataFrame = {
    
        val df = spark.read.format("csv")
            .option("sep", ",")
            .option("quote", "\"")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(path)
        df
    }
    
    def main(args: Array[String]): Unit = {
        
        val spark: SparkSession = SparkSession
            .builder()
            .appName("Collaborative Filtering")
            .master("local")
            .getOrCreate()
    
        val ratingsDF: DataFrame = loadCSVDataFrame(spark, "ml-latest-small/ratings.csv")
        val moviesDF: DataFrame = loadCSVDataFrame(spark, "ml-latest-small/movies.csv")
        
        val Array(training, test) = ratingsDF.randomSplit(Array(0.8, 0.2))
    
        val als = new ALS()
            .setMaxIter(5)
            .setRegParam(0.01)
            .setUserCol("userId")
            .setItemCol("movieId")
            .setRatingCol("rating")
        
        val model = als.fit(training)
    
        model.setColdStartStrategy("drop")
        val predictions = model.transform(test)
    
        val evaluator = new RegressionEvaluator()
            .setMetricName("rmse")
            .setLabelCol("rating")
            .setPredictionCol("prediction")
        
        val rmse = evaluator.evaluate(predictions)
        println(s"Root-mean-square error = $rmse")
        
        val filteredMovies = ratingsDF
            .groupBy("movieId")
            .agg(count("rating").as("rating_count"))
            .filter(col("rating_count") > 10)
            .select("movieId")
        
        val movieRecs = model.recommendForItemSubset(filteredMovies, 3)
        movieRecs.join(moviesDF, movieRecs("movieId") === moviesDF("movieId"), "inner").select(moviesDF("title"), movieRecs("recommendations")).show(5, false)
        
        spark.stop()
    
    }
}
