package ratingsmean

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RatingsMean {
    
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Ratings Mean").setMaster("local")
        val sc = SparkContext.getOrCreate(conf)
        
        val ratings: RDD[String] = sc.textFile("ml-latest-small/ratings.csv")
        val movies: RDD[String] = sc.textFile("ml-latest-small/movies.csv")
        
        val movies_prepared: RDD[(String, String)] = movies
            .map(line => {
                val arr: Array[String] = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
                (arr(0), arr(1))
            })
        
        val sums_prepared: RDD[(String, Double)] = ratings
            .map(line => {
                val arr: Array[String] = line.split(",")
                try{
                    val rating: Double = arr(2).toDouble
                    (arr(1), rating)
                } catch {
                    case e: NumberFormatException => ("", 0.0)
                }
            })
        
        val sums: RDD[(String, Double)] = sums_prepared.reduceByKey(_+_)
    
        /**
         * Join approach
         */
        
        val count_prepared: RDD[(String, Int)] = ratings
            .map(line => {
                val arr: Array[String] = line.split(",")
                (arr(1), 1)
            })
        
        val counts: RDD[(String, Int)] = count_prepared.reduceByKey(_+_)
        
        val means: RDD[(String, Double)] = sums.join(counts).map(line => (line._1, line._2._1/line._2._2))
        val result: RDD[(String, Double)] = movies_prepared.join(means).map(line => (line._2._1, line._2._2))
        
        result.foreach(println)
    
        /**
         * CombineByKey Approach
         */

        val means2 = sums_prepared.combineByKey(
            v => (v, 1),
            (acc: (Double, Int), v) => (acc._1 + v, acc._2 + 1),
            (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
        ).map{ case (key, value) => (key, value._1 / value._2.toDouble) }
        
        val result2: RDD[(String, Double)] = movies_prepared.join(means).map(line => (line._2._1, line._2._2))
        result2.collectAsMap().foreach(println)
        sc.stop()
    }
}
