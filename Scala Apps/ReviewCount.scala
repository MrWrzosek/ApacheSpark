package reviewcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReviewCount {
    
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Word Count").setMaster("local")
        val sc = SparkContext.getOrCreate(conf)
    
        val textFile: RDD[String] = sc.textFile("ml-latest-small/ratings.csv")
    
        val lines: RDD[Array[String]] = textFile.map(line => line.split(","))
        val pairs: RDD[(String, Int)] = lines.map(word => (word(1), 1))
        val counts: RDD[(Int, String)] = pairs.reduceByKey(_+_).map(word => (word._2, word._1))
        val sorted: RDD[(Int, String)] = counts.sortByKey(false)
        sorted.foreach(println)
        sc.stop()
    }
}
