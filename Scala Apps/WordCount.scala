package wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    
    def withStopWordsFiltered(rdd: RDD[String],
                              separators: Array[Char] = " ".toCharArray,
                              stopWords: Set[String] = Set("the")): RDD[(String, Int)] = {
        
        val tokens: RDD[String] = rdd.flatMap(_.split(separators).
            map(_.trim.toLowerCase))
        val lcStopWords = stopWords.map(_.trim.toLowerCase)
        val words = tokens.filter(token =>
            !lcStopWords.contains(token) && token.nonEmpty)
        val wordPairs = words.map((_, 1))
        val wordCounts = wordPairs.reduceByKey(_ + _)
        wordCounts
    }
    
    def basicWordCount(rdd: RDD[String]): RDD[(String, Int)] = {
        
        val counts: RDD[(String, Int)] = rdd
            .flatMap(line => line.split(" "))
            .map(word => (word, 1))
            .reduceByKey(_ + _)
            .sortBy({case (word, count) => count}, false)
        counts
    }
    

    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setAppName("Word Count").setMaster("local")
        val sc = SparkContext.getOrCreate(conf)
        
        val textFile: RDD[String] = sc.textFile("LodowyOgrod.txt")
        val counts = basicWordCount(textFile)
        
        counts.foreach(println)
        println("Total words: " + counts.count())
        
        sc.stop()
    }
}
