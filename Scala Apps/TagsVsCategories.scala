package tagsvscategories

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TagsVsCategories {
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setAppName("Word Count").setMaster("local")
        val sc = SparkContext.getOrCreate(conf)
    
        val movies: RDD[String] = sc.textFile("ml-latest-small/movies.csv")
        val linesMovies: RDD[Array[String]] = movies.map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
        val genreMovies: RDD[(String, Array[String])] = linesMovies.map(line => (line(0), line(2).split("\\|")))
        val genreMoviesCount: RDD[(String, Int)] = genreMovies.map(line => (line._1, line._2.length))
        //genreMoviesCount.foreach(println)
        
        val genreCounts: RDD[(String, Int)] = genreMovies.flatMap(line => line._2.map(field => (field, 1)))
        val genreRanking: RDD[(String, Int)] = genreCounts.reduceByKey(_+_).sortBy({case (word, count) => count}, false)
        //genreRanking.foreach(println)
    
        val tags: RDD[String] = sc.textFile("ml-latest-small/tags.csv")
        val movieIdTag: RDD[(String, Int)] = tags
            .map(line=>{
                val arr: Array[String] = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
                (arr(1), 1)})
            .reduceByKey(_+_)
        val moviesNames: RDD[(String, String)] = linesMovies.map(line => (line(0), line(1)))
        val movieNameTag: RDD[(String, Int)] = movieIdTag.join(moviesNames).map(line => (line._2._2, line._2._1))
        //movieNameTag.foreach(println)
        
        val result: RDD[String] = movieIdTag.join(genreMoviesCount).filter(line => line._2._1 > line._2._2).join(moviesNames).map(line => line._2._2)
        result.foreach(println)
    
        sc.stop()
    }
}
