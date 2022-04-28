package wikipediagraph

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.graphx._
import org.graphframes.GraphFrame
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer


object WikipediaGraph {
    
    case class Relation(src: String, dst: String)
    implicit def enc: Encoder[Relation] = Encoders.product[Relation]
    
    def main(args: Array[String]): Unit = {
    
        val spark: SparkSession = SparkSession
            .builder()
            .appName("Some Data Analysis")
            .master("local")
            .getOrCreate()
        
        val pages: DataFrame = spark.read
            .format("xml")
            .option("rowTag", "page")
            .load("diabetes.xml")
        
        
        val relationRegex: Regex = "\\[\\[([.[^\\]]]+)\\]\\]".r
        
        val relations: Dataset[Relation] = pages
            .select("title", "revision.text._VALUE")
            .flatMap(fields => {
                val text = fields.getString(1)
                val matches = relationRegex.findAllIn(text)
                val listOfRelations = new ListBuffer[Relation]()
                matches.foreach(element => {
                    if (!(element.startsWith("[[Site:") || element.startsWith("[[Image:") || element.startsWith("[[Category:"))) {
                        listOfRelations += Relation(fields.getString(0), element.split("\\|")(0))
                    }
                })
                listOfRelations.toIterator
            })
        
        val graph: GraphFrame = GraphFrame.fromEdges(relations.toDF())
        graph.vertices.show(1000, false)
        graph.edges.show(1000, false)
        graph.pageRank
            .maxIter(5)
            .resetProbability(0.15)
            .run()
            .vertices
            .show()

        
        spark.stop()
    }
}
