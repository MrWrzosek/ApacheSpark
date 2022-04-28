package somedataanalysis


import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import SparkSessionExt.SparkSessionMethods
import org.apache.spark.sql.types.{IntegerType, StringType}

object SomeDataAnalysis {
    
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
            .appName("Some Data Analysis")
            .master("local")
            .getOrCreate()
        
        val weekDays = spark.createDF(
            List(
                (1, "Sunday"),
                (2, "Monday"),
                (3, "Tuesday"),
                (4, "Wednesday"),
                (5, "Thursday"),
                (6, "Friday"),
                (7, "Saturday")
            ), List(
                ("day_id", IntegerType, true),
                ("day_name", StringType, true)
            )
        )
        
        weekDays.show()
        
        val orders: DataFrame = loadCSVDataFrame(spark, "instacart/orders.csv")
        val departments: DataFrame = loadCSVDataFrame(spark, "instacart/departments.csv")
        val products: DataFrame = loadCSVDataFrame(spark, "instacart/products.csv")
        
        orders
            .groupBy(col("order_hour_of_day"))
            .agg(count(col("order_id")))
            .orderBy(col("order_hour_of_day"))
            .show()

        orders
            .join(weekDays, orders("order_dow") === weekDays("day_id"), "inner")
            .groupBy(col("day_name"))
            .agg(count(col("order_id")).as("Order Count"))
            .orderBy(col("Order Count"))
            .show()

        products
            .join(departments, products("department_id") === departments("department_id"), "inner")
            .groupBy(departments("department"))
            .agg(count(products("product_id")).as("count"), (count(products("product_id"))/sum(products("product_id"))).as("procent"))
            .orderBy("count")
            .show()

        spark.stop()
    
    }
}
