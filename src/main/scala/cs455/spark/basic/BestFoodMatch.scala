package cs455.spark.basic

import cs455.spark.basic.MostTimeOfDay.findMostCommonByHour
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

object BestFoodMatch {
  val USDA_PRODUCTS = "Products.csv"
  val INSTACART_PRODUCTS = "products.csv"
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val directory_insta = args(0)
    val directory_usda = args(1)
    val output = args(2)
    val usda_products = directory_usda + USDA_PRODUCTS
    val insta_products = directory_insta + INSTACART_PRODUCTS
    var master = args(3)

    //if (args.length > 3) master = "local"

    val spark = SparkSession
      .builder
      .appName("TopFoodByHour")
      .master(master)
      .getOrCreate()

    findBestMatches(spark, usda_products, insta_products, output)
  }

class MatchTuple()

  def findBestMatches(spark : SparkSession, usda_products : String, insta_products : String, output : String): Unit = {
    val usda = spark.read.format("csv").option("header", "true").load(usda_products).selectExpr("NDB_Number","long_name").rdd
    val instacart = spark.read.format("csv").option("header", "true").load(insta_products).selectExpr("product_id","product_name").rdd

    val cartesian_rdd = instacart.cartesian(usda)
    val cartesian_result = cartesian_rdd.map(row =>{
      val insta_product = row._1.get(1).toString.toUpperCase.split(" ")
      val usda_product = row._2.get(1).toString.toUpperCase.split(" ")
      val cart_val = cartesianFilter(insta_product, usda_product)
      if (cart_val > .1)
        Tuple2(row._1.get(0).toString.toInt, Array(row._1.get(1),row._2.get(0),row._2.get(1), cart_val))
      else
        Tuple2(0,Array())
    }).filter(row => row._1 != 0)

    val reduced_cartesian = cartesian_result.reduceByKey((a, b) => {
      if (a(3).toString.toDouble > b(3).toString.toDouble)
        a
      else
        b
    } ).map(row => {
      (row._1, row._2(1), row._2(3))
    })

    //reduced_cartesian.foreach(println)
    reduced_cartesian.saveAsTextFile(output)
  }


  def cartesianFilter(arr1 : Array[String], arr2 : Array[String]): Double ={
    return arr1.intersect(arr2).length.toDouble / arr1.union(arr2).length.toDouble
  }

}
