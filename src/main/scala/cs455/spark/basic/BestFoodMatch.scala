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

    val directory = args(0)
    val output = args(1)
    val usda_products = directory + USDA_PRODUCTS
    val insta_products = directory + INSTACART_PRODUCTS
    var master = "yarn"

    if (args.length > 2) master = "local"

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
      if (cart_val > .1) Tuple2( Array(row._1.get(0),row._1.get(1)), Array(row._2.get(0),row._2.get(1), cart_val))
      else Tuple2(Array(),Array())
    }).filter(row => row._1.nonEmpty)
    cartesian_result.foreach(arr => {
      println(arr._1.mkString(" "))
      println(arr._2.mkString(" "))
    })
    val reduced_cartesian = cartesian_result.reduceByKey((a, b) => if (a(2).toString.toDouble > b(2).toString.toDouble) a else b )
    reduced_cartesian.foreach(arr => {
      println(arr._1.mkString(" "))
      println(arr._2.mkString(" "))
    })
  }

  def printWithSpace(): Unit = {

  }

  def cartesianFilter(arr1 : Array[String], arr2 : Array[String]): Double ={
    return arr1.intersect(arr2).length.toDouble / arr1.union(arr2).length.toDouble
  }

}
