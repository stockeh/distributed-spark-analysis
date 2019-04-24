package cs455.spark.basic

import cs455.spark.basic.MostTimeOfDay.findMostCommonByHour
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

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


  def findBestMatches(spark : SparkSession, usda_products : String, insta_products : String, output : String): Unit = {
    val usda = spark.read.format("csv").option("header", "true").load(usda_products).rdd
    val instacart = spark.read.format("csv").option("header", "true").load(insta_products).rdd

    val cartesian_rdd = instacart.cartesian(usda)

    val cartesian_result = cartesian_rdd.map(row =>{
      val insta_product = row._1.get(1).toString.toUpperCase.split(" ")
      val usda_product = row._2.get(1).toString.toUpperCase.split(" ")

      val cart_val = cartesianFilter(insta_product, usda_product)
      if (cart_val > 0) (row._1, row._2, cart_val)
    })

    cartesian_result.foreach(println)

  }

  def printWithSpace(): Unit = {

  }

  def cartesianFilter(arr1 : Array[String], arr2 : Array[String]): Double ={
    return arr1.intersect(arr2).length.toDouble / arr1.union(arr2).length.toDouble
  }

}
