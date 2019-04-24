package cs455.spark.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object MostTimeOfDay {
  val ORDER_PROD_SET = "order_*.csv"
  val ORDERS = "orders.csv"
  val PRODUCTS = "products.csv"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val directory = args(0)
    val output = args(1)
    val orders_products = directory + ORDER_PROD_SET
    val orders = directory + ORDERS
    val products = directory + PRODUCTS
    var master = "yarn"

    if(args.length > 2) master = "local"

    val spark = SparkSession
      .builder
      .appName("TopFoodByHour")
      .master(master)
      .getOrCreate()

    findMostCommonByHour(spark, orders, orders_products, products, output)
  }

  def findMostCommonByHour(spark : SparkSession, str_orders : String, order_to_products : String, product_info : String, output : String): Unit = {
    //load in all order data
    val orders = spark.read.format("csv").option("header", "true").load(str_orders)
    val order_products = spark.read.format("csv").option("header", "true").load(order_to_products)

    //load product data
    val products = spark.read.format("csv").option("header", "true").load(product_info)

    //joined order data
    var joined = orders.join(order_products, Seq("order_id"))

    //select necessary columns from the main dataset
    joined = joined.selectExpr("order_id", "order_hour_of_day", "product_id")

    //collect counts for each occurence of each product at each hour of the day
    joined = joined.groupBy("product_id","order_hour_of_day").count()

    //order dataframe into partitions for each hour of day and sort count in descending order for each hour
    //drop all but top value for each column
    joined = joined.withColumn("row", row_number().over(Window.partitionBy(col("order_hour_of_day")).orderBy(col("count").desc)))
      .where(col("row") <= 10).drop("row")

    //join with products in order to output product names
    val top_products = joined.join(products, Seq("product_id"), "inner").selectExpr("order_hour_of_day", "count", "product_name")

    top_products.rdd.repartition(1).saveAsTextFile(output)
  }
}
