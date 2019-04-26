package cs455.spark.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.collection.{Map, mutable}
import scala.collection.mutable.ArrayBuffer

//case class UserProduct(user_id : Int, product_id : Int, rating : Int)
object CollaborativeFilteringRecommender {
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
  val master = args(2)



  val spark = SparkSession
  .builder
  .appName("CollaborativeFilteringRecommender")
  .master(master)
  .getOrCreate()

  recommend(spark, orders, orders_products, products, output)
  }

  def recommend(spark : SparkSession, str_orders : String, str_orders_products : String, str_products : String, output : String): Unit = {

    var schema = StructType(Array(StructField("user_id", IntegerType, nullable = true), StructField("order_id", IntegerType, nullable = true)))
    var schema2 = StructType(Array(StructField("product_id", IntegerType, nullable = true), StructField("order_id", IntegerType, nullable = true)))

    val orders = spark.read.format("csv").option("header", "true").schema(schema).load(str_orders)
    val order_products = spark.read.format("csv").option("header", "true").schema(schema2).load(str_orders_products)
    import spark.implicits._
    //load product data
//    val products = spark.read.format("csv").option("header", "true").load(str_products)

    //join orders datasets
    val orders_set = orders.join(order_products, Seq("order_id"))

    val user_counts = orders_set.groupBy("user_id").count()
      .orderBy($"count".desc).limit(100).withColumnRenamed("count", "total_purchased")

    val product_counts = orders_set.groupBy("user_id", "product_id").count()

    val users_products_counts = user_counts.join(product_counts, Seq("user_id"))
      .withColumn("rating", $"count" / $"total_purchased").drop("count", "total_purchased")
//    users_products_counts.printSchema()
//    users_products_counts.show(false)
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("user_id")
      .setItemCol("product_id")
      .setRatingCol("rating")
//      .setColdStartStrategy("drop")


    var Array(training, test) = users_products_counts.randomSplit(Array(.8,.2))
//    training = training.persist(StorageLevel.MEMORY_ONLY)
//    test = test.persist(StorageLevel.MEMORY_ONLY)
    val model = als.fit(training)
    model.save(output + "/model/")

    //model.setColdStartStrategy("drop")
//    val predictions = model.transform(test)
//
//    val evaluator = new RegressionEvaluator()
//      .setMetricName("rmse")
//      .setLabelCol("rating")
//      .setPredictionCol("prediction")
//    val rmse = evaluator.evaluate(predictions)
//    println("Root-mean-square-error = " + rmse)
//
//    val topProductRecsPerUser = model.recommendForAllUsers(10)
//    topProductRecsPerUser.rdd.saveAsTextFile(output+"topProductsRecsPerUser/")
//
//    val topUserRecsPerProduct = model.recommendForAllItems(10)
//    topUserRecsPerProduct.rdd.saveAsTextFile(output+"topUserRecsPerProduct/")
//
//    val users = users_products_counts.select(als.getUserCol).distinct().limit(3)
//    val topProductRecsPerUserSubset = model.recommendForUserSubset(users, 10)
//    topProductRecsPerUserSubset.rdd.saveAsTextFile(output+"topProductRecsPerUserSubset/")
//
//    val items = users_products_counts.select(als.getItemCol).distinct().limit(3)
//    val topUserRecsPerProductSubset = model.recommendForItemSubset(items, 10)
//    topUserRecsPerProductSubset.rdd.saveAsTextFile(output+"topUserRecsPerProductSubset/")
  }



}
