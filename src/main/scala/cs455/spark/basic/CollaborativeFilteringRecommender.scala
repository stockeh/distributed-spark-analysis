package cs455.spark.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.sql.SparkSession

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
    val orders = spark.read.format("csv").option("header", "true").load(str_orders)
    val order_products = spark.read.format("csv").option("header", "true").load(str_orders_products)
    import spark.implicits._
    //load product data
    val products = spark.read.format("csv").option("header", "true").load(str_products)

    //join orders datasets
    val orders_set = orders.join(order_products, Seq("order_id"))

    //map user id to product ids
    var orders_rdd = orders_set.rdd.map(row => {
      (row.getAs("user_id").toString.toInt, ArrayBuffer(row.getAs("product_id").toString.toInt))
    })

    //reduce by key on users to get list of prodcuts for each user
    orders_rdd = orders_rdd.reduceByKey((p1,p2) => {
     val combined = p1 ++= p2
      combined
    })
    //user,list(product_ids)
    val users_products = orders_rdd.mapValues(list => {
      var fulllist = new Array[Int](49689)
      list.foreach(e => fulllist(e) += 1)
      fulllist.toList
    })
    val user_to_product = users_products.flatMap(v => {
      var list = ArrayBuffer[Rating[Int]]()
      for(i <- v._2.indices) {
        list += Rating(v._1, i, v._2(i))
      }
      list
    }).toDF("user_id", "product_id", "rating")
    user_to_product.show(false)

    val als = new ALS()
      .setMaxIter(1)
      .setRegParam(0.01)
      .setUserCol("user_id")
      .setItemCol("product_id")
      .setRatingCol("rating")


    val Array(training, test) = user_to_product.randomSplit(Array(.0005,.9995))
    val model = als.fit(training)

    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println("Root-mean-square-error = " + rmse)

    val topProductRecsPerUser = model.recommendForAllUsers(10)
    topProductRecsPerUser.rdd.saveAsTextFile(output+"topProductsRecsPerUser/")

    val topUserRecsPerProduct = model.recommendForAllItems(10)
    topUserRecsPerProduct.rdd.saveAsTextFile(output+"topUserRecsPerProduct/")

    val users = user_to_product.select(als.getUserCol).distinct().limit(3)
    val topProductRecsPerUserSubset = model.recommendForUserSubset(users, 10)
    topProductRecsPerUserSubset.rdd.saveAsTextFile(output+"topProductRecsPerUserSubset/")

    val items = user_to_product.select(als.getItemCol).distinct().limit(3)
    val topUserRecsPerProductSubset = model.recommendForItemSubset(items, 10)
    topUserRecsPerProductSubset.rdd.saveAsTextFile(output+"topUserRecsPerProductSubset/")
  }



}
