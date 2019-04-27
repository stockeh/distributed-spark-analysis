package cs455.spark.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.round

import scala.Long

//case class UserProduct(user_id : Int, product_id : Int, rating : Int)
object CollaborativeFilteringRecommender {
  val ORDER_PROD_SET = "order_*.csv"

  val ORDERS = "orders.csv"

  val PRODUCTS = "products.csv"

  def main(args: Array[String]): Unit = {
    Logger.getLogger( "org" ).setLevel( Level.ERROR )
    Logger.getLogger( "akka" ).setLevel( Level.ERROR )

    val directory = args( 0 )
    val output = args( 1 )
    val orders_products = directory + ORDER_PROD_SET
    val orders = directory + ORDERS
    val products = directory + PRODUCTS
    val master = args( 2 )

    val spark = SparkSession
    .builder
    .appName( "CollaborativeFilteringRecommender" )
    .master( master )
    .getOrCreate()

    tryCombos(spark, orders, orders_products, products, 2000, output)

  }

  def setupUserProductCounts(spark : SparkSession, str_orders : String, str_orders_products : String, num_users : Int) : DataFrame = {
    var schema = StructType(Array(StructField("order_id", IntegerType, nullable = true), StructField("user_id", IntegerType, nullable = true)))
    var schema2 = StructType(Array(StructField("order_id", IntegerType, nullable = true), StructField("product_id", IntegerType, nullable = true)))

    val orders = spark.read.format("csv").option("header", "true").schema(schema).load(str_orders)
    val order_products = spark.read.format("csv").option("header", "true").schema(schema2).load(str_orders_products)
    import spark.implicits._
    //join orders datasets
    val orders_set = orders.join(order_products, Seq("order_id"))

    val user_counts = orders_set.groupBy( "user_id" ).count().orderBy("count").orderBy( $"count".desc ).limit( num_users ).withColumnRenamed( "count", "total_purchased" )
    val product_counts = orders_set.groupBy( "user_id", "product_id" ).count()

    user_counts.join(product_counts, Seq( "user_id" ) )
      .withColumn( "rating",$"count").drop( "count", "total_purchased" )


  }

  def setupProducts(spark : SparkSession, str_products : String): DataFrame = {
    val schema3 = StructType(Array(StructField("product_id", IntegerType, nullable = true), StructField("product_name", StringType, nullable = true)))
    spark.read.format( "csv" ).option( "header", "true" ).schema( schema3 ).load( str_products )
  }

  def tryCombos(spark: SparkSession, str_orders : String, str_orders_products : String, str_products : String, num_users : Int, output : String): Unit = {
    val products = setupProducts(spark, str_products)
    val users_products_counts = setupUserProductCounts(spark, str_orders, str_orders_products, num_users)

    val max_iter = Array(1)
    val reg_param = Array(1.0)
    val rank = Array(1)
    val alpha = Array(1)
    val seed = 12345L
    originalProducts(spark, users_products_counts, products, output+"/topOriginalProductsPerUser/"+num_users)

    var best_params = (max_iter(0), reg_param(0), rank(0), alpha(0))
    var best_rmse = Double.MaxValue
    for( iter <- max_iter) {
      for ( param <- reg_param) {
        for ( r <- rank) {
          for ( a <- alpha) {
            val rmse = collaborativeFilter(spark, users_products_counts, products, num_users, iter,
              r, param, a, seed, output, resolve_output = false)
            if(rmse < best_rmse) {
              best_rmse = rmse
              best_params = (iter, param, r, a)
            }
          }
        }
      }
    }

    collaborativeFilter(spark, users_products_counts, products, num_users, best_params._1,
      best_params._3, best_params._2, best_params._4, seed, output, resolve_output = true)
  }

  def createNewALS(max_iter : Int, reg_param : Double, rank : Int, alpha : Double, num_users : Int, seed : Long): ALS = {
    printf("Creating new ALS:\nTotal Products: %d\nMax Iterations: %d\nReg Param: %f\nRank: %d\nAlpha: %f\n", num_users, max_iter, reg_param, rank, alpha)
    new ALS()
      .setMaxIter( max_iter )
      .setRegParam( reg_param )
      .setRank( rank )
      .setAlpha(alpha)
      .setSeed(seed)
      .setUserCol( "user_id" )
      .setItemCol( "product_id" )
      .setRatingCol( "rating" )
      .setImplicitPrefs(true)
      .setColdStartStrategy( "drop" )
  }

  private def RMSE(model : ALSModel, test : Dataset[Row]): Double = {
    val predictions = model.transform( test )

    val evaluator = new RegressionEvaluator()
      .setMetricName( "rmse" )
      .setLabelCol( "rating" )
      .setPredictionCol( "prediction" )

    val rmse = evaluator.evaluate( predictions )
    println( "Root-mean-square-error = " + rmse )
    rmse
  }

  private def topProductRecsPerUser(spark : SparkSession, model : ALSModel, users_products_counts : DataFrame, products : DataFrame, output : String): Unit = {
    import spark.implicits._

    val topProductRecsPerUser = model.recommendForAllUsers( 400 )
    var topExploded = topProductRecsPerUser
      .select( $"user_id", explode( $"recommendations" ).alias( "recommendation" ) )
      .select( $"user_id", $"recommendation.*" )

    topExploded = topExploded.join( users_products_counts.drop( "rating" ),
      Seq( "user_id", "product_id" ), "leftanti" )

    topExploded.join( products, Seq( "product_id" ), "inner" ).as[(Int,Int,Float,String)].rdd.map(row => {
      (row._2,(row._1,row._3,row._4))
    }).coalesce( 1 ).groupByKey.mapValues( x =>
    {
      x.toList.sortWith( _._2 > _ ._2 ).slice( 0, 10 )
    } ).saveAsTextFile( output + "/topProductsRecsPerUser" )
  }

  private def originalProducts(spark : SparkSession, users_products_counts : DataFrame, products : DataFrame, output : String) : Unit = {
    users_products_counts.join( products, Seq( "product_id" ), "inner" )
      .rdd.map( row =>
    {
      ( row.getAs[Int]( "user_id" ),
        ( row.getAs[Int]( "product_id" ), row.getAs[String]( "product_name" ), row.getAs[Long]( "rating" ) ) )
    } ).coalesce( 1 ).groupByKey.mapValues( x =>
    {
      x.toList.sortWith( _._3 > _ ._3 ).slice( 0, 10 )
    } )
      .saveAsTextFile( output)
  }

  def collaborativeFilter(spark: SparkSession, users_products_counts : DataFrame, products : DataFrame, num_users : Int,
                          max_iter : Int, rank : Int, reg_param : Double, alpha : Double, seed : Long, output : String, resolve_output : Boolean):  Double = {
    val als = createNewALS(max_iter, reg_param, rank, alpha, num_users, seed)

    var Array(training, test) = users_products_counts.randomSplit( Array( 0.80, 0.20 ) )

    val model = als.fit( training )
    val rmse = RMSE(model, test)

    if ( resolve_output )
    {
      topProductRecsPerUser(spark, model, users_products_counts, products, output+"/originalProductsRecsPerUser/"+max_iter+"_"+rank+"_"+reg_param+"_"+alpha+"_"+seed)
    }

    rmse
  }
}
