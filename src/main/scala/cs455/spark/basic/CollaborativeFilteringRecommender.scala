package cs455.spark.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.round

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

    recommend( spark, orders, orders_products, products, output )
  }

  def recommend(spark : SparkSession, str_orders : String, str_orders_products : String, str_products : String, output : String): Unit = {

    var schema = StructType(Array(StructField("order_id", IntegerType, nullable = true), StructField("user_id", IntegerType, nullable = true)))
    var schema2 = StructType(Array(StructField("order_id", IntegerType, nullable = true), StructField("product_id", IntegerType, nullable = true)))

    val orders = spark.read.format("csv").option("header", "true").schema(schema).load(str_orders)
    val order_products = spark.read.format("csv").option("header", "true").schema(schema2).load(str_orders_products)
    import spark.implicits._

    //join orders datasets
    val orders_set = orders.join(order_products, Seq("order_id"))

    val num_users = 2000
    val max_iter = 6
    val reg_param =   1.0
    val rank = 50
    val alpha = 1


    val user_counts = orders_set.groupBy( "user_id" ).count()
      .orderBy( $"count".desc ).limit( num_users ).withColumnRenamed( "count", "total_purchased" )
    val product_counts = orders_set.groupBy( "user_id", "product_id" ).count()
    val users_products_counts = user_counts.join(product_counts, Seq( "user_id" ) )
      .withColumn( "rating",$"count").drop( "count", "total_purchased" )

    val als = new ALS()
      .setMaxIter( max_iter )
      .setRegParam( reg_param )
      .setRank( rank )
        .setAlpha(alpha)
      .setUserCol( "user_id" )
      .setItemCol( "product_id" )
      .setRatingCol( "rating" )
      .setImplicitPrefs(true)
      .setColdStartStrategy( "drop" )

    printf("Total Products: %d\nMax Iterations: %d\nReg Param: %f\nRank: %d\nAlpha: %d\n", num_users, max_iter, reg_param, rank, alpha)

    var Array(training, test) = users_products_counts.randomSplit( Array( 0.80, 0.20 ) )

    val model = als.fit( training )
    val predictions = model.transform( test )

    val evaluator = new RegressionEvaluator()
      .setMetricName( "rmse" )
      .setLabelCol( "rating" )
      .setPredictionCol( "prediction" )

    val rmse = evaluator.evaluate( predictions )
    println( "Root-mean-square-error = " + rmse )

    val topProductRecsPerUser = model.recommendForAllUsers( 400 )

    var topExploded = topProductRecsPerUser
      .select( $"user_id", explode( $"recommendations" ).alias( "recommendation" ) )
      .select( $"user_id", $"recommendation.*" )

    topExploded = topExploded.join( users_products_counts.drop( "rating" ),
      Seq( "user_id", "product_id" ), "leftanti" )

    val schema3 = StructType(Array(StructField("product_id", IntegerType, nullable = true), StructField("product_name", StringType, nullable = true)))
    val products = spark.read.format( "csv" ).option( "header", "true" ).schema( schema3 ).load( str_products )

    val recommendation = topExploded.join( products, Seq( "product_id" ), "inner" ).rdd

    recommendation.map( row =>
      {
        ( row.getAs[Int]( "user_id" ),
        ( row.getAs[Int]( "product_id" ), row.getAs[String]( "product_name" ), row.getAs[Float]( "rating" ) ) )
      } ).coalesce( 1 ).groupByKey.mapValues( x =>
        {
          x.toList.sortWith( _._3 > _ ._3 ).slice( 0, 10 )
        } )
      .saveAsTextFile( output + "/topProductsRecsPerUser" )

    users_products_counts.join( products, Seq( "product_id" ), "inner" )
      .rdd.map( row =>
        {
          ( row.getAs[Int]( "user_id" ),
          ( row.getAs[Int]( "product_id" ), row.getAs[String]( "product_name" ), row.getAs[Int]( "rating" ) ) )
        } ).coalesce( 1 ).groupByKey.mapValues( x =>
          {
            x.toList.sortWith( _._3 > _ ._3 ).slice( 0, 10 )
          } )
          .saveAsTextFile( output+"/originalProductsRecsPerUser" )
  }
}
