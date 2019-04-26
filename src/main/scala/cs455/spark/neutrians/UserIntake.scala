package cs455.spark.neutrians

import org.apache.spark.sql.SparkSession

/**
  *
  * @author stock
  */
object UserIntake {

  val ORDER_PROD_SET = "order_train.csv"
//  val ORDER_PROD_SET = "order_products__*.csv"
  val ORDERS = "orders.csv"

  val NUTRIENTS = "Nutrients.csv"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("FirstOrder")
      .master("local")
      .getOrCreate()

    val instacart = args(0)
    val usda = args(1)
    val linked = args(2)
    val output = args(3)

    driver(spark, instacart, usda, linked, output)
  }

  def driver(spark : SparkSession, instacart: String,
             usda: String, linked: String, output: String): Unit = {
    import spark.implicits._

    // Read in order data as DataFrame
    val order_set = spark.read.format( "csv" ).option( "header", "true" )
      .load( instacart + ORDER_PROD_SET ).selectExpr( "order_id", "product_id" )

    // Read in users data as a DataFrame
    val users = spark.read.format( "csv" ).option( "header", "true" )
      .load( instacart + ORDERS ).selectExpr( "order_id", "user_id" )

    val joint = order_set.join( users, "order_id" ).drop( "order_id" )

//    joint.show()

    // Read in users data as a DataFrame
    val nutrients = spark.read.format( "csv" ).option( "header", "true" )
      .load( usda + NUTRIENTS ).drop("Nutrient_Code").rdd.flatMap( row =>
    {
      if ( row( 1 ).equals("Sugars, total") )
        List( ( row( 0 ).toString, row( 3 ).toString ) )
      else
        None
    } ).toDF("NDB_Number", "sugar")

    nutrients.show()
//    val jointRDD = joint.rdd.map( row =>
//    {
//      (row(1), row(0))
//    } ).groupByKey.mapValues( _.toList )
//
//    jointRDD.foreach(println)


//    joint.rdd.coalesce(1).saveAsTextFile(output)
  }
}