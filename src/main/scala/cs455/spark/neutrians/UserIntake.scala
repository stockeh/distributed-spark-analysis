package cs455.spark.neutrians

import org.apache.spark.sql.SparkSession

/**
  * Get the average amount of sugars in 100 gram measures on
  * all purchased products per user.
  *
  * @author stock
  */
object UserIntake {

  val ORDER_PROD_SET = "order_*.csv"

  val ORDERS = "orders.csv"

  val NUTRIENTS = "Nutrients.csv"

  val PRODUCTS = "products.csv"

  /**
    * Entry point for the application
    *
    * @param args [ instacart_folder, usada_folder, linked_file,
    *             output_directory ]
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("UserIntake")
        .master("local")
      .getOrCreate()

    val instacart = args(0)
    val usda = args(1)
    val linked = args(2)
    val output = args(3)

    driver(spark, instacart, usda, linked, output)
  }

  /**
    * Driver function containing the logic for this class.
    *
    * @param spark sparkSession
    * @param instacart directory
    * @param usda directory
    * @param linked file
    * @param output directory
    */
  def driver(spark : SparkSession, instacart: String,
             usda: String, linked: String, output: String): Unit = {
    import spark.implicits._

    // Read in order data as DataFrame
    val order_set = spark.read.format( "csv" ).option( "header", "true" )
      .load( instacart + ORDER_PROD_SET ).selectExpr( "order_id", "product_id" )

    // Read in users data as a DataFrame
    val users = spark.read.format( "csv" ).option( "header", "true" )
      .load( instacart + ORDERS ).selectExpr( "order_id", "user_id" )

    // Read in linked data
    val linker = spark.read.format( "csv" ).option( "header", "true" )
      .load( linked ).select( "product_id", "NDB_Number" )

    // Format nutrients on NDB_Number and sugars
    val nutrients = spark.read.format( "csv" ).option( "header", "true" )
      .load( usda + NUTRIENTS ).drop("Nutrient_Code").rdd.flatMap( row =>
      {
        if ( row( 1 ).equals("Sugars, total") )
          List( ( row( 0 ).toString, row( 3 ).toString ) )
        else
          None
      } ).toDF( "NDB_Number", "sugar" )

    // Join datasets to return < user_id, sugar >
    val joint = order_set.join( users, "order_id" ).drop( "order_id" )
      .join( linker, "product_id" ).join( nutrients, "NDB_Number" ).drop( "NDB_Number", "product_id" )

    // Reduce by user_id, and average the sugars for each user
//    joint.rdd.map( row =>
//    {
//      ( row( 0 ), row( 1 ).toString.toDouble )
//    } ).groupByKey.mapValues( _.toList )
//    .map( x =>
//      {
//        x._1 + "," + x._2.sum / x._2.length
//      } ).coalesce( 1 ).saveAsTextFile( output )

    val products = spark.read.format( "csv" ).option( "header", "true" ).load( instacart + PRODUCTS )
      .select( "product_id", "aisle_id" )

    products.join( linker, "product_id" )
      .join( nutrients, "NDB_Number" ).drop( "NDB_Number", "product_id" ).show(false)
  }
}