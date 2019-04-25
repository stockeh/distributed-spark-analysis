package cs455.spark.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object BestFoodMatch {

  val USDA_PRODUCTS = "Products.csv"

  val INSTACART_PRODUCTS = "products.csv"

  def main(args: Array[String]): Unit = {
    Logger.getLogger( "org" ).setLevel( Level.ERROR )
    Logger.getLogger( "akka" ).setLevel( Level.ERROR )
    if ( args.length < 4 )
    {
      return
    }
    val usda_products = args( 1 ) + USDA_PRODUCTS
    val insta_products = args( 0 ) + INSTACART_PRODUCTS

    val output = args( 2 )
    val master = args( 3 )

    val spark = SparkSession
      .builder
      .appName( "TopFoodByHour" )
      .master( master )
      .getOrCreate()

    findBestMatches( spark, usda_products, insta_products, output )
  }

class MatchTuple()

  val THRESHOLD = 0.19

  def jaccardIndex(arr1 : Array[String], arr2 : Array[String]): Double = {
    arr1.intersect( arr2 ).length.toDouble / arr1.union( arr2 ).length.toDouble
  }

  def findBestMatches(spark : SparkSession, usda_products : String,
                      insta_products : String, output : String): Unit = {
    val usda = spark.read.format( "csv" ).option( "header", "true" ).load( usda_products )
      .selectExpr( "NDB_Number","long_name" ).rdd
    val instacart = spark.read.format( "csv" ).option( "header", "true" ).load( insta_products )
      .selectExpr( "product_id","product_name" ).rdd

    var top = List( ( 0, 0, THRESHOLD ) )

    // output the values as ( insta-product-key , usda-product-key , top-jaccard-value )
    val cartesian_result = instacart.cartesian( usda ).flatMap( row =>
    {
      val insta_product = row._1.get( 1 ).toString.toUpperCase.split(" ")
      val usda_product = row._2.get( 1 ).toString.toUpperCase.split(" ")
      val cart_val = jaccardIndex( insta_product, usda_product )

      val id = row._1.get( 0 ).toString.toInt

      // store highest jaccard value for current ID
      if ( id == top.head._1 && cart_val > top.head._3 )
      {
        top = List( ( id, row._2.get( 0 ).toString.toInt, cart_val ) )
      }

      val output = top
      var emit_output = false

      // "possibly" emit the output for a given ID
      if ( id != top.head._1 )
      {
        // remove this and emit_output if we want every key to have the "best" match
        if ( output.head._3 > THRESHOLD )
          emit_output = true

        top = List( ( id, row._2.get( 0 ).toString.toInt,
          if ( cart_val > THRESHOLD ) cart_val else THRESHOLD ) )
      }

      if ( emit_output )
        output
      else
        None
    })

//    cartesian_result.foreach( println )
    cartesian_result.coalesce( 1 ).saveAsTextFile( output )
  }
}
