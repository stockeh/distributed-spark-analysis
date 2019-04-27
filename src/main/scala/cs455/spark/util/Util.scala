package cs455.spark.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class Util {

  def filterIDByColumn(spark : SparkSession, food_matches: DataFrame, exclude_deps: Array[Int], column_name : String, insta_location : String, filename : String): DataFrame = {
    val deps_file = insta_location + "/" + filename
    val departments = spark.read.format( "csv" ).option( "header", "true" ).load(deps_file)
    return food_matches.join(departments).filter(row => !exclude_deps.contains(row.getAs[Int](column_name)))
  }

  def filterByFoodDepartments(spark : SparkSession, food_matches : DataFrame, insta_location : String): DataFrame = {
    val exclude_deps = Array(2, 8, 11, 17, 21)
    return filterIDByColumn(spark, food_matches, exclude_deps, "department_id", insta_location, "departments.csv")
  }

  def filterByFoodAisles(spark : SparkSession, food_matches : DataFrame, exlude_aisles: Array[Int], insta_location : String): DataFrame = {
    val exclude_aisles = Array(10, 11, 20, 22, 25, 44, 54, 55, 56, 60, 70, 73, 74, 75, 80, 82, 85, 87, 97, 101, 102, 109, 111, 114, 118, 126, 127, 132, 133)
    return filterIDByColumn(spark, food_matches, exclude_aisles, "aisle_id", insta_location, "aisles.csv");
  }

}
