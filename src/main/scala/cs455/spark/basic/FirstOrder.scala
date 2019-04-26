package cs455.spark.basic

import org.apache.spark.sql.SparkSession

/**
  * What is the first item people put in their carts most often?
  * Does this change with time of day? Do returning users always
  * put the same item in their cart first/How often do they put
  * the same item in their cart first?
  *
  * @author stock
  */
object FirstOrder {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("FirstOrder")
      .getOrCreate()

    import spark.implicits._

    val instacartFolderPath = args(0)
    val bfpdFolderPath = args(1)
    val linkPath = args(2)
    val output = args(3)

    // Read in order data as DataFrame
    val orders = spark.read.format("csv").option("header", "true")
      .load(instacartFolderPath + "order_products__*.csv")

    // Get all first item products added to cart
    val filtered = orders.filter($"add_to_cart_order" === 1)
      .groupBy("product_id").count().sort($"count".desc)

    // Read in product data as a DataFrame
    val instaProducts = spark.read.format("csv")
      .option("header", "true")
      .load( instacartFolderPath + "products.csv" )
      .drop("aisle_id", "department_id")

    val bfpdProducts = spark.read.format("csv")
      .option("header", "true")
      .load( bfpdFolderPath + "Products.csv" )

    val link = spark.read.format("csv")
      .option("header", "true")
      .load( linkPath )
      .select("product_id", "NDB_Number")

    // Join the two DataFrames on product_id
    val joint = filtered
      .join(instaProducts, "product_id")
      .join(link, "product_id")
      .join(bfpdProducts, "NDB_Number")

    joint.rdd.coalesce(1).saveAsTextFile(output)

  }

}