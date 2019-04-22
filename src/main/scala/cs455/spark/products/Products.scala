import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

class Product

object Products {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("products")
      .getOrCreate()

    val schema: StructType = new StructType()
      .add("product_id", IntegerType)
      .add("product_name", StringType)
      .add("aisle_id", IntegerType)
      .add("department_id", IntegerType)

    val dataFrame = spark.sqlContext.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(args(0))

    dataFrame.createTempView("products")

    val names = dataFrame.select("SELECT product_id FROM products WHERE product_id BETWEEN 100 AND 200")

    names.take(10).foreach(println)

  }

}
