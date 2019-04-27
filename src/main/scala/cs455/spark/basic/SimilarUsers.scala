package cs455.spark.basic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature._


class SimilarUsers {

  def main(args: Array[String]): Unit = {

    val instacartFolderPath = args(0)
    val bfpdFolderPath = args(1)
    val linkPath = args(2)
    val output = args(3)

    val spark = SparkSession
      .builder
      .appName("SimilarUsers")
      .getOrCreate()


    import spark.implicits._

    val schema = StructType(
      List(
        StructField("order_id", IntegerType),
        StructField("product_id", IntegerType)
      )
    )

    val orders = spark.read.format("csv").option("header", "true").load(instacartFolderPath + "orders.csv").select("order_id", "user_id")

    val order_products = spark.read.format("csv").option("header", "true").schema(schema).load(instacartFolderPath + "order_*.csv")

    val orders_set = orders.join(order_products, "order_id").drop("order_id")

    val product_counts = orders_set.groupBy("user_id", "product_id").count()

    val user_desc = product_counts.drop("count").as[(String, Int)].rdd.groupByKey.mapValues(_.toList)

    val user_desc_vector = user_desc.map(row => (row._1, row._2.map(x => (x, 1.0))))

    val user_vector_df = user_desc_vector.map(row => (row._1, Vectors.sparse(49689, row._2))).toDF("user_id", "keys")


    val mh = new MinHashLSH()
      .setNumHashTables(20)
      .setInputCol("keys")
      .setOutputCol("values")

    val model = mh.fit(user_vector_df)

    val splitDf = user_vector_df.randomSplit(Array(.2, .005))
    val (usersA, usersB) = (splitDf(0), splitDf(1))

    val transformedA = model.transform(usersA).cache()
    val transformedB = model.transform(usersB).cache()

    // println(user_vector_df.take(1)(0))
    val key = Vectors.sparse(49689, Array(5077,11323,14303,20082,22108,46522),Array(1.0,1.0,1.0,1.0,1.0,1.0))
    model.approxNearestNeighbors(transformedA, key, 7)
    model.approxNearestNeighbors(transformedB, key, 7)

    val key2 = Vectors.sparse(49689, Array(4210,5077,22108,46522),Array(1.0,1.0,1.0,1.0))
    model.approxNearestNeighbors(transformedA, key2, 7)

    model.approxSimilarityJoin(transformedA, transformedB, 0.4).coalesce()

  }


}
