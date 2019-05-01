package cs455.spark.basic

import org.apache.spark.ml.feature.{Normalizer, _}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object NutrientProjection {



  val interestedNutrients = Set(203,204,205)

  def main(args: Array[String]): Unit = {

    val instacartFolderPath = args(0)
    val bfpdFolderPath = args(1)
    val linkPath = args(2)
    val output = args(3)

    val spark = SparkSession
      .builder
      .appName("PCA")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.broadcast(interestedNutrients)

    val nutrientsSchema = StructType(
      List(
        StructField("NDB_No", StringType, nullable = true),
        StructField("Nutrient_Code", IntegerType, nullable = true),
        StructField("Nutrient_Name", StringType, nullable = true),
        StructField("Derivation_Code", StringType, nullable = true),
        StructField("Output_value", FloatType, nullable = true),
        StructField("Output_uom", StringType, nullable = true)
      )
    )

    val servingSizeSchema = StructType(
      List(
        StructField("NDB_No", StringType, nullable = true),
        StructField("Serving_Size", FloatType, nullable = true),
        StructField("Serving_Size_UOM", StringType, nullable = true),
        StructField("Household_Serving_Size", StringType, nullable = true),
        StructField("Household_Serving_Size_UOM", StringType, nullable = true),
        StructField("Preparation_State", StringType, nullable = true)
      )
    )

    val productsSchema = StructType(
      List(
        StructField("NDB_Number", StringType, nullable = true),
        StructField("long_name", StringType, nullable = true),
        StructField("data_source", StringType,nullable =  true),
        StructField("gtin_upc", StringType, nullable = true),
        StructField("manufacturer", StringType, nullable = true),
        StructField("date_modified", DateType, nullable = true),
        StructField("date_available", DateType, nullable = true),
        StructField("ingredients_english", StringType, nullable = true)
      )
    )

    val nutrients = spark.read.format("csv")
      .option("header", "true")
      .schema(nutrientsSchema)
      .load(bfpdFolderPath+"Nutrients.csv")

    val servingSize = spark.read.format("csv")
      .option("header", "true")
      .schema(servingSizeSchema)
      .load(bfpdFolderPath+"Serving_size.csv")

    val products = spark.read.format("csv")
      .option("header", "true")
      .schema(productsSchema)
      .load(bfpdFolderPath+"Products.csv")



//    val tmp = nutrients.select("Nutrient_Code", "Nutrient_name")
//      .groupBy("Nutrient_Code", "Nutrient_name")
//      .count()
//      .as[(String,String,BigInt)]
//      .collect()
//      .sortBy(tup => -tup._3)
//
//    tmp.foreach(println)

    val nutrientVectors = nutrients.select("NDB_No","Nutrient_Code", "Output_value", "Output_uom")
      .as[(String,Int,Float,String)]
      .filter(tup => interestedNutrients.contains(tup._2) )
      .filter(tup => tup._4 == "g")
      .map(tup => (tup._1, (tup._2, tup._3)))
      .as[(String,(Int,Float))]
      .rdd.groupByKey.mapValues(_.toList)
      .filter( row => row._2.size == interestedNutrients.size )
      .map( row => (row._1, row._2.sortBy( tup => tup._1 )) )
      .toDF("NDB_No", "nutrients")
      .join(servingSize, "NDB_No")
      .select("NDB_No", "nutrients", "Serving_Size", "Serving_Size_UOM")
      .filter( row => row.get(2) != null  && row.get(3) != null && row.get(3) == "g")
      .drop("Serving_Size_UOM")
      .as[(String,List[(Int,Float)],Float)]
      .map( row => (row._1, Vectors.dense(scaleByServingSize(row._2, row._3))) )
      .toDF("NDB_No", "nutrients")

//    nutrientVectors.take(50).foreach(println)
//    println(nutrientVectors.count())

    val normalizer = new Normalizer()
      .setInputCol("nutrients")
      .setOutputCol("features")
      .setP(1.0)

    val normData = normalizer.transform(nutrientVectors)

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("output")
      .setK(2)
      .fit(normData)

    val transformed = pca.transform(normData)

    transformed.withColumnRenamed("NDB_No", "NDB_Number")
      .join(products, "NDB_Number")
      .select("long_name", "output")
      .rdd.map( entry => entry.get(0)+"$,$,"+entry.get(1) ).coalesce(1)
      .saveAsTextFile(output)

  }

  def scaleByServingSize(nutrients: List[(Int,Float)], servingSize: Float): Array[Double] = {
    nutrients.map( nutrient => nutrient._2.toDouble / servingSize ).toArray[Double]
  }

}





















