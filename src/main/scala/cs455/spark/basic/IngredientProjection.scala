package cs455.spark.basic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature._

object IngredientProjection {

  val inputDim = 200
  val ingredientInvScaleF = 0.75d
  val takeIngredientCount = 5
  val requireIngredientCount = 3

  def main(args: Array[String]): Unit = {

    val instacartFolderPath = args(0)
    val bfpdFolderPath = args(1)
    val linkPath = args(2)
    val output = args(3)

    val spark = SparkSession
      .builder
      .appName("PCA")
      .getOrCreate()

    import spark.implicits._

    val schema = StructType(
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

    val products = spark.read.format("csv")
      .option("header", "true")
      .load(bfpdFolderPath+"Products.csv")

    val ingredients = products.select("ingredients_english")
      .as[String]
      .filter(_ != null)
      .flatMap(str => splitIngredients(str).take(5))
//      .flatMap(str => splitIngredients(str))
      .filter(str => str != "" )
      .toDF("ingredients_english")
      .groupBy("ingredients_english")
      .count().as[(String,BigInt)]

    val sortedIngredients = ingredients.collect().sortBy(row => -row._2)

    sortedIngredients.take(inputDim).foreach(println)

    val array = sortedIngredients.take(inputDim).map(_._1)
    val idxMap = (array zip array.indices).toMap
    spark.sparkContext.broadcast(idxMap)

    val data = products.select("NDB_Number", "ingredients_english")
      .as[(String,String)]
      .filter(row => row._2 != null)
      .map( row => (row._1, splitIngredients(row._2)) )
      .filter( row => containsEnoughIngredients(row._2, idxMap))
      .map( row => (row._1, getVector(row._2, idxMap)) )
      .toDF("NDB_Number", "ingredients" )

//    data.select("ingredients").take(inputDim).foreach(println)
//    println(data.count())

    val normalizer = new Normalizer()
      .setInputCol("ingredients")
      .setOutputCol("features")
      .setP(1.0)

    val normData = normalizer.transform(data)

//    normData.show()

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("output")
      .setK(2)
      .fit(normData)

    val transformed = pca.transform(normData)

//    data.show(100)
//    transformed.show(100)
//
    transformed.join(products, "NDB_Number").select("long_name", "output", "ingredients_english")
      .rdd.map(entry => entry.get(0)+"$,$,"+entry.get(1)+"$,$,"+entry.get(2)).coalesce(1).saveAsTextFile(output)

    println(transformed.count)
  }

  def getVector(ingredients: Array[String], idxMap: Map[String, Int]): org.apache.spark.ml.linalg.Vector = {
    var idx = 0
    // this sucks but flatMap doesn't want to work
    val tupleArr = ingredients.distinct.map( entry => {
      idx += 1
      (idxMap.getOrElse(entry, -1), 1.0d/math.pow(idx,ingredientInvScaleF))
    })
      .filter(tup => tup._1 != -1)
      .sortWith((a,b) => a._1 < b._1)

    Vectors.sparse(inputDim, tupleArr)
  }

  def splitIngredients(ingredients: String): Array[String] = {

    val filteredWordSet = Set("for", "as", "less", "and", "of", "to", "from", "in", "no", "with", "or", "an", "each",
      "the", "following", "than", "protect", "flavor","color","added","natural","grade",
      "may", "low", "organic", "enriched", "unbleached", "bleached", "ingredients", "processed", "degermed", "prepared",
      "contains", "nonfat", "extra", "virgin", "modified", "pasteurized", "purified", "cultured", "high", "consist",
      "pure", "filtered", "product", "mechanically", "distilled", "fresh", "cured")

    val replaceMap = Map(("cornstarch","corn starch"),("skimmilk","milk"),("wholemilk","milk"),
      ("milkfatmilk","milk"), ("reducedfatmilk","milk"), ("partskimmilk","milk"),("lowfatmilk", "milk"),
      ("fatfreemilk", "milk"),("cornmeal","corn meal"),("almondmilk", "almond milk"),("coconutmilk"," coconut milk"),
      ("fatmilk","milk"),("cowsmilk", "milk"))

    // don't deal with things that have ingredients in the string because who knows where the ingredients actually start
    if(ingredients.toLowerCase.contains("ingredients"))
      return new Array[String](0)

    ingredients.toLowerCase.split("[,)(:\\[\\]]")
      .map(str => str.replaceAll("[^\\w ]", "") )
      .map(ingredient => {

        val tmp1 = ingredient.split(" ")
          .filter(word => word.length > 2)
          .filter(word => !filteredWordSet.contains(word))
          .mkString(" ")

        val tmp2 = tmp1.replaceAll("[^\\w]", "")
        if(replaceMap.contains(tmp2)) {
          replaceMap(tmp2)
        } else {
          tmp1
        }

      })

  }

  def containsEnoughIngredients(ingredients: Array[String], idxMap: Map[String,Int]): Boolean = {
    if(ingredients.isEmpty || !idxMap.contains(ingredients(0)))
      return false

    ingredients.take(takeIngredientCount).count(thing => idxMap.contains(thing)) >=  math.min(ingredients.length, requireIngredientCount)
  }


  //  def getIngredientTuples(ingredients: Array[String], idxMap: Map[String, Int]): Array[(Int, Double)] = {
//    ingredients.distinct.flatMap(elem => if(idxMap.contains(elem)) elem else None)
//
//    ingredients
//      .sorted.map(elem => (elem, 1.0))
//
//  }


}
