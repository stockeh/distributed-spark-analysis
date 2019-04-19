import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.sql.SparkSession

object Basic {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("MultilayerPerceptronClassifierExample")
      .getOrCreate()
    
    val input = args(0)
    val output = args(1)
    
    // Load the data stored in LIBSVM format as a DataFrame.
    val data = spark.read.format("libsvm")
      .load(input)

    // Split the data into train and test
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 1234L)
    val train = splits(0)
    val test = splits(1)
    data.collect.foreach(println)
    
    // specify layers for the neural network:
    // input layer of size 4 (features), three intermediate of size 5
    // and 4 with an output of size 3 (classes)
    val layers = Array[Int](4, 5, 4, 3)

    // create the trainer for 100 epochs and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(80)

    // train the model
    val model = trainer.fit(train)

    // compute accuracy on the test set
    val testresult = model.transform(test)
    val testpredictionAndLabels = testresult.select("prediction", "label")
    val testevaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    println(s"Test set accuracy = ${testevaluator.evaluate(testpredictionAndLabels)}")

    val trainresults = model.transform(train)
    val trainpredictionAndLabels = trainresults.select("prediction", "label")
    val trainevaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    println(s"Train set accuracy = ${trainevaluator.evaluate(trainpredictionAndLabels)}")

    spark.stop()
  }
}
