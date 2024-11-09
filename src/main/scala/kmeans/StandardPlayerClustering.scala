package kmeans

import csv.WriteCSV
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{PCA, VectorAssembler}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import preprocess.StandardPlayerDataPreProcess


class StandardPlayerClustering(std: DataFrame, adv: DataFrame, spark: SparkSession) {
  private val cleanser = new StandardPlayerDataPreProcess(std, adv);
  private val headValue = udf((arr: org.apache.spark.ml.linalg.Vector) => arr.toArray(0))
  private val tailValue = udf((arr: org.apache.spark.ml.linalg.Vector) => arr.toArray(1))

  private def normalize(df: DataFrame, features: Array[String]): DataFrame = {
    println("normalize:: start")
    val assembler = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("features")
    val ft = assembler.transform(df)
    println("normalize:: features df:")
    ft.show(truncate = false)
    ft
  }


  private def cluster(ftDf: DataFrame): DataFrame = {
    println("cluster:: start")
    val kMeans = new KMeans().setK(8).setSeed(1)
    val model = kMeans.fit(ftDf)
    val clusterCenters = model.clusterCenters
    for (i <- clusterCenters.indices) {
      println(s"cluster:: cluster-$i, ${clusterCenters(i)}")
    }

    // pca
    val predictions = model.transform(ftDf)
    val pca = new PCA().setInputCol("features").setOutputCol("pca_features").setK(2)
    val pcaModel = pca.fit(predictions)
    val pcaResult = pcaModel.transform(predictions)
    println(pcaResult.columns.mkString("Array(", ", ", ")"))

    val test = pcaResult.select("pca_features")
    test.show(truncate = false)
    pcaResult.printSchema()


    val plotData = pcaResult.select("Player", "pca_features", "prediction")
      .withColumn("x", headValue(pcaResult("pca_features")))
      .withColumn("y", tailValue(pcaResult("pca_features")))
      .select("Player", "x", "y", "prediction")
    plotData.show(truncate = false)
    println("cluster:: end")
    plotData
  }

  def kMeans(): Unit = {
    println("kMeans:: start")
    // pre process
    val nbaDf = cleanser.process()
    nbaDf.show(20, truncate = false)
    val featureColumns = nbaDf.columns.drop(1)
    println(s"kMeans:: feature columns - ${featureColumns.mkString("Array(", ", ", ")")}")

    // normalize
    val featuresDf = normalize(nbaDf, featureColumns)
    val plotData = cluster(featuresDf)
    val fileName = "standard_player.csv"
    val writer = new WriteCSV(plotData, fileName)
  }
}
