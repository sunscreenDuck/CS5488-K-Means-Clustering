package kmeans

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{PCA, VectorAssembler}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import preprocess.StandardPlayerDataPreProcess

import java.io.File


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


  private def cluster(ftDf: DataFrame): Unit = {
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

    // Export CSV
    val srcDir = "/tmp/address"

    // Write the DataFrame to CSV
    plotData.repartition(1) // Consider the performance implications
      .write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("charset", "UTF-8")// Include header in CSV
      .csv(srcDir)

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)

    // Paths
    val srcPath = new Path(srcDir)
    val destPath = new Path("/tmp/standard_player.csv")

    // List the files in the output directory
    val srcFiles = FileUtil.listFiles(new File(srcDir))
    val csvFiles = srcFiles.filter(_.getName.endsWith(".csv"))

    // Check if any CSV files were created
    if (csvFiles.nonEmpty) {
      // Copy the first CSV file to the destination path
      FileUtil.copy(new File(csvFiles.head.getPath), hdfs, destPath, true, hadoopConfig)
    } else {
      println("No CSV files found in the output directory.")
    }

    // Cleanup
    hdfs.delete(srcPath, true) // Remove the temporary directory
    hdfs.delete(new Path("/tmp/.standard_player.csv.crc"), true) // Remove CRC file if needed

    println("cluster:: end")
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
    cluster(featuresDf)

  }
}
