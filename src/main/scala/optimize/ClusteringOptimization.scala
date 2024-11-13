package optimize

import breeze.plot._
import com.github.tototoshi.csv._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.DataFrame

object ClusteringOptimization {
  def optimize(featuredDf: DataFrame, _type: String): Unit = {
    // List to store the results
    var results = List(("number_of_clusters", "Silhouette_distance", "WCSS_distance"))
    var silhouetteScores = Seq[(Int, Double)]()
    var wcssScores = Seq[(Int, Double)]()
    val evaluator = new ClusteringEvaluator()

    // Perform K-means clustering for different numbers of clusters and compute Silhouette scores
    for (k <- 2 to 10) {
      val kmeans = new KMeans().setK(k).setSeed(1L)
      val model = kmeans.fit(featuredDf)
      val predictions = model.transform(featuredDf)
      val silhouette = evaluator.evaluate(predictions)

      println(s"Silhouette with squared euclidean distance for K=$k = $silhouette")
      val centroids = model.clusterCenters
      val clusteredData = predictions.select("prediction", "features")

      val squaredDistances = clusteredData.rdd.map { row =>
        val cluster = row.getAs[Int]("prediction")
        val features = row.getAs[org.apache.spark.ml.linalg.Vector]("features")
        val centroid = centroids(cluster)
        math.pow(org.apache.spark.ml.linalg.Vectors.sqdist(features, centroid), 2)
      }
      val wcss = squaredDistances.reduce(_ + _)
      results = results :+ (k.toString, silhouette.toString, wcss.toString)

      silhouetteScores = silhouetteScores :+ (k, silhouette)
      wcssScores = wcssScores :+ (k, wcss)

      // Show the result
      println(s"Cluster Centers K=$k: ")
      model.clusterCenters.foreach(println)
    }

    // Write results to a CSV file
    val outputFilePath = s"src/main/resources/${_type}_silhouette_scores.csv"
    val writer = CSVWriter.open(outputFilePath)
    writer.writeAll(results.map { case (k, s, w) => Seq(k, s, w) })
    writer.close()

    val f = Figure()
    val p = f.subplot(0)
    p += plot(wcssScores.map(_._1.toDouble), wcssScores.map(_._2), name = "WCSS")

    p.xlabel = "Number of Clusters"
    p.ylabel = "WCSS Distance"
    p.title = "Clusters vs WCSS"
    // Customizing the y-axis scale and ticks
    val minWcss = wcssScores.map(_._2).min
    val maxWcss = wcssScores.map(_._2).max
    val tickInterval = (maxWcss - minWcss) / 5
    p.ylim(minWcss - tickInterval, maxWcss + tickInterval)
    p.setYAxisDecimalTickUnits()
    f.saveas(s"src/main/resources/${_type}_wcss_plot.png")
  }
}