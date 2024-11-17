package kmeans

import cluster_builder.ClusterBuilder
import csv.WriteCSV
import evaluator.Evaluator
import normalize.Normalizer
import optimize.ClusteringOptimization
import org.apache.spark.sql.DataFrame
import preprocess.DataCleanser


object RookiesClustering {
  private final val columns = Array("Age", "Yrs", "G", "MP", "FG", "FGA", "3P", "3PA", "FT", "FTA", "ORB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS", "FG%", "3P%", "FT%")
  private final val _type = "rookies"

  def kMeans(df: DataFrame): Unit = {
    val cleanDf = DataCleanser.rookieDfPreprocess(df)
    cleanDf.show(20, truncate = false)
    val featureDf = Normalizer.normalize(cleanDf, columns)
    ClusteringOptimization.optimize(featureDf, _type)

    // from the optimize analysis, we determined the optimal number of clusters are 5
    val optimalNumOfClusters = 3
    val (predictions, plotData) = ClusterBuilder.build(featureDf, optimalNumOfClusters, _type, columns)
    WriteCSV.writeDataFrame(plotData, s"src/main/resources/cluster-plot-data/${_type}_player.csv")
    val silhouette = Evaluator.silhouette(predictions)
    println(s"rookies player clustering silhouette score: $silhouette")
  }
}
