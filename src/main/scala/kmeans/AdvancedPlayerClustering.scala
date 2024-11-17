package kmeans

import cluster_builder.ClusterBuilder
import csv.WriteCSV
import evaluator.Evaluator
import normalize.Normalizer
import optimize.ClusteringOptimization
import org.apache.spark.sql.DataFrame
import preprocess.DataCleanser


object AdvancedPlayerClustering {
  private final val columns = Array("Age", "G", "MP", "PER", "TS%", "3PAr", "FTr", "ORB%", "DRB%", "TRB%", "AST%", "STL%", "BLK%", "TOV%", "USG%", "OWS", "DWS", "WS", "WS/48", "OBPM", "DBPM", "BPM", "VORP")
  private final val _type = "advanced"

  def kMeans(df: DataFrame): Unit = {
    val cleanDf = DataCleanser.advancedDfPreprocess(df)
    cleanDf.show(20, truncate = false)
    val featureDf = Normalizer.normalize(cleanDf, columns)
    ClusteringOptimization.optimize(featureDf, _type)

    // from the optimize analysis, we determined the optimal number of clusters are 5
    val optimalNumOfClusters = 5
    val (predictions, plotData) = ClusterBuilder.build(featureDf, optimalNumOfClusters, _type, columns)
    WriteCSV.writeDataFrame(plotData, s"src/main/resources/cluster-plot-data/${_type}_player.csv")
    val silhouette = Evaluator.silhouette(predictions)
    println(s"standard player clustering silhouette score: $silhouette")
  }
}
