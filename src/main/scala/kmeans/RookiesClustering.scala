package kmeans

import cluster.ClusterBuilder
import csv.WriteCSV
import evaluator.Evaluator
import normalize.Normalizer
import optimize.ClusteringOptimization
import org.apache.spark.sql.DataFrame
import preprocess.DataCleanser


object RookiesClustering {
  private final val resourcesFolder = "src/main/resources"
  private final val columns = Array("Age", "Yrs", "G", "MP", "FG", "FGA", "3P", "3PA", "FT", "FTA", "ORB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS", "FG%", "3P%", "FT%", "MP", "PTS", "TRB", "AST", "STL", "BLK")
  private final val _type = "rookies"

  def kMeans(df: DataFrame): Unit = {
    val cleanDf = DataCleanser.rookieDfPreprocess(df)
    cleanDf.show(20, truncate = false)
    val featureDf = Normalizer.normalize(cleanDf, columns)
    ClusteringOptimization.optimize(featureDf, _type)

    // from the optimize analysis, we determined the optimal number of clusters are 5
    val optimalNumOfClusters = 5
    val (predictions, plotData) = ClusterBuilder.build(featureDf, optimalNumOfClusters)
    WriteCSV.writeDataFrame(plotData, s"${resourcesFolder}/rookies_player.csv")
    val silhouette = Evaluator.silhouette(predictions)
    println(s"standard player clustering silhouette score: $silhouette")
  }
}
