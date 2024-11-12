package kmeans

import cluster.ClusterBuilder
import csv.WriteCSV
import evaluator.Evaluator
import normalize.Normalizer
import optimize.ClusteringOptimization
import org.apache.spark.sql.DataFrame
import preprocess.DataCleanser


class StandardPlayerClustering(adv: DataFrame) {
  private final val resourcesFolder = "src/main/resources"


  def kMeans(): Unit = {
    val df = DataCleanser.process(adv)
    df.show(20, truncate = false)
    val columns = Array("Age", "G", "MP", "PER", "TS%", "3PAr", "FTr", "ORB%", "DRB%", "TRB%", "AST%", "STL%", "BLK%", "TOV%", "USG%", "OWS", "DWS", "WS", "WS/48", "OBPM", "DBPM", "BPM", "VORP")
    val featureDf = Normalizer.normalize(df, columns)
    ClusteringOptimization.optimize(featureDf)

    // from the optimize analysis, we determined the optimal number of clusters are 5
    val optimalNumOfClusters = 5
    val (predictions, plotData) = ClusterBuilder.build(featureDf, optimalNumOfClusters)
    WriteCSV.writeDataFrame(plotData, s"${resourcesFolder}/standard_player.csv")
    val silhouette = Evaluator.silhouette(predictions)
    println(s"standard player clustering silhouette score: $silhouette")
  }
}
