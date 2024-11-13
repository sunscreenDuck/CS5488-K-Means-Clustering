import csv.ReadCSV
import kmeans.{AdvancedPlayerClustering, RookiesClustering}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NBA KMeans Clustering")
      .config("spark.master", "local")
      .getOrCreate()

    val reader = new ReadCSV(spark)
    val adv = reader.advanced()
    val rookies = reader.rookies()

    AdvancedPlayerClustering.kMeans(adv)
    RookiesClustering.kMeans(rookies)
  }
}
