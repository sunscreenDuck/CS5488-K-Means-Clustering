import csv.ReadCSV
import kmeans.StandardPlayerClustering
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("NBA KMeans Clustering")
      .config("spark.master","local")
      .getOrCreate()

    val reader = new ReadCSV(spark)
    val std = reader.standard()
    val adv = reader.advanced()
    val rookies = reader.rookies()
    std.show()

    val kMeans = new StandardPlayerClustering(std, adv, spark)
    kMeans.kMeans()
  }
}
