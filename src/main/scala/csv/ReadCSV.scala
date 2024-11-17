package csv

import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadCSV(spark: SparkSession) {
  def standard(): DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/raw_data/nba-totals-2324.csv")
    .orderBy("Rk")

  def advanced(): DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/raw_data/nba-advanced-2324.csv")
    .orderBy("Rk")

  def rookies(): DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/raw_data/nba-rookies-2324.csv")
    .orderBy("Rk")
}
