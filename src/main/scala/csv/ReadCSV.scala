package csv

import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadCSV(spark: SparkSession) {
  def standard(): DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/nba-totals-2324.csv")
    .orderBy("Rk")

  def advanced(): DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/nba-advanced-2324.csv")
    .orderBy("Rk")

  def rookies(): DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/nba-rookies-2324.csv")
    .orderBy("Rk")
}
