package normalize

import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.DataFrame

object Normalizer {
  def normalize(df: DataFrame, features: Array[String]): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("features")
    val featureDf = assembler.transform(df.na.drop)
    featureDf.show(truncate = false)
    featureDf
  }
}
