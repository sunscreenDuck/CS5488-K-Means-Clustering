package preprocess

import org.apache.spark.sql.DataFrame

object DataCleanser {
  private val pk = "Player"
  private val sk = "Rk"

  def process(advancedPlayerDf: DataFrame): DataFrame = {
    var df = advancedPlayerDf
    df = df.dropDuplicates(Seq(pk)).orderBy(sk)
    val columns = Seq("_c19", "_c24", "Player-additional")
    df = df.drop(columns: _*).orderBy(sk)
    df
  }
}
