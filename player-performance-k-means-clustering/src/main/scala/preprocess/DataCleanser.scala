package preprocess

import org.apache.spark.sql.DataFrame

object DataCleanser {
  private val pk = "Player"
  private val sk = "Rk"

  def advancedDfPreprocess(advancedPlayerDf: DataFrame): DataFrame = {
    var df = advancedPlayerDf
    df = df.dropDuplicates(Seq(pk))
    val columns = Seq("_c19", "_c24", "Player-additional")
    df = df.drop(columns: _*).orderBy(sk)
    df
  }

  def rookieDfPreprocess(rookieDf: DataFrame): DataFrame = {
    var df = rookieDf
    df = df.dropDuplicates(Seq(pk))
    df = df.drop("-9999").orderBy(sk)
    df
  }
}
