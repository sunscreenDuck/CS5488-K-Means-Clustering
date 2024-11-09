package preprocess

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class StandardPlayerDataPreProcess(var sd: DataFrame, var adv: DataFrame) {
  private val pk = "Player"
  private val sk = "Rk"
  private val fk = "Rk"

  private def deduplicate(): Unit = {
    println("deduplicate:: start")
    sd = sd.dropDuplicates(Seq(pk)).orderBy(sk)
    adv = adv.dropDuplicates(Seq(pk)).orderBy(sk)
    println("deduplicate:: end")
  }

  private def dropCol(): Unit = {
    println("dropCol:: start")
    var columns = Seq("Awards", "Player-additional")
    sd = sd.drop(columns: _*).orderBy(sk)
    columns = Seq("_c19", "_c24", "Player-additional")
    adv = adv.drop(columns: _*).orderBy(sk)
    println("dropCol:: end")
  }

  private def join(): DataFrame = {
    println("join:: start")
    val sdAlias = sd.as("std")
    val advAlias = adv.as("adv")
    var joined = sdAlias.join(advAlias, fk)
      .drop(advAlias("Player"), advAlias("Pos"), advAlias("Age"), advAlias("Tm"), advAlias("G"), advAlias("Mp"))
      .orderBy(col(s"std.$sk"))
      .na.fill(0)
    joined = joined.filter(col("G") >= 41 && col("MP") >= 24)
    val columns = Seq("Rk", "Age", "Team", "Pos", "G", "GS", "MP")
    joined = joined.drop(columns: _*)
    println("join:: end")
    joined
  }

  def process(): DataFrame = {
    println("process:: start")
    deduplicate()
    dropCol()
    val nba = join()
    println(s"process:: shape(row:${nba.count()}, columns:${nba.columns.length})")
    println("process:: end")
    nba
  }
}
