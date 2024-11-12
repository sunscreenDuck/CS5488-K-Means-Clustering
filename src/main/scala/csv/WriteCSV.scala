package csv

import com.github.tototoshi.csv._
import org.apache.spark.sql.DataFrame


object WriteCSV {

  def writeDataFrame(df: DataFrame, fileName: String): Unit = {
    val headerWriter = CSVWriter.open(fileName, append = false)
    headerWriter.writeAll(Seq(df.columns))
    val writer = CSVWriter.open(fileName, append = true)
    writer.writeAll(df.rdd.map(row => row.toSeq)
      .collect().toList)
    writer.close()
  }
}
