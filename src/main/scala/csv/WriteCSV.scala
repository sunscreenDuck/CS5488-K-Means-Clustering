package csv

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.File

class WriteCSV(data: DataFrame, fileName: String) {
  private val srcDir = "/tmp/address"
  private def process(): Unit = {
    // Write the DataFrame to CSV
    data.repartition(1)
      .write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("charset", "UTF-8")
      .csv(srcDir)

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val srcPath = new Path(srcDir)
    val destPath = new Path(s"/tmp/$fileName")

    // List the files in the output directory
    val srcFiles = FileUtil.listFiles(new File(srcDir))
    val csvFiles = srcFiles.filter(_.getName.endsWith(".csv"))

    // Check if any CSV files were created
    if (csvFiles.nonEmpty) {
      FileUtil.copy(new File(csvFiles.head.getPath), hdfs, destPath, true, hadoopConfig)
    } else {
      println("No CSV files found in the output directory.")
    }

    // Cleanup
    hdfs.delete(srcPath, true)
    hdfs.delete(new Path(s"/tmp/.$fileName.crc"), true)
  }

}
