package stats

import org.apache.spark.sql.{DataFrame, SparkSession}

class Utils {

  def readCsvFile(inputPath: String, spark: SparkSession): DataFrame = {
    println("Reading file in the patch %s", inputPath)
    spark.read.
      option("header", "true").
      option("inferSchema", "true").
      csv(inputPath)
  }
}
