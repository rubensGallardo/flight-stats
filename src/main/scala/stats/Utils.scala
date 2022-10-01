package stats

import org.apache.spark.sql.{DataFrame, SparkSession}

class Utils {

  def readCsvFile(inputPath: String, spark: SparkSession): DataFrame = {
    println("Reading file in the path", inputPath)
    spark.read.
      option("header", "true").
      option("inferSchema", "true").
      csv(inputPath)
  }

  def writeCsvFile(path: String, csvDf: DataFrame, spark: SparkSession): Unit = {
    csvDf
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .mode("overwrite")
      .save(path)

  }
}
