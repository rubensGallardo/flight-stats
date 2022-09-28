package stats

import org.scala.scalatest.{FunSuite}
import org.apache.spark.sql.{DataFrame, SparkSession}

class StatsTest extends FunSuite:
  val spark = SparkSession
  .builder()
  .appName("My first Spark App")
  .config("spark.master", "local")
  .getOrCreate()

  test("Stats.flightByMonth") {
    assert true
  }
