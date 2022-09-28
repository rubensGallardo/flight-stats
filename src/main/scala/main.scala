import stats.stats
import stats.utils
import org.apache.spark.sql.{DataFrame, SparkSession}


object MyFirstScala {

  val spark = SparkSession.
    builder().
    appName("Spark SQL basic example").
    config("spark.master", "local").
    getOrCreate()

  def main(args: Array[String]):Unit = {
    val statistics = new stats()
    val utils = new utils()

    val flightData = utils.readCsvFile("src/main/resources/flightData.csv", spark)
    val passengerData = utils.readCsvFile("src/main/resources/passengers.csv", spark)


    println("Exercise #1 - Calculate the number of flights by month")
    statistics.flightsByMonth(flightData, spark).show()


    println("Exercise #2 - Calculate the number of flights by passenger")
    statistics.flightsByPassenger(flightData, passengerData, spark).show()


    //println("Exercise #3 - Calculate the longest run by passenger")
    //statistics.getLongestRunByPassenger(flightData, spark).show()

    println("Exercise #4 - Calculate passengers in more than 3 flights together")
    statistics.passengersFlightsTogether(flightData, spark).show()


  }
}