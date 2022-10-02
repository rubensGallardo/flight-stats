import org.apache.spark.sql.SparkSession
import stats.{Stats, Utils}


object MyFirstScala {

  //SparkSession will be the entry point of Spark for this APP
  val spark: SparkSession = SparkSession.
    builder().
    appName("My first Spark App").
    config("spark.master", "local").
    getOrCreate()

  def main(args: Array[String]):Unit = {
    val statistics = new Stats()
    val utils = new Utils()
    val path = "src/main/result/"

    // Reading CSV files in the path provided
    val flightData = utils.readCsvFile("src/main/resources/flightData.csv", spark)
    val passengerData = utils.readCsvFile("src/main/resources/passengers.csv", spark)

    //There are few commented lines below right after the exercises,
    // this is because I tried to store the files locally
    // I don't know if this is something connected to the IDE or some versions of sbt, spark and scala
    // to provide you with a solution, I commented them and at least you can see the table printed

    println("Exercise #1 - Calculate the number of flights by month")
    //val flightsByMonthDf =
    statistics.flightsByMonth(flightData, spark)
        .show()
    //utils.writeCsvFile(path + "1-flightsByMonth", flightsByMonthDf, spark )


    println("Exercise #2 - Calculate the number of flights by passenger")
    //val flightsByPassengerDf =
      statistics.flightsByPassenger(flightData, passengerData, spark)
      .show()
    //utils.writeCsvFile(path + "2-flightsByPassenger", flightsByPassengerDf, spark )


    println("Exercise #3 - Calculate the longest run by passenger")
    //val longestRunDf =
      statistics.getLongestRunByPassenger(flightData, spark)
        .show()
    //utils.writeCsvFile(path + "3-longestRun", longestRunDf, spark )


    println("Exercise #4 - Calculate passengers in more than 3 flights together")
    //val flightsTogetherDf =
    statistics.passengersFlightsTogether(flightData, spark)
      .show()
    //utils.writeCsvFile(path + "4-flightsTogether", flightsTogetherDf, spark )

  spark.stop()
  }
}
