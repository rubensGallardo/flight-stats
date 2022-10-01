package stats
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import stats.test.SparkSessionTestWrapper



class StatsTest extends AnyFunSuite
  with SparkSessionTestWrapper {

  val flightsColumns = Seq("passengerId","flightId","from","to","date")
  val flightsData = Seq (
          ("7485","0","cg","ir","2017-01-01"),
          ("967","103","tk","co","2017-02-04")
  )

  val flightsDf: DataFrame = spark.createDataFrame(flightsData).toDF(flightsColumns:_*)
  val passengerColumns = Seq("passengerId","firstName","lastName")
  val passengerData = Seq(("7485","Carman","Elicia"), ("967", "Adriana", "Miguelina"))
  val passengerDf: DataFrame = spark.createDataFrame(passengerData).toDF(passengerColumns:_*)


  test ("1. Test exercise #1 - Flights by month") {

    val statistics = new Stats()
    val specData = Seq((1,1),(2, 1))
    val specColumns = Seq("Month", "NumberOfFlights")
    val specDf = spark
      .createDataFrame(specData)
      .toDF(specColumns:_*)

    val resultDf = statistics.flightsByMonth(flightsDf, spark)

    val result = resultDf.collect()
    val spec = specDf.collect()

    assert (result.sameElements(spec))

  }


  test ("2. Test exercise #2 - Flights by passenger") {

    val statistics = new Stats()
    val specData = Seq(("7485", 1, "Carman", "Elicia"),("967", 1, "Adriana","Miguelina"))
    val specColumns = Seq("PassengerId", "NumberOfFlights", "firstName", "lastName")
    val specDf = spark
      .createDataFrame(specData)
      .toDF(specColumns:_*)

    val resultDf = statistics.flightsByPassenger(flightsDf, passengerDf, spark)

    val result = resultDf.collect()
    val spec = specDf.collect()

    assert (result.sameElements(spec))

  }

  test("3. Test exercise #3 - Longest run by passenger, no UK in the route") {

    val statistics = new Stats()

    val specData = Seq(("7485", 2), ("967", 2))
    val specColumns = Seq("PassengerId", "LongestRun")
    val specDf = spark
      .createDataFrame(specData)
      .toDF(specColumns:_*)

    val resultDf= statistics.getLongestRunByPassenger(flightsDf, spark)

    val result = resultDf.collect()
    val spec = specDf.collect()

    assert (result.sameElements(spec))
  }

  test("4. Test exercise #3 - Longest run by passenger, with UK in the route") {

    val statistics = new Stats()

    val flightDataWithUK = Seq(
      ("33","0","cg","ir","2017-01-01"),
      ("33","0","ir","fr","2017-01-05"),
      ("33","0","fr","uk","2017-01-15"),
      ("33","0","uk","sp","2017-02-01"),
      ("33","0","sp","it","2017-02-20"),
      ("33","0","it","uk","2017-04-01")
    )

    val flightsWithUkDf: DataFrame = spark.createDataFrame(flightDataWithUK).toDF(flightsColumns:_*)
    val specData = Seq(("33", 3))
    val specColumns = Seq("PassengerId", "LongestRun")
    val specDf = spark
      .createDataFrame(specData)
      .toDF(specColumns:_*)

    val resultDf= statistics.getLongestRunByPassenger(flightsWithUkDf, spark)

    val result = resultDf.collect()
    val spec = specDf.collect()

    assert (result.sameElements(spec))
  }

  test("5. Test exercise #3 - Longest run by passenger, with from and to in the same country") {

    val statistics = new Stats()

    val flightDataWithSameFromTo = Seq(
      ("13","0","sp","uk","2017-04-01"),
      ("13","0","uk","uk","2017-06-20"),
      ("13","0","uk","cg","2017-07-20"),
      ("13","0","cg","ir","2017-08-01"),
      ("13","0","ir","fr","2017-09-05"),
      ("13","0","fr","fr","2017-10-15"),
      ("13","0","fr","it","2017-10-15"),
      ("13","0","it","uk","2017-11-01"),
    )
    val flightsSameFromToDf: DataFrame = spark
      .createDataFrame(flightDataWithSameFromTo)
      .toDF(flightsColumns:_*)

    val specData = Seq(("13", 4))
    val specColumns = Seq("PassengerId", "LongestRun")
    val specDf = spark
      .createDataFrame(specData)
      .toDF(specColumns:_*)

    val resultDf= statistics.getLongestRunByPassenger(flightsSameFromToDf, spark)

    val result = resultDf.collect()
    val spec = specDf.collect()

    assert (result.sameElements(spec))
  }


  test("6. Test exercise #4 - Passengers were together in number of flights, empty result") {

    val statistics = new Stats()

    val flightsPassengersTogether = Seq (
      ("7485","0","cg","ir","2017-01-01"),
      ("5478","0","cg","ir","2017-01-01"),
      ("967","103","tk","co","2017-02-04"),
      ("5478","25","sp","uk","2017-05-01"),
      ("7485","25","sp","uk","2017-05-01"),
    )

    val flightsTogetherDf: DataFrame = spark
      .createDataFrame(flightsPassengersTogether)
      .toDF(flightsColumns:_*)

    val resultDf= statistics.passengersFlightsTogether(flightsTogetherDf, spark)

    val result = resultDf.collect()


    assert (result.length == 0)
  }

  test("7. Test exercise #4 - Passengers were together in number of flights") {

    val statistics = new Stats()

    val flightsPassengersTogether = Seq (
      ("7485","0","cg","ir","2017-01-01"),
      ("5478","0","cg","ir","2017-01-01"),
      ("967","103","tk","co","2017-02-04"),
      ("5478","25","sp","uk","2017-05-01"),
      ("7485","25","sp","uk","2017-05-01"),
      ("5478","78","sp","uk","2017-06-01"),
      ("7485","78","sp","uk","2017-06-01"),
      ("5478","7894","sp","uk","2017-07-01"),
      ("7485","7894","sp","uk","2017-07-01"),
      ("5478","987","sp","uk","2017-10-01"),
      ("7485","987","sp","uk","2017-10-01"),
      ("967","987","sp","uk","2017-10-01")
    )

    val flightsTogetherDf: DataFrame = spark
      .createDataFrame(flightsPassengersTogether)
      .toDF(flightsColumns:_*)

    val specData = Seq(("7485", "5478", 5))
    val specColumns = Seq("passenger1", "passenger2", "flightsTogether")
    val specDf = spark.
      createDataFrame(specData)
      .toDF(specColumns:_*)

    val resultDf= statistics.passengersFlightsTogether(flightsTogetherDf, spark)

    val result = resultDf.collect()
    val spec = specDf.collect()

    assert (result.sameElements(spec))
  }

}
