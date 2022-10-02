package stats

import org.apache.spark.sql.functions.{col, sum, count, expr, max}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window

class Stats {


  def flightsByMonth(flights: DataFrame, spark: SparkSession): DataFrame = {
    /**
     *  This function will provide the solution for the Exercise number 1
     *  - flights: type - DataFrame
     *             description - Dataframe with the following structure, (passengerId,flightId,from,to,date)
     *  - spark: type - SparkSession
     *            description - Common entry point to run a Spark application
     *  - Result: type - Dataframe
     *            description - Dataframe with the following structure (Month , Number of Flights)
     */

    println("Calculating the number of flights by month")

    //Select to get the number of different flights in a month with number as format
    val flightMonthDf = flights
                          .selectExpr("flightId", "month(date) as month")
                          .distinct()

    // Calculate with following statement the number of flights in a month
    // Order by Month to show it from 1 to 12 and rename columns
    flightMonthDf
      .groupBy("month")
      .count()
      .orderBy("month")
      .selectExpr("month as Month","count AS NumberOfFlights")


  }


  def flightsByPassenger(flights: DataFrame, passengers: DataFrame, spark: SparkSession): DataFrame= {
    /**
     *  This function will provide the solution for the Exercise number 2
     *  - flights: type - DataFrame
     *             description - Dataframe with the following structure, (passengerId,flightId,from,to,date)
     *  - passengers: type - DataFrame
     *             description - Dataframe with the following structure, (passengerId,firstName,lastName)
     *  - spark: type - SparkSession
     *             description - Common entry point to run a Spark application
     *  - Result: type - Dataframe
     *             description - Dataframe with the following structure
     *             (PassengerID , Number of Flights, firstName, lastName )
     */

    println("Calculating the number of flights by passenger")

    //Query to get the number of different flights by passenger
    val numberOfFlights = flights
                            .selectExpr("flightId", "passengerId AS passenger")
                            .distinct()
                            .groupBy("passenger")
                            .count()

    // The result will be returned after joining flights and passengers DataFrames
    numberOfFlights
      .orderBy(col("count").desc)
      .limit(100)
      .join(passengers, numberOfFlights("passenger") === passengers("passengerId"))
      .selectExpr("passengerId AS PassengerID", "count as NumberOfFlights", "firstName", "lastName")
      .orderBy(col("NumberOfFlights").desc)

  }


  def getLongestRunByPassenger( flights: DataFrame, spark:SparkSession): DataFrame = {
    /**
     *  This function will provide the solution for the Exercise number 3
     *  - flights: type - DataFrame
     *             description - Dataframe with the following structure, (passengerId,flightId,from,to,date)
     *  - spark: type - SparkSession
     *            description - Common entry point to run a Spark application
     *  - Result: type - Dataframe
     *            description - Dataframe with the following structure (passenger1 , passenger2, longest_run)
     */

    println("Adding a routeId to every passenger")

    // Create a DataFrame that will add a new column specifying the number of route
    // Every time the field from is equal to 'UK' a new route will start
    val w = Window.partitionBy("passengerId").orderBy("date")
    val routeByPassenger = flights
      .withColumn("routeId",
        sum(
          expr("""
                    CASE WHEN LOWER(from) = 'uk' THEN 1 -- new route starts
                    ELSE 0 END                          -- still on route not in UK
                 """.stripMargin))
          .over(w))

    println("Calculating the number of steps in every route and getting the max number of steps by passenger")
    // Getting every different option by passengers
    // after adding a column with the result of adding every different country in every route
    // Finally the result generate the max number of the field run (number of countries in a route)

    routeByPassenger
      .groupBy("passengerId", "routeId")
      .agg((
          sum(
            expr("""
                CASE
                    -- scenario to cover UK-UK - adding -1 the count is reset
                    WHEN from = to AND lower(from) = 'uk'
                              THEN -1

                    -- scenario to cover same from and to - adding 0 the count remain the same
                    WHEN from = to
                              THEN 0

                    -- count all nonUK countries, except for first one
                    WHEN LOWER('UK') NOT IN (from,to)
                              THEN 1

                    -- don't count itineraries from/to UK
                    ELSE 0
                END""")
          ) + 1
          ).as("run"))
        .groupBy("passengerId")
        .agg(max("run")
          .as("longest_run"))
      .orderBy(col("longest_run").desc)

  }


  def passengersFlightsTogether(flights: DataFrame, spark: SparkSession): DataFrame = {
    /**
     *  This function will provide the solution for the Exercise number 4
     *  - flights: type - DataFrame
     *             description - Dataframe with the following structure, (passengerId,flightId,from,to,date)
     *  - spark: type - SparkSession
     *            description - Common entry point to run a Spark application
     *  - Result: type - Dataframe
     *            description - Dataframe with the following structure (passenger1 , passenger2, flightsTogether)
     */

    // DataFrame as result of joining flights DataFrame with itself, using
    // flightId and date to match both dataframes by those columns
    // and using passenger1 > passenger2 to avoid duplicates
    // (i.e flights1.passengerId=2012 and flights2.passengerId=2012) or
    // (i.e flights1.passengerId=2012 and flights2.passengerId=5789
    // and in the next row the same values but other way around)
    // another column is being added to have the number of flights where both passengers were together

    println("Get the number of flights 2 different passengers were in the same flight ")
    val flightsTogether = flights.as("flights1")
      .join(
        flights.as("flights2"),
        col("flights1.flightId") === col("flights2.flightId") &&
        col("flights1.date") === col("flights2.date") &&
        col("flights1.passengerId") > col("flights2.passengerId"),
        "inner"
      )
      .groupBy(col("flights1.passengerId"), col("flights2.passengerId"))
      .agg(count(col("*")).as("flightsTogether"))

    //The Dataframe out will be the DataFrame above after being filtered taking numbers higher than 3
    // The result will be ordered showing on top the row with passengers were in the same flight more times
    println("Result with passengers and the number of flights together higher or equal than 3")
    flightsTogether
      .selectExpr("flights1.passengerId AS passenger1", "flights2.passengerId AS passenger2", "flightsTogether")
      .filter(col("flightsTogether") > 3)
      .orderBy(col("flightsTogether").desc, col("passenger1").desc)
  }

}
