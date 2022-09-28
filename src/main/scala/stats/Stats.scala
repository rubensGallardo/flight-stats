package stats

import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Stats {


  def flightsByMonth(flights: DataFrame, spark: SparkSession): DataFrame = {

    println("Calcuating the number of flights by month")
    val flightMonthDf = flights
                          .selectExpr("flightId", "month(date) as month")
                          .distinct()

    flightMonthDf
      .groupBy("month")
      .count()
      .orderBy("month")
      .selectExpr("month as Month","count AS NumberOfFlights")


  }

  def flightsByPassenger(flights: DataFrame, passengers: DataFrame, spark: SparkSession): DataFrame= {
    println("Calcuating the number of flights by passenger")
    flights.createOrReplaceTempView("flights_tbl")

    val numberOfFlights = flights
                            .selectExpr("flightId", "passengerId AS passenger")
                            .groupBy("passenger")
                            .count()

    numberOfFlights
      .orderBy(col("count").desc)
      .limit(100)
      .join(passengers, numberOfFlights("passenger") === passengers("passengerId"))
      .selectExpr("passengerId AS PassengerID", "count as NumberOfFlights", "firstName", "lastName")
      .orderBy(col("NumberOfFlights").desc)

  }

/*
  def getLongestRunByPassenger( flights: DataFrame, spark:SparkSession): DataFrame = {

    val passengers = flights.select("passengerId").distinct()
    val fromByPassenger = passengers.foreach(f => {
      fromByPassenger.add(flights.flights.
      filter(org.apache.spark.sql.functions.col("passengerId") === f.("passengerId")).
      select("from","to").
      collect().
      map(toTuple(_(0),_(1))).
      toList
    ).toDF
  })
  */

  def passengersFlightsTogether(flights: DataFrame, spark: SparkSession): DataFrame = {

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


    flightsTogether
      .selectExpr("flights1.passengerId AS passenger1", "flights2.passengerId AS passenger2", "flightsTogether")
      .filter(col("flightsTogether") >= 3)
      .orderBy(col("flightsTogether").desc)
  }

}