name := "flight-stats"

version := "1.0"

scalaVersion := "2.13.9"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.scalatest" %% "scalatest" % "3.2.13" % Test,
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25"
)