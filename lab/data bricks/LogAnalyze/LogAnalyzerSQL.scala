package com.databricks.apps.chap1

import org.apache.spark.sql._
import org.apache.log4j._
import com.databricks.apps.ApacheAccessLog

object LogAnalyzerSQL extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession
    .builder()
    .appName("Log Analyzer SQL in Scala")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  val logFile = args(0)

  val accessLogs: Dataset[ApacheAccessLog] =
    spark.read.textFile(logFile).map(ApacheAccessLog.parseLogLine(_))
  accessLogs.createOrReplaceTempView("logs")

  // Calculate statistics based on the content size.
  val contentSizes = spark.sql("select sum(contentSize), count(*), min(contentSize), max(contentSize) from logs").first()
  println("Content Size Avg: %s, Min: %s, Max: %s".format(
    contentSizes.getLong(0) / contentSizes.getLong(1),
    contentSizes.get(2),
    contentSizes.get(3)))

  // Compute Response Code to Count.
  val responseCodeToCount = spark.sql("select responseCode, count(*) from logs group by responseCode")
    .map(row => (row.getInt(0), row.getLong(1)))
    .collect()
  println(s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

  // Any IPAddress that has accessed the server more than 10 times.
  val ipAddresses: Array[String] = spark
    .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 order by total LIMIT 100")
    .map(row => row.getString(0))
    .collect()
  println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

  //Top 10 endpoint
  val topEndpoints: Array[(String, Long)] = spark
    .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
    .map(row => (row.getString(0), row.getLong(1)))
    .collect()
  println(s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")

  spark.stop()
}