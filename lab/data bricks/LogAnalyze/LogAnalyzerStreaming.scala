package com.databricks.apps.chap1

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Logger, Level}

import com.databricks.apps.ApacheAccessLog
/**
 * this app will open nc server in localhost 9999 and receive log data by spark stream rdd
 * do following steps to test:
 *    1. start this program
 *    2. open terminal and use "echo '64.242.88.10 - - [07/Mar/2004:16:37:27 -0800] "GET /twiki/bin/view/TWiki/DontNotify HTTP/1.1" 200 4140' | nc -l 9999" 
 *       (or use streamLogs.sh to send log continuously)
 */
object LogAnalyzerStreaming extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val WINDOW_LENGTH = Seconds(30)
  val SLIDE_INTERVAL = Seconds(10)

  val sparkConf = new SparkConf().setAppName("Log Analyzer Streaming in Scala").setMaster("local[*]")
  val streamingContext = new StreamingContext(sparkConf, SLIDE_INTERVAL)

  val logLinesDStream: DStream[String] = streamingContext.socketTextStream("localhost", 9999)

  val accessLogsDStream: DStream[ApacheAccessLog] = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()
  val windowDStream: DStream[ApacheAccessLog] = accessLogsDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL)

  windowDStream.foreachRDD(accessLogs => {
    if (accessLogs.count() == 0) {
      println("No access logs received in this time interval")
    } else {
      // Calculate statistics based on the content size.
      val contentSizes: RDD[Long] = accessLogs.map(_.contentSize).cache()
      println("Content Size Avg: %s, Min: %s, Max: %s".format(
        contentSizes.reduce(_ + _) / contentSizes.count,
        contentSizes.min,
        contentSizes.max
      ))

      // Compute Response Code to Count.
      val responseCodeToCount: Array[(Int, Long)] = accessLogs
        .map(_.responseCode -> 1L)
        .reduceByKey(_ + _)
        .take(100)
      println( s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

      // Any IPAddress that has accessed the server more than 10 times.
      val ipAddresses: Array[String] = accessLogs
        .map(_.ipAddress -> 1L)
        .reduceByKey(_ + _)
        .filter(_._2 > 10)
        .map(_._1)
        .take(100)
      println( s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

      // Top Endpoints.
      val topEndpoints: Array[(String, Long)] = accessLogs
        .map(_.endpoint -> 1L)
        .reduceByKey(_ + _)
        .top(10)(Ordering.by[(String, Long), Long](_._2))
      println( s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
    }
  })

  // Start the streaming server.
  streamingContext.start() // Start the computation
  streamingContext.awaitTermination() // Wait for the computation to terminate
}