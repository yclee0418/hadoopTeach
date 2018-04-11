package com.databricks.apps.twitter_classifier

import java.io.File

import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.{ SparkConf, SparkContext }
import twitter4j.conf._
import twitter4j._

/**
 * Collect at least the specified number of tweets into json text files.
 */
object Collect {
  private var numTweetsCollected = 0L
  private var partNum = 0
  private var gson = new Gson()

  def main(args: Array[String]) : Unit = {
    val outputDirectory = "data/output" //output json dir
    val numTweetsToCollect = 50000 //max number to collect
    val intervalSecs = 10 //every 10 sec collect data from twitter
    val partitionsEachInterval = 1 //number of rdd partition
    val outputDir = new File(outputDirectory.toString)
    if (outputDir.exists()) {
      System.err.println("ERROR - %s already exists: delete or specify another directory".format(
        outputDirectory))
      System.exit(1)
    }
    outputDir.mkdirs()

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(intervalSecs))

    //create auth obj for twitter api
    val cb: ConfigurationBuilder = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey("*****************")
      .setOAuthConsumerSecret("*****************")
      .setOAuthAccessToken("*****************")
      .setOAuthAccessTokenSecret("*****************");
    val tf: TwitterFactory = new TwitterFactory(cb.build())
    val twitter: Twitter = tf.getInstance()
    //invoke create stream to get data from twitter
    val tweetStream = TwitterUtils.createStream(ssc, Some(twitter.getAuthorization))
      .map(gson.toJson(_))

    tweetStream.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(partitionsEachInterval)
        outputRDD.saveAsTextFile(outputDirectory + "/tweets_" + time.milliseconds.toString)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsToCollect) {
          System.exit(0)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
