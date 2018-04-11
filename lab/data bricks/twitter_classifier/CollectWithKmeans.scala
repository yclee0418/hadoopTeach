package com.databricks.apps.twitter_classifier

import java.io.File

import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.{ Tokenizer, Word2Vec, Word2VecModel }
import org.apache.spark.ml.clustering.KMeansModel
import twitter4j.conf._
import twitter4j._

/**
 * Collect at least the specified number of tweets into json text files.
 */
object CollectWithKmeans {
  private var numTweetsCollected = 0L
  private var partNum = 0
  private var gson = new Gson()

  def main(args: Array[String]): Unit = {
    val numTweetsToCollect = 200 //max number to collect
    val intervalSecs = 10 //every 10 sec collect data from twitter
    val partitionsEachInterval = 1 //number of rdd partition
    val predictTarget = 3 //只取cluster為ja者
    val outputDirectory = s"data/filtered$predictTarget" //output json dir for specific lang
    val outputDir = new File(outputDirectory.toString)
    if (outputDir.exists()) {
      System.err.println("ERROR - %s already exists: delete or specify another directory".format(
        outputDirectory))
      System.exit(1)
    }
    outputDir.mkdirs()

    //初始化SparkSession以使用Spark DataFrame API
    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(intervalSecs))
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //create auth obj for twitter api
    val cb: ConfigurationBuilder = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey("=================")
      .setOAuthConsumerSecret("=================")
      .setOAuthAccessToken("=================")
      .setOAuthAccessTokenSecret("=================");
    val tf: TwitterFactory = new TwitterFactory(cb.build())
    val twitter: Twitter = tf.getInstance()
    //invoke create stream to get data from twitter
    val tweetStream = TwitterUtils.createStream(ssc, Some(twitter.getAuthorization))

    //建立文字分析所需之相關instance

    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("textVec")
    //load Kmeans model
    val kmodelInputPath = "model/twitter_kmeans_model"
    val kmeansModel = KMeansModel.read.load(kmodelInputPath)
    //load w2v model
    val w2vModelPath = "model/twitter_w2v_model"
    val w2vModel = Word2VecModel.read.load(w2vModelPath)
    
    tweetStream.foreachRDD((rdd, time) => {
      if (rdd.count() > 0) {
        val twitterTable = rdd.map(f => (f.getUser.getLang, f.getUser.getName, f.getText)).toDF("lang", "name", "text")
        val twitterTableVec = tokenizer.transform(twitterTable)
        val w2vData = w2vModel.transform(twitterTableVec)
        
        val predictResRaw = kmeansModel.transform(w2vData).select("lang", "name","text", "prediction")
        predictResRaw.repartition(partitionsEachInterval).write.json(outputDirectory + "_raw" + "/tweets_" + time.milliseconds.toString)  
        val predictRes = predictResRaw.filter(s"prediction=$predictTarget") //只取cluster為ja者
                            
        val predictResCnt = predictRes.count()
        if (predictResCnt > 0) {
          predictRes.repartition(partitionsEachInterval).write.json(outputDirectory + "/tweets_" + time.milliseconds.toString)  
        }
        
        numTweetsCollected += predictResCnt
        if (numTweetsCollected > numTweetsToCollect) {
          System.exit(0)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}