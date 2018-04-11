package com.databricks.apps.twitterClassifier

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{ HashingTF, Tokenizer, Word2Vec }
import org.apache.spark.ml.clustering.KMeans

object TrainClusterModel {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    
    var trainDataPath: String = "file:/Users/yungchuanlee/workspace_spark/spark-twitter-sbt/data/train/tweets_*/part-*" //讀取訓練資料的檔案來源
    var testDataPath: String = "file:/Users/yungchuanlee/workspace_spark/spark-twitter-sbt/data/test/tweets_*/part-*" //讀取測試資料的檔案來源
    val numTFFeatures = 128 //HashTf 的輸出長度
    if (args != null && args.size > 1) {
      trainDataPath = args(0)
      testDataPath = args(1)
    }

    //產生SparkSession物件以使用 DataFrame API
    val spark = SparkSession
      .builder().master("local[*]")
      .appName("TrainClusterModel for twitter")
      .config("spark.executor.memory", "6g")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()

    //讀入訓練檔案
    val twitterTable = spark.read.json(trainDataPath)
    //進行HashTF前先將text內容由string轉為vector
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("textVec")
    val twitterTableVec = tokenizer.transform(twitterTable.select("user.lang", "text"))
    twitterTableVec.persist()
    twitterTableVec.printSchema()
    //進行HashTF，將textVec的內容進行HashTF後輸出至features欄位
    val hashingTF = new HashingTF()
      .setInputCol("textVec").setOutputCol("features").setNumFeatures(numTFFeatures)
    val hashingTFData = hashingTF.transform(twitterTableVec)
    //改用WordVector嘗試
    val word2Vec = new Word2Vec()
      .setInputCol("textVec")
      .setOutputCol("features")
      .setVectorSize(numTFFeatures)
      .setMinCount(0)
    val w2vModel = word2Vec.fit(twitterTableVec)
    val w2vData = w2vModel.transform(twitterTableVec)

    val seed = 1231L
    val clusterNum = 20
    val kmeans = new KMeans().setFeaturesCol("features").setPredictionCol("prediction").setK(clusterNum).setSeed(seed)
    val modelTF = kmeans.fit(hashingTFData)
    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSETF = modelTF.computeCost(hashingTFData)
    println(s"Within Set Sum of Squared Errors(HashTF) = $WSSSETF")
    //modelTF.transform(hashingTFData).select("lang", "text", "prediction").show(20)

    val modelW2v = kmeans.fit(w2vData)
    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSEW2v = modelW2v.computeCost(w2vData)
    println(s"Within Set Sum of Squared Errors(WordVec) = $WSSSEW2v")
    //modelW2v.transform(w2vData).select("lang", "text", "prediction").show(20)

    //tune clusterNum及numTFFeatures, 找出最佳之分群數
    val clusterNumArr = Array( 10, 20, 40, 50)
    val numTFFeatureArr = Array( 64, 128, 256)
    val paramsArr = for (cn <- clusterNumArr; tf <- numTFFeatureArr) yield {
      println(s"try param: clusterNum=$cn, numTFFeature=$tf")
      kmeans.setK(cn) //設定kmeans的分群數
      hashingTF.setNumFeatures(tf) //設定HashingTF的output長度
      val tfDataTune = hashingTF.transform(twitterTableVec)
      val modelTFTune = kmeans.fit(tfDataTune)
      val wssseTfTune = modelTFTune.computeCost(tfDataTune)//計算WSSSE

      word2Vec.setVectorSize(tf)//設定word2Vec的output長度
      val w2vModelTune = word2Vec.fit(twitterTableVec)
      val w2vDataTune = w2vModelTune.transform(twitterTableVec)
      val modelW2vTune = kmeans.fit(w2vDataTune)
      val wssseW2vTune = modelW2vTune.computeCost(w2vDataTune)//計算WSSSE
      val which = if (wssseW2vTune < wssseTfTune) "W" else "T" 
      val minWssse = if (wssseW2vTune < wssseTfTune) wssseW2vTune else wssseTfTune //取WSSSE最小的值
      (cn, tf, minWssse, which)
    }

    val paramSortEval = paramsArr.sortBy(_._3)
    val (bestClusterNum, bestTFFeatureNum, bestWSSSE, bestWhich) = paramSortEval(0)
    println(s"best parameter: clusterNum=$bestClusterNum, numTFFeature=$bestTFFeatureNum, WSSSE=$bestWSSSE, which=$bestWhich")

    //以最佳化參數作出Model後儲存Model
    kmeans.setK(bestClusterNum)
    println("Clustering with best param ...")
    val kmodelOutputPath = "model/twitter_kmeans_model"
    val w2vModelOutputPath = "model/twitter_w2v_model"
    if ("W".equals(bestWhich)) {
      word2Vec.setVectorSize(bestTFFeatureNum)
      val w2vModelBest = word2Vec.fit(twitterTableVec)
      val w2vDataBest = w2vModelBest.transform(twitterTableVec)
      val modelW2vBest = kmeans.fit(w2vDataBest)
      modelW2vBest.transform(w2vDataBest).select("lang", "text", "prediction").filter("lang='ja'").show(50, false)//以找出日語之分群為目標
      modelW2vBest.write.overwrite().save(kmodelOutputPath)//儲存KMeans Model
      //save Word2Vec Model also
      w2vModelBest.write.overwrite().save(w2vModelOutputPath)//Word2Vec Model亦需一併儲存
    } else {
      hashingTF.setNumFeatures(bestTFFeatureNum)
      val tfDataBest = hashingTF.transform(twitterTableVec)
      val modelTFBest = kmeans.fit(tfDataBest)
      modelTFBest.transform(tfDataBest).select("lang", "text", "prediction").filter("lang='ja'").show(50, false)
      modelTFBest.write.overwrite().save(kmodelOutputPath)
    }
    println(s"Kmeans model has been saved to $kmodelOutputPath !!")
    println(s"W2v model has been saved to $w2vModelOutputPath !!")
  }
}