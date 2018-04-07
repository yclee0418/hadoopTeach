### Spark Streaming Twitter 整合案例實作

* Project Ref: https://databricks.gitbooks.io/databricks-spark-reference-applications/twitter_classifier/index.html
* SBT-eclipse 整合：https://github.com/yclee0418/sparkTeach/tree/master/sbt
* spark streaming twitter 整合：https://github.com/apache/bahir/blob/master/streaming-twitter/README.md
* OAuth API reference for twitter API : http://twitter4j.org/en/configuration.html

#### 實作順序
* Collect.scala : 透過spark-twitter API 由 twitter 讀取訊息，並存為 json 格式
  * 先取得 twitter API 認證碼
  * 需先透過 SBT-eclipse 以 build.sbt 產生 eclipse 專案，執行Collect
* Spark_Twitter_Analysis : 透過 pyspark 分析 twitter 訊息，並產生統計圖
  * 透過 startNotebook3.sh 啟動 ipython notebook with pyspark
