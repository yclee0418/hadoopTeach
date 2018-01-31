package it30days

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark._
import org.apache.spark.streaming._

object StatefulObj2 {
  //2016-03-22 20:25:31,318525,9,FCEL,809,11.00,B
  //2016-03-22 20:25:30,221934,85,AFFX,865,26.00,S
  case class Order(time: String, orderId: Long, clientId: Long,
                   symbol: String, amount: Int, price: Double, buy: Boolean)
  def main(args: Array[String]): Unit = {
    Utils.setLogger
    val conf: SparkConf = new SparkConf()
    if (args(0).equals("local")) {
      conf.setMaster("local[*]").setAppName("StatefulStreamingSample2")
    } else {
      conf.setAppName("StatefulStreamingSample2")
    }
    val ssc = new StreamingContext(conf, Seconds(6))
    val hdfsStrem = ssc.textFileStream("hdfs://hadoop1:9000/sparkDir")
    //val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val orders = hdfsStrem.flatMap(line => {
      val words = line.split(",").map(_.trim())
      //2016-03-22 20:25:28,2,70,NFLX,158,8.00,B
      try {
        List(Order( /*new Timestamp(dateFormat.parse(words(0)).getTime)*/ words(0),
          words(1).toLong,
          words(2).toLong,
          words(3),
          words(4).toInt,
          words(5).toDouble,
          words(6).equals("B")))
      } catch {
        case e: Throwable =>
          println("parse text:" + line + " error, ex=" + e)
          List()
      }
    })

    //stateless
    val numMap = orders.map(order => (if (order.buy) "BUY" else "SELL", 1L)).reduceByKey(_ + _)
      .map(count => (count._1, List(count._2.toString())))

    //stateful
    //checkpoint
    ssc.checkpoint("streaming_ck2")
    val totalMap = orders.map(o => (o.clientId, o.amount * o.price))
    //------------ updateStateByKey version ------------
    //    val totalClientPrice = totalMap.updateStateByKey((vals: Seq[Double], total: Option[Double]) => {
    //      Some(vals.sum + total.getOrElse[Double](0))
    //    })
    //------------ mapWithState version ------------
    //ref: https://ithelp.ithome.com.tw/articles/10188456
    val updatePriceClient = (clientId: Long, amount: Option[Double], state: State[Double]) => {
      var total = amount.getOrElse(0.toDouble)
      if (state.exists()) {
        total += state.get()
      } 
      state.update(total)
      Some(clientId, state)
    }
    val totalClientPrice = totalMap.mapWithState(StateSpec.function(updatePriceClient)).stateSnapshots()
     
    //common logic
    val top5price = totalClientPrice.transform(_.sortBy(_._2, false).zipWithIndex.filter(_._2 < 5)).map(_._1)
    val top5str = top5price.repartition(1).map(_._1.toString()).glom.map(arr => ("TOP5Clients", arr.toList))

    //union
    numMap.union(top5str).print()
    ssc.start()
    ssc.awaitTermination()
  }
}

