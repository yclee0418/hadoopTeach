package it30days

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark._
import org.apache.spark.streaming._

object StatefulObj extends App {
  //2016-03-22 20:25:31,318525,9,FCEL,809,11.00,B
  //2016-03-22 20:25:30,221934,85,AFFX,865,26.00,S
  case class Order(time: String, orderId: Long, clientId: Long,
                   symbol: String, amount: Int, price: Double, buy: Boolean)
                   
  val conf=new SparkConf().setMaster("local[*]").setAppName("StatefulStreamingSample")
  val ssc = new StreamingContext(conf, Seconds(6))
  val hdfsStrem = ssc.textFileStream("hdfs://hadoop1:9000/sparkDir")
  //val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val orders = hdfsStrem.flatMap(line => {
    val words = line.split(",").map(_.trim())
    //2016-03-22 20:25:28,2,70,NFLX,158,8.00,B
    try {
      List(Order(/*new Timestamp(dateFormat.parse(words(0)).getTime)*/words(0),
          words(1).toLong,
          words(2).toLong,
          words(3),
          words(4).toInt,
          words(5).toDouble,
          words(6).equals("B")
          ))
    } catch {
      case e: Throwable => println("parse text:" + line + " error, ex=" + e)
      List()
    }
  })
  
  //stateful 
  //checkpoint
  ssc.checkpoint("streaming_ck")
  val numMap = orders.map(o => (o.clientId, o.amount * o.price))
  val top5price = numMap.updateStateByKey((vals: Seq[Double], total: Option[Double]) => {
    Some(vals.sum + total.getOrElse[Double](0))
  }).transform(_.sortBy(_._2, false).zipWithIndex.filter(_._2 < 5)).map(_._1)
  
  top5price.print()
  
  ssc.start()
  ssc.awaitTermination()
}

