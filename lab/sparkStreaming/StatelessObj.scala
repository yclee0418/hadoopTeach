package it30days

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark._
import org.apache.spark.streaming._

object StatelessObj extends App {
  case class Order(time: java.sql.Timestamp, orderId: Long, clientId: Long,
                   symbol: String, amount: Int, price: Double, buy: Boolean)
                   
  val conf=new SparkConf().setMaster("local[*]").setAppName("StatelessStreamingSample")
  val ssc = new StreamingContext(conf, Seconds(2))
  val hdfsStrem = ssc.textFileStream("hdfs://hadoop1:9000/sparkDir")
   val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  val orders = hdfsStrem.flatMap(line => {
    val words = line.split(",")
    //2016-03-22 20:25:28,2,70,NFLX,158,8.00,B
    try {
      List(Order(new Timestamp(dateFormat.parse(words(0)).getTime),
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
  
  val numMap = orders.map(order => (order.buy, 1L)).reduceByKey(_ + _)
  numMap.print()
  
  ssc.start()
  ssc.awaitTermination()
}

