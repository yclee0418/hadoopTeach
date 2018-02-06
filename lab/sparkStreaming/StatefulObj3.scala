package it30days

import kafka.serializer.StringDecoder //this package need to be declared before spark_kafka_integration library
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

object StatefulObj3 {
  case class Order(time: String, orderId: Long, clientId: Long,
                   symbol: String, amount: Int, price: Double, buy: Boolean)

  def main(args: Array[String]): Unit = {
    Utils.setLogger
    val conf: SparkConf = new SparkConf()
    if (args(0).equals("local")) {
      conf.setMaster("local[*]").setAppName("Stateful Streaming Kafka")
    } else {
      conf.setAppName("Stateful Streaming Kafka")
    }
    val ssc = new StreamingContext(conf, Seconds(6))
    //create kafka stream
    val brokerList = "hadoop1:9092"
    val kafkaRecParam = Map[String, String]("metadata.broker.list" -> brokerList)
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaRecParam, Set("orders"))
    //create kafka stream

    val orders = kafkaStream.flatMap(line => {
      val words = line._2.split(",").map(_.trim())
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
    ssc.checkpoint("streaming_kafka")
    val totalMap = orders.map(o => (o.clientId, o.amount * o.price))
    //------------ updateStateByKey version ------------
    //    val totalClientPrice = totalMap.updateStateByKey((vals: Seq[Double], total: Option[Double]) => {
    //      Some(vals.sum + total.getOrElse[Double](0))
    //    })
    //------------ mapWithState version ------------
    //ref: https://ithelp.ithome.com.tw/articles/10188456
    val updatePriceClient = (clientId: Long, amount: Option[Double], state: State[Double]) => {
      var total = amount.getOrElse(0.toDouble)
      if (state.exists()) 
        total += state.get()
     
      state.update(total)
      //Some((clientId, state)) => pass state into 2nd param will cause cannot serialize error
      Some(clientId, total)
    }
    val totalClientPrice = totalMap.mapWithState(StateSpec.function(updatePriceClient)).stateSnapshots()

    //common logic
    val top5clients = totalClientPrice.
    transform(_.sortBy(_._2, false).
      zipWithIndex().
      filter(_._2 < 5)).
    map(_._1)
    val top5str = top5clients.repartition(1).map(_._1.toString()).glom.map(arr => ("TOP5Clients", arr.toList))

    //union
    val finalRdd = numMap.union(top5str)
    //send message to metrics topic
    finalRdd.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        KafkaProducerWrapper.brokerList = brokerList
        val producer = KafkaProducerWrapper.getInstance
        iter.foreach(item => {
          producer.send("metrics", item._1 + ": " + item._2.mkString(","))
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

