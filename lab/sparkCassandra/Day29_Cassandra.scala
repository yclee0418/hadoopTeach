package sparkIronMan

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._

object Day29_Cassandra extends App {
  setLogger
  val conf = new SparkConf()
    .set("spark.cassandra.connection.host", "hadoop1")
    .setMaster("local[*]")
    .setAppName("cassandra")
  val sc = new SparkContext(conf)
  val rdd = sc.cassandraTable("my_keyspace", "kv")
  val count = rdd.count()
  println(rdd.count)
  println(rdd.first)
  println(rdd.map(_.getInt("value")).sum)
  rdd.foreach(println)
  
  val collection = sc.parallelize(Seq(("key" + (count+1).toString(), (count + 1).toInt), 
      ("key" + (count+2).toString, (count+2).toInt)))
  collection.saveToCassandra("my_keyspace", "kv", SomeColumns("key", "value"))
  println("write to cassandra success!!")
  
  val rdd2 = sc.cassandraTable("my_keyspace", "user")
      .where("first_name=?", "Albert") //use primary key for where
  val firstRow = rdd2.first()
  val first_name = firstRow.get[String]("first_name") //retrieve String field
  val addressMap = firstRow.getMap[String, UDTValue]("address") //retrieve Map field
  val address = addressMap.get("home")
  val addressStr = if (address.nonEmpty) address.get.toMap.map(f => /*f._1 + ": " +*/ f._2.toString()).mkString(" ") else "noAddress" 
  val emails = firstRow.getList[String]("email").mkString(";") //get list field
  val last_name = firstRow.getStringOption("last_name").getOrElse("none") //get optional field
  val title = firstRow.getStringOption("title").getOrElse("noTitle") //get option field
  println(s"name: $first_name $last_name, address: $addressStr, emails: $emails, title: $title")
  
    
  def setLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)//mark for MLlib INFO msg
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("io").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.ALL);
  }
}

