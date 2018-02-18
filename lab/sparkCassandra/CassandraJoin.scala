package sparkIronMan

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._

//data frame
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.functions.{udf => udf, col => col, lit => lit}

object CassandraJoin extends App {
  setLogger
  val conf = new SparkConf()
    .set("spark.cassandra.connection.host", "hadoop1")
    .setMaster("local[*]")
    .setAppName("Cassandra_Join")
    
  val sc = new SparkContext(conf)
  val keyspace = "my_keyspace"
  //select table with RDD
  val customers = sc.cassandraTable(keyspace, "customer_info")
                      .select("cust_id","name")
                      .as((c:String, n:String) => (c, n))
  val shopp_info = sc.cassandraTable(keyspace, "shopping_info")
                      .select("cust_id","shopping_date","shopping_id","item","price")
                      .as((c:String, sd:java.util.Date, sid:java.util.UUID, i:String, p:Double) 
                          => (c, (sd, sid, i, p)))
  val cust_shopping_rdd = customers.join(shopp_info)
  cust_shopping_rdd.collect().foreach(println)
  //write table with RDD
  val write_back_rdd = cust_shopping_rdd.map{
      case (cust_id, (name, (shopping_date, shopping_id, item, price))) =>
        (cust_id , name , shopping_date , price , shopping_id)
  }
  write_back_rdd.saveToCassandra(keyspace, "cust_with_shopping", 
      SomeColumns("cust_id" , "name" , "shopping_date" , "price" , "shopping_id"))
  println("write rdd to cassandra successfully!")
  
  //select columns with dataframe
  val spark = new org.apache.spark.sql.SQLContext(sc)
  spark.setCassandraConf("Cluster1", CassandraConnectorConf.ConnectionHostParam.option("hadoop1"))
  val cust_df =  spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "customer_info", "keyspace" -> keyspace , "cluster" -> "Cluster1"))
  .load()
  val shopping_df =  spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "shopping_info", "keyspace" -> keyspace , "cluster" -> "Cluster1"))
  .load()
  
  //cust_df.printSchema()
  //shopping_df.printSchema()
  
  val join_df = cust_df.join(shopping_df, cust_df.col("cust_id").equalTo(shopping_df.col("cust_id")))
                        .drop(shopping_df.col("cust_id"))
  join_df.show()
  //use udf to discount
  val discountUdf = udf((price:Double, discount: Double) => price * discount)
  val discount_df = join_df.filter("cust_id=='2'").withColumn("discount_price", discountUdf(col("price"), lit(0.8)))
                            .select(col("cust_id"), col("name"), col("shopping_date"),
                                    col("shopping_id"), col("discount_price").alias("price"))
  
  discount_df.show()
  //use append save mode to update if exist, overwrite mode will trucate table first(need to go with option "confirm.truncate","true")
  discount_df.write.cassandraFormat("cust_with_shopping", keyspace, "Cluster1")
                /*.option("confirm.truncate", "true")*/.mode(org.apache.spark.sql.SaveMode.Append).save()
  println("write dataframe to cassandra successfully!")
  
  def setLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)//mark for MLlib INFO msg
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("io").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.ALL);
  }
}