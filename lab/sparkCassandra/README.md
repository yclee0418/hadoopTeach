#### Spark and Cassandra Integration Sample
* Reference: [https://ithelp.ithome.com.tw/articles/10188914](https://ithelp.ithome.com.tw/articles/10188914)
* All project need to use sbteclipse to load related libraries: [https://github.com/yclee0418/sparkTeach/tree/master/sbt](https://github.com/yclee0418/sparkTeach/tree/master/sbt)
* Day29_Cassandra.scala : 
  * how to connect to cassandra with sparkContext
    * The default configuration of cassandra <B>only allow accessing localhost</B>, need to modify `conf/cassandra.yaml` to <B>allow accessing from other machine</B>: 
    <pre><code>#ip of cassandra mechine: 192.168.0.221
    - seeds: "192.168.0.221"
    listen_address: 192.168.0.221
    rpc_address: 192.168.0.221
    </code></pre>
  * how to get data from cassandra table 
  * using where condition
  * write data to cassandra table
