name := "spark-twitter-lang-classifier"

version := "2.0.0"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVer = "2.2.1"
  Seq(
    "org.apache.spark"     %% "spark-core"              % sparkVer ,
    "org.apache.spark"     %% "spark-mllib"             % sparkVer ,
    "org.apache.spark"     %% "spark-sql"               % sparkVer ,
    "org.apache.spark"     %% "spark-streaming"         % sparkVer ,
    "org.apache.bahir"     %% "spark-streaming-twitter" % "2.0.0" ,
    "com.google.code.gson" %  "gson"                    % "2.8.0" ,
    "org.twitter4j"        %  "twitter4j-core"          % "4.0.5" ,
    "com.github.acrisci"   %% "commander"               % "0.1.0"   
  )
}

