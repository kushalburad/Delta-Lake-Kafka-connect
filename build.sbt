name := "streaming-piplines"

version := "1.0.0"

scalaVersion := "2.11.12"




libraryDependencies ++= Seq(
  "io.delta" %% "delta-core" % "0.6.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.5",
  "org.apache.hudi" %% "hudi-spark-bundle" % "0.9.0",
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.kafka" % "kafka-clients" % "2.8.1",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.4.5",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.4.5",
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % "2.2.1",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0",
  "com.github.catalystcode" %% "streaming-reddit" % "0.0.1",
  "com.amazonaws" % "amazon-kinesis-client" % "1.8.9"
)
//dependencyOverrides += "com.google.guava" % "guava" % "15.0"


//org.apache.spark spark-core_2.11 2.3.0 provided

//org.apache.spark spark-sql_2.11 2.3.0

//org.apache.spark spark-sql-kafka-0-10_2.11 2.3.0

//org.apache.kafka kafka-clients 0.10.1.0
//libraryDependencies += "org.apahe.spark" % "spark-streaming-kafka-0-8_2.11" % "2.4.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
//libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.3" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"