name := "streaming-piplines"

version := "1.0.0"

scalaVersion := "2.11.12"




libraryDependencies ++= Seq(
  "io.delta" %% "delta-core" % "0.6.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.5",
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.kafka" % "kafka-clients" % "2.8.1",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.4.5",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.4.5",
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % "2.2.1"
)
dependencyOverrides += "com.google.guava" % "guava" % "15.0"
