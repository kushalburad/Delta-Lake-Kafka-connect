//import jdk.internal.org.jline.keymap.KeyMap.display
import org.apache.kafka
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

import scala.reflect.internal.util.TableDef
import io.delta.tables._
import io.netty.handler.codec.smtp.SmtpRequests.data
import org.apache.spark.sql.functions._

import java.time.LocalDateTime
import org.apache.spark.sql._

import scala.util.parsing.json._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._

import java.util.logging.StreamHandler
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import java.time.LocalDateTime
import org.apache.spark.sql.Row
import org.apache.spark.sql.Row.merge

import javax.json.JsonValue
import scala.collection.mutable.ListBuffer
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{MapType, StringType}
import org.json4s.Merge.merge

import scala.Any
import scala.util.control.Breaks

import org.apache.spark.sql._
import scala.util.control._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.from_json

import scala.util.parsing.json._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper

import java.util.logging.StreamHandler
import scala.util.Try
//import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import java.time.LocalDateTime







case class DeviceData(ts: String  ,board_version: String,computer_name: String,cpu_brand: String,cpu_logical_cores:String, cpu_microcode:String, partitionpath: String)


object getstreamingdata {
  def main(args: Array[String]) {
    println("asdf")
    val spark = SparkSession.builder.
      master("local")
      .appName("spark")
      .getOrCreate()

    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    //spark.sparkContext.hadoopConfiguration.setClass("mapreduce.input.pathFilter.class", classOf[org.apache.hudi.hadoop.HoodieROTablePathFilter], classOf[org.apache.hadoop.fs.PathFilter]);
/*

    val tablename = "table_name"
    val basepath = "/home/kushal/Desktop/tmp1"
    val basepath1 = "/tmp/"+"table_name"
    val osquery_names = List("pack_system-snapshot_some_query1", "pack_system-snapshot_some_query3")
    var count = 0
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "Orders6")
      .option("failOnDataLoss", "false")
      // .option("startingOffsets", "earliest")
      .load()




                val df2 =  df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)", "CAST(partition AS INT)", "CAST(offset AS INT)", "CAST(timestamp AS STRING)")
                  .writeStream
                  .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
                    println("Writing batch : "+ batchId.toString + "having " + batchDF.count().toString + " records.")
                    batchDF.persist()
                     println("asdf")
                    //Filter queries
                    osquery_names.foreach(query_name =>{
                      val outLoop = new Breaks
                      outLoop.breakable{
                        val filteredDF = batchDF.filter("value like '%" + query_name+ "%'")
                        filteredDF.show(50,false)
                        println("    " + query_name + " has " + filteredDF.count().toString + " records.")
                        if(filteredDF.count() == 0)outLoop.break()
                       // filteredDF.show(50,false)
                        val parsed = filteredDF.select("value").rdd.map(r => r(0)).collect().map(x =>x.toString.dropRight(1).drop(1).slice(x.toString.indexOf("columns") + 8, x.toString.indexOf("}") ))
                        //parsed.foreach(x=> println(x))
                        //val parsed = filteredDF.select("value").rdd.map(r => r(0)).collect().map(x =>x.toString)


                        val gp = parsed.map(x => x.toString.slice(x.toString.indexOf("[") + 1, x.toString.indexOf("]") ))
                         //gp.foreach(x=> println(x))


                        var str = "["
                        parsed.foreach(x => {str = str + x.toString + ","})
                        str = str.dropRight(1)
                        str = str + "]"

                        println("str1")

                        println(str.toString)

                        if(str == "]")outLoop.break()
                        else
                          {
                            println(str.toString)
                          }


                        str = str.replaceAll("\"\"","\"temp\"")
                        //            println(gp.getClass)
                        //            println("Incoming " + str)

                        val tempStr = Seq(str)

                        val df_final = tempStr.toDF("json")

                        val schema = df_final.select(schema_of_json(df_final.select(col("json")).first.getString(0))).as[String].first
                        //            println(schema.toString)

                        val expr_string = "from_json(json, '"+schema.toString + "') as parsed_json"

                        //            println( "EXPR : " + expr_string)
                        //            df_final.select(to_json(schema))
                        //            df_final.show()
                        val parsedJson1 = df_final.selectExpr(expr_string)
                        //parsedJson1.show(20,false)

                        val data = parsedJson1.selectExpr("explode(parsed_json) as json").select("json.*").withColumn("id",monotonicallyIncreasingId())
                       // data.show(50,false)
                        var df_1 = filteredDF.withColumn("id1",monotonically_increasing_id())
                        var df_2 =data.withColumn("id2", monotonically_increasing_id())
                        println("print df2")
                        df_2.show(50,false)

                        def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
                        val hasTSasColumn = hasColumn(df_2, "timestamp")

                        if(hasTSasColumn == true){
                          df_2 = df_2.drop("timestamp")
                        }


                        val data_new = df_1.join(df_2,col("id1")===col("id2"),"inner")
                          .drop("id1","id2")
                        println("data new")
                        data_new.show(50,false)

    println("newdata")
                        data_new.
                          write.
                          format("delta").
                          mode(Append).
                          option("checkpointLocation", "/home/kushal/Desktop/tmp/"+query_name).
                          save("/home/kushal/Desktop/tmp/"+query_name)



                      }
                    })
                    batchDF.unpersist()
                    println("Done writing the batch !")
                  }
                df2.start().awaitTermination()


    */
        /*

            var count = 0
            val df = spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", "localhost:9092")
              .option("subscribe", "Orders2")
              .option("failOnDataLoss", "false")
              .option("startingOffsets", "earliest")
              .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)", "CAST(partition AS INT)", "CAST(offset AS INT)", "CAST(timestamp AS STRING)").writeStream
              .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
                println("Writing batch : " + batchId.toString())
                batchDF.persist()

                osquery_names.foreach(x => {
                  batchDF
                    .filter("value like '%" + x + "%'").
                    write.
                    format("delta")
                    .mode(Append)
                    .option("checkpointLocation", "/home/kushal/Desktop/tmp1/"+x)
                    .save("/home/kushal/Desktop/tmp1/"+x)

                  //                format("hudi").
                  //                options(getQuickstartWriteConfigs).
                  //                option(PRECOMBINE_FIELD.key(), "key").
                  //                option(RECORDKEY_FIELD.key(), "key").
                  //                option(PARTITIONPATH_FIELD.key(), "_hoodie_partition_path").
                  //                option(TABLE_NAME.key(), query_name).
                  //                mode(Overwrite).
                  //                save(basepath)

                })
                batchDF.unpersist()
              }.start().awaitTermination()


        */


        //              start(basepath)
        //              .awaitTermination()


        //      batchDF.write // Use Cassandra batch data source to write streaming out
        //        .cassandraFormat(tableName, keyspace)
        //        .option("cluster", clusterName)
        //        .mode("append")
        //        .save()


        //      batchDF.write // Use Cassandra batch data source to write streaming out
        //        .cassandraFormat(tableName, keyspace)
        //        .option("cluster", clusterName)
        //        .mode("append")
        //        .save()


        /*.select(json_tuple(col("value").cast("string"), "name").alias("name"))
    .where("name == 'pack_system-snapshot_some_query3'")
    .select("*")
    .distinct()
    */


        // val vgDF = spark.sql("select value as value1 from data where value like '%pack_system-snapshot_some_query1%' ")
        //val vgDF1 = spark.sql("select value as value2 from data where value like '%pack_system-snapshot_some_query3%' ")
        //val vgDF3 = spark.sql("select value from fata")
        //vgDF.show(50,false)


        // .select((col("value").cast("string")))
        //.distinct()
        //.select("\"columns\"")


        /*
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "Orders1")
      .option("startingOffsets", "earliest")
      .load()
    println("asdf")
     /*val df= spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "Orders1")
    .option("startingOffsets", "latest")
    .load()
    .selectExpr("CAST(value AS STRING)")*/


    /*println(df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)"));

    val query = df
      .writeStream
      .format("console")
      .start()
      .awaitTermination()
    val query = df
      .writeStream
      .format("console")
      .start()
    println(query)

     /*df.printSchema()
    // print(df.col("timestamp"))
    df.writeStream
      .format("console")
      .option("truncate","false")
      .start()

    //  df.sqlContext.sql("select * from df")
    */
    // val stream = df.writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoint").start("/tmp/delta-table")


    val query1 = df.writeStream
      .outputMode("Append")
      .format("console")
      .start()
      .awaitTermination()

    */




    //val stream2 = spark.readStream.format("delta").load("/home/kushal/Downloads/DMW_MiniProject_2018-master/Churn_Modelling.csv/").writeStream.format("console").start()
    //println(stream2)


    val query1 = df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
      .writeStream
      .outputMode("Append")
      .format("console")
      .start()
      .awaitTermination()

    val read_schema = ("id int, " +
      "firstName string, " +
      "middleName string, " +
      "lastName string, " +
      "gender string, " +
      "birthDate timestamp, " +
      "ssn string, " +
      "salary int")
    val json_read_path = "/home/kushal/Desktop/tmp/people-10m"
    val checkpoint_path = "/home/kushal/Desktop/tmp/people-10m/checkpoints"
    val save_path = "/home/kushal/Desktop/tmp/people-10m"

    val people_stream = (spark
      .readStream
      .schema(read_schema)
      .option("maxFilesPerTrigger", 1)
      .option("multiline", true)
      .json(json_read_path))

    people_stream.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpoint_path)
      .start(save_path)

    */
        var emp_data = spark.read.csv("/home/kushal/Downloads/DMW_MiniProject_2018-master/Churn_Modelling.csv")
        println(emp_data.head())

        /*
    val people = spark
      .read
      .format("csv")
      .load("/home/kushal/Downloads/DMW_MiniProject_2018-master/Churn_Modelling.csv")

    // Write the data to its target.
    people.write
      .format("delta")
      .mode("overwrite")
      .save("/home/kushal/Desktop/tmp/people-10m")



      */

        //val df1=spark.read.format("delta").option("versionAsof",1).load("/home/kushal/Desktop/tmp/people-10m")
        //df1.show()


        //df.writeStream.format("delta").outputMode("append").option("checkpointLocation", checkpoint_path).option("checkpointLocation", checkpoint_path).start("/home/kushal/Desktop/tem1/people-10m")


        //read the data


        /*
    val rawDF = df.selectExpr("CAST(value AS STRING )")

    import spark.implicits._
    val df2 = df.select(split(col("value"),",").getItem(0).as("A"),
      split(col("value"),",").getItem(1).as("B"),
      split(col("value"),",").getItem(2).as("C"),
    split(col("value"),",").getItem(3).as("D"),
    split(col("value"),",").getItem(4).as("E"),
    split(col("value"),",").getItem(5).as("F"),
    split(col("value"),",").getItem(6).as("G"))
    .drop("value")



      df.writeStream
      .format("delta")
      .outputMode("Append")
      .option("mergeSchema", "true")
      .option("checkpointLocation", "/home/kushal/Desktop/tmp3/people-10m")
      .start("/home/kushal/Desktop/tmp3/people-10m")
      .awaitTermination()

    */
        // print the data
        import spark.implicits._


        /*
            def jsonToMap(js:String) : Map[String,Any] = {
              return JSON.parseFull(js).get.asInstanceOf[Map[String,Any]]
            }


             val df9 = spark.read.format("delta").option("versionAsof", 3).load("/home/kushal/Desktop/tmp1/pack_system-snapshot_some_query3")
             val df10 =df9.select("value").foreach(x =>( jsonToMap(x(0).toString()).get("columns")     )     )
            println(df10)
          */




    val outputdf = spark.read.format("delta").option("versionAsof", 3).load("/home/kushal/Desktop/tmp3/pack_system-snapshot_some_query1").show(50,false)
    //outputdf.withColumn("size",col("size")+1).show(50,false)

    //outputdf.write.format("delta").mode("overwrite").option("checkpointLocation", "/home/kushal/Desktop/tmp1/pack_system-snapshot_some_query1").save("/home/kushal/Desktop/tmp1/pack_system-snapshot_some_query1")



    val deltaTable=DeltaTable.forPath(spark,"/home/kushal/Desktop/tmp3/pack_system-snapshot_some_query1").history()
    deltaTable.show(50,false)
    val hist= deltaTable.select("operationParameters").rdd.map(x => (x).toString().slice(x.toString.indexOf("->") + 2, x.toString.indexOf(",") ))
    for(x<-hist)
    {
      println(x)
    }
   /* val deltaTable1 = DeltaTable.forPath("/home/kushal/Desktop/tmp3/pack_system-snapshot_some_query1")
    deltaTable1.generate("symlink_format_manifest")
    */

    //val parsed = filteredDF.select("value").rdd.map(r => r(0)).collect().map(x =>x.toString.dropRight(1).drop(1).slice(x.toString.indexOf("columns") + 8, x.toString.indexOf("}") ))






  }


}