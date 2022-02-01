//import jdk.internal.org.jline.keymap.KeyMap.display
import org.apache.kafka
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions.{col, from_json}
import scala.util.control.Breaks
import org.apache.spark.sql.DataFrame
import java.util.logging.StreamHandler
import scala.util.Try


case class DeviceData(ts: String  ,board_version: String,computer_name: String,cpu_brand: String,cpu_logical_cores:String, cpu_microcode:String, partitionpath: String)


object StreamHandler {
  def main(args: Array[String]) {
    println("asdf")
    val spark = SparkSession.builder.
      master("local")
      .appName("spark")
      .getOrCreate()

    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    


    val tablename = "table_name"
    val basepath = "table location"
    val basepath1 = "table location"
    val osquery_names = List("pack_system-snapshot_some_query4")
    var count = 0
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "Orders1")
      .option("failOnDataLoss", "false")
      .option("startingOffsets", "earliest")
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
                       
                        val parsed = filteredDF.select("value").rdd.map(r => r(0)).collect().map(x =>x.toString.dropRight(1).drop(1).slice(x.toString.indexOf("columns") + 8, x.toString.indexOf("}") ))
                       

                        val gp = parsed.map(x => x.toString.slice(x.toString.indexOf("[") + 1, x.toString.indexOf("]") ))
                         


                        var str = "["
                        parsed.foreach(x => {str = str + x.toString + ","})
                        str = str.dropRight(1)
                        str = str + "]"

                      
                        if(str == "]")outLoop.break()
                        else
                          {
                          //  println(str.toString)
                          }


                        str = str.replaceAll("\"\"","\"temp\"")
                        

                        val tempStr = Seq(str)

                        val df_final = tempStr.toDF("json")

                        val schema = df_final.select(schema_of_json(df_final.select(col("json")).first.getString(0))).as[String].first
                        

                        val expr_string = "from_json(json, '"+schema.toString + "') as parsed_json"

                        val parsedJson1 = df_final.selectExpr(expr_string)
                        
                        val data = parsedJson1.selectExpr("explode(parsed_json) as json").select("json.*").withColumn("id",monotonicallyIncreasingId())
                       
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
                       
   
                        data_new.
                          write.
                          format("delta").
                          mode(Append).
                          option("checkpointLocation", "tmp6"+query_name).
                          save("tmp6"+query_name)



                      }
                    })
                    batchDF.unpersist()
                    println("Done writing the batch !")
                  }
                df2.start().awaitTermination()
    
 

  }
  
}
