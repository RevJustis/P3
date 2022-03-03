import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

import scala.collection.JavaConverters._

object streaming {

  import java.util.Properties
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:/winutils")
    val spark = SparkSession
      .builder()
      .appName("test")
      .config("spark.master","local")
      .getOrCreate()


    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "[::1]:9092")
      .option("subscribe", "topic1")
      .load()
    df.printSchema()
    import spark.implicits._
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)","CAST(topic AS STRING)","CAST(partition AS STRING)","CAST(offset AS STRING)","CAST(timestamp AS STRING)","CAST(timestampType AS STRING)").as[(String,String,String,String,String,String,String)]
    val query = df.writeStream
      .outputMode("append")
      .format("console")
      .start()

    // some statement
    // some statement
    query.awaitTermination();



  }
}

