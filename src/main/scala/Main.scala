import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka
import org.apache.spark.streaming.StreamingContext._
import Alg._

object Main {
  def main(args: Array[String]) = {
    //some generator tests
    println("This is a random name " + nameGen)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("program started")

    val conf = new SparkConf().setMaster("local[4]").setAppName("kafkar")
    val ssc = new StreamingContext(conf, Seconds(5))

    //my kafka topic name is 'mytest'
    val kafkaStream = KafkaUtils.createStream(
      ssc,
      "localhost:2181",
      "spark-streaming-consumer-group",
      Map("test-topic" -> 5)
    )
    kafkaStream.print()
    ssc.start
    ssc.awaitTermination()
  }
}
