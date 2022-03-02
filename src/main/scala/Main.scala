import Alg._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import scala.util.Random._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

object Main {

  def main(args: Array[String]): Unit = {
    hostGen()
    val (a, b, c) = priceGen()
    println(a)
    println(b)
    println(c)

    // This is the code from https://drive.google.com/drive/u/1/folders/1t8XbhV7p99POeHhWi6PAJBZ8p1KyDZMJ
    // Logger.getLogger("org").setLevel(Level.OFF)
    // Logger.getLogger("akka").setLevel(Level.OFF)

    // println("program started")

    // val conf = new SparkConf().setMaster("local[4]").setAppName("kafkar")
    // val ssc = new StreamingContext(conf, Seconds(2))

    // // my kafka topic name is 'mytest'
    // val kafkaStream = KafkaUtils.createStream(
    //   ssc,
    //   "localhost:2181",
    //   "spark-streaming-consumer-group",
    //   Map("test-topic" -> 5)
    // )
    // kafkaStream.print()
    // ssc.start
    // ssc.awaitTermination()
  }
}
