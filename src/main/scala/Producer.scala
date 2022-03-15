import Alg._
import Trends._

import scala.util.Random.nextInt
import scala.collection.mutable
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.kafka.clients.producer._

object Producer {
  //System.setProperty("hadoop.home.dir", "c:/winutils")
  //System.setProperty("hadoop.home.dir", "C:/hadoop")
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("P3")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // Create the DataFrames at global scope so that they are made once and used many times
  val dfA = spark.read
    .parquet("input/pq/amazon.parquet")
    .withColumn(
      "SellingPrice",
      col("SellingPrice").cast(DoubleType)
    )
    .select("ProductName", "Category", "SellingPrice", "ProductUrl")

  val dfW = spark.read
    .parquet("input/pq/walmart.parquet")
    .withColumn(
      "SalePrice",
      col("SalePrice").cast(DoubleType)
    )
    .select("ProductName", "Category", "SalePrice", "ProductUrl")

  val dfE = spark.read
    .parquet("input/pq/ebay.parquet")
    .withColumn("Price", col("Price").cast(DoubleType))
    .select("Title", "Price", "Pageurl")

  var count: Long = 1 // starting point for the incrementing id
  val props = new Properties()
  props.put("bootstrap.servers", "[::1]:9092")
  // props.put("bootstrap.servers", "3.86.155.113:9092")
  props.put(
    "key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer"
  )
  props.put(
    "value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer"
  )

  var result = mutable.Map.empty[String, String]
  val producer = new KafkaProducer[String, String](props)
  val topic = "Monday"

  def main(args: Array[String]): Unit = {
    Utilities.overwriteParquet() // this overwrites the parquets!!!
    dfA.persist(StorageLevel.MEMORY_ONLY_SER_2)
    dfW.persist(StorageLevel.MEMORY_ONLY_SER_2)
    dfE.persist(StorageLevel.MEMORY_ONLY_SER_2)

    try {
      while (true) {
        //send csv
        var th = new ProducerTh()
        th.setName(count.toString)
        th.start() // reassigns result
        Thread.sleep(100)
        count += 1
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      producer.close()
    }
  }

  def getKey(): String = {
    nextInt(1000).toString()
  }
}
