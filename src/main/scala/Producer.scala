import Alg._
import Trends._

import scala.util.Random._
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

  var ID = 1 // starting point for the incrementing id

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    // Utilities.overwriteParquet()
    dfA.persist(StorageLevel.MEMORY_ONLY_SER_2)
    dfW.persist(StorageLevel.MEMORY_ONLY_SER_2)
    dfE.persist(StorageLevel.MEMORY_ONLY_SER_2)

    val props = new Properties()
    //3.86.155.113:9092
    props.put("bootstrap.servers", "3.86.155.113:9092")
    props.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    val producer = new KafkaProducer[String, String](props)

    try {
      while (true) {
        //send csv
        sendRecord(producer, "pandoras_box")
        //send json
        //  sendRecord(producer,"json")

        Thread.sleep(2000)
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
    val r = scala.util.Random
    val key = r.nextInt(1000).toString()
    key
  }

  def sendRecord(
      producer: KafkaProducer[String, String],
      topic: String
  ): Unit = {
    // TODO replace the placeholder
    val result = getMap("placeholder")

    val key = getKey()

    val order_id = result("order_id")
    val customer_id = result("customer_id")
    val customer_name = result("customer_name")
    val product_id = result("product_id")
    val product_name = result("product_name")
    val product_category = result("product_category")
    val payment_type = result("payment_type")
    val qty = result("qty")
    val price = result("price")
    val datetime = result("datetime")
    val country = result("country")
    val city = result("city")
    val ecommerce_website_name = result("ecommerce_website_name")
    val payment_txn_id = result("payment_txn_id")
    val payment_txn_success = result("payment_txn_success")
    val failure_reason = result("failure_reason")

    //send json
    //  val value = s"""{"order_id": "$order_id","customer_id": "$customer_id","customer_name": "$customer_name","product_id": "$product_id","product_name": "$product_name","product_category": "$product_category","payment_type": "$payment_type","qty": "$qty","price": "$price","datetime": "$datetime","country": "$country","city": "$city","ecommerce_website_name": "$ecommerce_website_name","payment_txn_id": "$payment_txn_id","payment_txn_success": "$payment_txn_success","failure_reason": "$failure_reason"}"""
    //send csv
    val value =
      s"$order_id," + s"$customer_id," + s"$customer_name," + s"$product_id," + s"$product_name," + s"$product_category," + s"$payment_type," + s"$qty," + s"$price," + s"$datetime," + s"$country," + s"$city," + s"$ecommerce_website_name," + s"$payment_txn_id," + s"$payment_txn_success," + s"$failure_reason,"
    val record = new ProducerRecord[String, String](
      topic,
      key,
      value
    )
    producer.send(record)
    println("New record sent...")
  }
}
