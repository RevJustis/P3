import Test._
import Trends._
import Producer.{getKey, topic, producer}
import spark.implicits._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.kafka.clients.producer._

class ProducerTh extends Thread {
  override def run() {
    val n = Thread.currentThread().getName()
    // Displaying the thread that is running
    // println("Thread " + n + " is running.")
    val result = getMap(n)
    result.toSeq.toDF.show
    val key = getKey()

    //send json
    //  val value = s"""{"order_id": "$order_id","customer_id": "$customer_id","customer_name": "$customer_name","product_id": "$product_id","product_name": "$product_name","product_category": "$product_category","payment_type": "$payment_type","qty": "$qty","price": "$price","datetime": "$datetime","country": "$country","city": "$city","ecommerce_website_name": "$ecommerce_website_name","payment_txn_id": "$payment_txn_id","payment_txn_success": "$payment_txn_success","failure_reason": "$failure_reason"}"""
    //send csv

    val value: String =
      result("order_id") + "," +
        result("customer_id") + "," +
        result("customer_name") + "," +
        result("product_id") + "," +
        result("product_name") + "," +
        result("product_category") + "," +
        result("payment_type") + "," +
        result("qty") + "," +
        result("price") + "," +
        result("datetime") + "," +
        result("country") + "," +
        result("city") + "," +
        result("ecommerce_website_name") + "," +
        result("payment_txn_id") + "," +
        result("payment_txn_success") + "," +
        result("failure_reason")

    val record = new ProducerRecord[String, String](
      topic,
      key,
      value
    )
    producer.send(record)
    println("New record sent...")
  }
}
