import Alg._
import Trends._

import scala.util.Random._
import scala.collection.mutable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {
  System.setProperty("hadoop.home.dir", "C:\\hadoop")
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

  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
    dfA.cache()
    dfW.cache()
    dfE.cache()

    var id = 1 // starting point for the incrementing id
    var i = 0
    while (i < 3) {
      var record = mutable.Map[String, String]()
      // Order ID and timestamp generation
      record += ("order_id" -> id.toString, "datetime" -> timestampGen)
      id = id + 1

      // Price, unit price and quantity gen
      val price = priceGen // Total, Unit, qty
      record += (
        "price" -> price._1.toString(),
        "unitPrice" -> price._2.toString(),
        "qty" -> price._3.toString()
      )

      // Payment info generation
      val pay = payStatusGen
      record += (
        "payment_type" -> payTypeGen,
        "payment_txn_id" -> payIdGen,
        "payment_txn_success" -> pay._1,
        "failure_reason" -> pay._2
      )

      // Customer info gen
      val customer = cusRecord(nextInt(1000), isEnemyName(pay._1))
      record += (
        "customer_id" -> customer._1,
        "customer_name" -> customer._2
      )

      // create url and store
      val host = hostNameGen
      // record += ("ecommerce_website_name" -> urlGen(host, customer._2))
      // Product info generation
      val product = proRecord(nextInt(1000), price._2, host, spark)
      record += (
        "product_id" -> product._1,
        "product_name" -> product._2,
        "product_category" -> product._3,
        "original_price" -> product._4,
        "ecommerce_website_name" -> product._5
      )

      val location = cityCountryGen(spark)
      record += (
        "city" -> location._1,
        "country" -> location._2
      )

      println(
        record
      ) // Print the record (the map) to console so we can see that it is all good

      val duration = (System.nanoTime - t1) / 1e9d

      println()
      println(
        "The execution time of the function is: " + duration + " seconds."
      )

      i += 1
    }
  }
}
