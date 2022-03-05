import Alg._
import Trends._

import scala.util.Random._
import scala.collection.mutable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      .getOrCreate()

    var id = 1 // starting point for the incrementing id
    var keepLooping = true
    while (keepLooping) {
      var record = mutable.Map[String, String]()
      // Order ID and timestamp generation
      record += ("order_id" -> id.toString, "datetime" -> timestampGen)
      id = id + 1

      // Price, unit price and quantity gen
      val price = priceGen() // Total, Unit, qty
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
      val host = hostNameGen()
      record += ("ecommerce_website_name" -> urlGen(host, customer._2))
      // Product info generation
      val product = proRecord(nextInt(1000), price._2, host, spark)
      record += (
        "proName" -> product._2,
        "proType" -> product._3,
        "proID" -> product._1
      )

      val location = randomCityCountry(spark)
      record += (
        "city" -> location._1,
        "country" -> location._2
      )

      println(
        record
      ) // Print the record (the map) to console so we can see that it is all good

      // FIXME the code currently coded to only loop once
      keepLooping = false
    }
  }
}
