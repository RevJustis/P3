import Alg._
import Main._
import Producer.ID
import scala.util.Random._
import scala.collection.mutable

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

import scala.collection.mutable
import scala.util.Random._

object Trends {
  def getMap(): mutable.Map[String, String] = {
    val t1 = System.nanoTime
    var record = mutable.Map[String, String]()
    // Order ID and timestamp generation
    record += ("order_id" -> Main.ID.toString, "datetime" -> timestampGen)
    Main.ID += 1

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
      "The execution time of the function is: " + (System.nanoTime - t1) / 1e9 + " seconds."
    )
    record
  }

  def megaTrendLogic(record: mutable.Map[String, String]): Unit = {
    if (record("customer_name").length >= 8) record("product_name") = "pillow"
  }

  def isEnemyName(pay: String): Boolean = {
    if (pay == "N") {
      val weight = nextInt(10)
      if (nextInt(10) == 0) true
    }
    false
  }

  def spenderCities(): String = {
    var rand = nextInt(10)
    if (rand == 0) {
      val x = IndexedSeq(
        "New York",
        "Paris",
        "Hong Kong",
        "Singapore",
        "Zurich",
        "Tel Aviv",
        "Osaka",
        "Los Angeles",
        "Geneva",
        "Copenhagen"
      )
      x(nextInt(x.length))
    } else "Other"
  }

  def fitness(): Unit = {}
  def payFailTime(time: String, status: String): Boolean = {

    if (time <= "00:01:00" && time >= "00:04:00") {

      val w = nextInt(10)
      if (w <= 4) {
        status == "N"
      }
    } else {
      "Y"
    }
    false
  }
}
