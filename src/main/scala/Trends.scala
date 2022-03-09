import Alg._
import Producer._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

import scala.collection.mutable
import scala.util.Random
import scala.util.Random._
import DateTimeGenerator._
import java.time._

object Trends {
  def getMap(): mutable.Map[String, String] = {
    val t1 = System.nanoTime
    var record = mutable.Map[String, String]()
    // Order ID and timestamp generation
    val time = createDateTime
    record += ("order_id" -> ID.toString, "datetime" -> timestampGen)
    ID += 1

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
    val product = proRecord(nextInt(1000), price._2, host, time, spark)
    record += (
      "product_id" -> product._1,
      "product_name" -> product._2,
      "product_category" -> product._3,
      "original_price" -> product._4,
      "ecommerce_website_name" -> product._5
    )
    //if price is decreased at the end of the month, increase qty purchased
    if(record("product_category").contains("Clothing") && time.getDayOfMonth >= 24) {
      record("qty") = (record("qty").toInt + 5).toString
    } else if(record("product_category").contains("Food") && time.getDayOfMonth >= 24) {
      record("qty") = (record("qty").toInt + 5).toString
    }

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
    val rand = nextInt(10)
    if (rand == 0) {
      val x = IndexedSeq(
        "New York",
        "Paris",
        "Hong Kong",
        "Singapore",
        "Zurich",
        "Tel Aviv-Yafo",
        "Osaka",
        "Los Angeles",
        "Geneva",
        "Copenhagen"
      )
      x(nextInt(x.length))
    } else "Other"
  }

  def fitness(): Unit = {
    println("")
  }

  //Decreases price for purchases made last week of the month
  def lastWeekDecrease(time: LocalDateTime): (Boolean, Int) = {
    //val date = time.
    val x = time.getDayOfMonth
    var bool = false
    var num = 0
    if (x >= 24) {
      bool = true
    }
    if(x >= 26){
      num = 1
    }
    if (x >= 28){
      num = 2
    }
    (bool, num)

  }

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

  def pillow(): Unit = {
    val name = cusNameGen().length
    if (name > 10 && name < 20) {
      println("hello")
    }
  }

  def nameFailPay(name: String): String = {
    var payStatus = ""
    if (name == "Ava") {
      payStatus = "Y"
    } else {
      payStatus = ""
    }
    payStatus
  }
}
