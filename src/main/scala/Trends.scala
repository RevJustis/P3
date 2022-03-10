import Alg._
import DateTimeGenerator._
import Producer._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

import java.time._
import scala.collection.mutable
import scala.util.Random._
import scala.concurrent._
import scala.util.{Failure, Success}
import scala.concurrent.duration.Duration
import ExecutionContext.Implicits.global

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
      "customer_name" -> customer._2,
      "city" -> customer._3,
      "country" -> customer._4
    )

    /*val location = cityCountryGen(spark)
    record += (
      "city" -> location._1,
      "country" -> location._2
    )*/

    val fitStatus = fitness(customer._3)
    // create url and store
    val host = hostNameGen
    // record += ("ecommerce_website_name" -> urlGen(host, customer._2))
    // Product info generation
    val product = {
      proRecord(nextInt(1000), price._2, host, fitStatus, time, spark)
    }
    val tempPrice = product._4.toDouble
    record += (
      "product_id" -> product._1,
      "product_name" -> product._2,
      "product_category" -> product._3,
      "original_price" -> f"$tempPrice%1.2f",
      "ecommerce_website_name" -> product._5
    )
    //if price is decreased at the end of the month, increase qty purchased
    if (
      record.keys.toString().contains("Clothing") && time.getDayOfMonth >= 24
    ) {
      record("qty") = (record("qty").toInt + 5).toString
    } else if (
      record.keys.toString().contains("Food") && time.getDayOfMonth >= 24
    ) {
      record("qty") = (record("qty").toInt + 5).toString
    }

    //if order is in certain countries, change price
    if (record.keys.toString().contains("Canada")) {
      val newPrice = (record("price").toDouble) * 1.2
      val newTotal = (record("unitPrice").toDouble) * 1.2
      record("price") = f"$newPrice%1.2f"
      record("unitPrice") = f"$newTotal%1.2f"
    } else if (record.keys.toString().contains("Venezuela")) {
      val newPrice = (record("price").toDouble) * 5.0
      val newTotal = (record("unitPrice").toDouble) * 5.0
      record("price") = f"$newPrice%1.2f"
      record("unitPrice") = f"$newTotal%1.2f"
    } else if (record.keys.toString().contains("South Africa")) {
      val newPrice = (record("price").toDouble) * 0.5
      val newTotal = (record("unitPrice").toDouble) * 0.5
      record("price") = f"$newPrice%1.2f"
      record("unitPrice") = f"$newTotal%1.2f"
    } else if (record.keys.toString().contains("Vietnam")) {
      val newPrice = (record("price").toDouble) * 0.8
      val newTotal = (record("unitPrice").toDouble) * 0.8
      record("price") = f"$newPrice%1.2f"
      record("unitPrice") = f"$newTotal%1.2f"
    } else if (record.keys.toString().contains("Monaco")) {
      val newPrice = (record("price").toDouble) * 3.0
      val newTotal = (record("unitPrice").toDouble) * 3.0
      record("price") = f"$newPrice%1.2f"
      record("unitPrice") = f"$newTotal%1.2f"
    }
    //end the uncomment here for testing futures

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

  //Customers from these cities buy fitness items 50% of the time
  def fitness(city: String): String = {
    var b = ""
    var num = 0
    val rand = nextInt(1)
    if (rand == 0) {
      val x = IndexedSeq(
        "Los Angeles",
        "Amsterdam",
        "Berlin",
        "Copenhagen",
        "Stockholm",
        "San Francisco",
        "Boston",
        "Dublin",
        "Montreal",
        "Miami"
      )
      for (i <- x) {
        if (i == city) {
          num += 1
        } else {
          num += 0
        }
      }
      if (num == 1) {
        b = "true"
      } else { b = "false" }
      b
    } else "false"

  }

  //Decreases price for purchases made last week of the month
  def lastWeekDecrease(time: LocalDateTime): (Boolean, Int) = {
    //val date = time.
    val x = time.getDayOfMonth
    var bool = false
    var num = 0
    if (x >= 24 && x < 26) {
      bool = true
    }
    else if (x >= 26 && x < 28) {
      num = 1
    }
    else if (x >= 28) {
      num = 2
    }
    (bool, num)
  }

  def payFailTime(time: LocalDateTime): Boolean = {

    var status = " "
    val hour = time.getHour
    if (hour >= 1 && hour <= 4) {
      val w = nextInt(10)
      if (w <= 4) {
        status = "N"
      } else {
        status = "Y"
      }
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
