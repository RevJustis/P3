import Alg._
import Producer._
import DateTimeGenerator._
import Producer._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

import java.time._
import scala.collection.mutable
import scala.concurrent._
import scala.util.Random._

object Trends {
  def getMap(id: String): mutable.Map[String, String] = {
    val t1 = System.nanoTime
    var record = mutable.Map.empty[String, String]
    // Order ID and timestamp generation
    val time = createDateTime
    record += ("order_id" -> id, "datetime" -> timestampGen)

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
      proRecord(nextInt(1000), price._2, host, fitStatus, time, customer._2)
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
      record("qty") = (record("qty").toInt + nextInt(6)).toString
    } else if (
      record.keys.toString().contains("Food") && time.getDayOfMonth >= 24
    ) {
      record("qty") = (record("qty").toInt + nextInt(6)).toString
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
      "Time to create record " + id + "is: " + (System.nanoTime - t1) / 1e9 + " seconds."
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
        "Tel Aviv",
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
      } else {
        b = "false"
      }
      b
    } else "false"

  }

  //Decreases price for purchases made last week of the month
  def lastWeekDecrease(time: LocalDateTime): (Boolean, Double) = {
    //val date = time.
    val x = time.getDayOfMonth
    var bool = false
    var num = 0.0
    if (x >= 24) {
      bool = true
      num = (nextInt(20) + 1).toDouble / 100.0
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

  def pillow(name: String): Boolean = {
    val result = name.split(" ")(0)
    val count = result.toCharArray.length
    var long = false
    if (count > 7 && count < 20) {
      long = true
    } else { long = false }
    long
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

  def holidayIncrease(): Boolean = {
    val rand = nextInt(3)
    var q = false
    if (rand == 0) {
      q = true

      // val x = time.getMonth
      //val y = time.getDayOfMonth

      /* if (x == 11) {
        if (y >= 26) {
          q = true
        }
      }
      if (x == 12) {
        if (y < 25 && y >= 1) {
          q = true
        }
      }

      if (x == 1) {
        if (y <= 3 && y >= 1) {
          q = true
        }

      }
       */

    }
    q

  }
}
