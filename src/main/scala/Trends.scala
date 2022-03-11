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
  def getMap(id: String): mutable.Map[String, String] = {
    val t1 = System.nanoTime
    var record = mutable.Map[String, String]()
    // Order ID and timestamp generation
    val time = createDateTime
    record += ("order_id" -> id, "datetime" -> timestampGen)

    // Using below code to work on futures, uncomment and feel free to change or experiment as you wish


    //create future definitions


    val host = hostNameGen

    val priceFuture: Future[(Double,Double,Int)] = Future {priceGen()}
    //val priceFuture: Future[(Double)] = Future {priceGen()._2}
    //val price1: Future[Double] = Future {priceGen()._1}
    val payFuture: Future[(String,String)] = Future {payStatusGen()}
    val locationFuture: Future[(String,String)] = Future {cityCountryGen(spark)}


    /*
    price1.onComplete({
      case Success(p1) => record += ("price" -> p1.toString)
      case Failure(exception) => record += ("price" -> "Bad Data")
    })
     */

    payFuture.onComplete({
      case Success(pay) => record += ("payment_type" -> payTypeGen, "payment_txn_id" -> payIdGen,"payment_txn_success" -> pay._1, "failure_reason" -> pay._2)
        val customerFuture: Future[(String,String)] = Future {cusRecord(nextInt(1000), isEnemyName(pay._1))}
        //Await.result(customerFuture,Duration.Inf)
        customerFuture.onComplete({
          case Success(customer) => record += ("customer_id" -> customer._1, "customer_name" -> customer._2)
          case Failure(exception) => record += ("customer_id" -> "Bad Data", "customer_name" -> "Bad Data")
        })
      //Await.result(customerFuture,Duration.Inf)
      case Failure(exception) => record += ("payment_type" -> "Bad Data", "payment_txn_id" -> "Bad Data","payment_txn_success" -> "Bad Data", "failure_reason" -> "Bad Data")
    })

    locationFuture.onComplete({
      case Success(location) => record += ("city" -> location._1, "country" -> location._2)
      case Failure(exception) => record += ("city" -> "Bad Data", "country" -> "Bad Data")
    })


    priceFuture.onComplete({
      case Success(price) => record += ("price" -> price._1.toString(),"unitPrice" -> price._2.toString, "qty" -> price._3.toString)
      case Failure(x) => println("Could not process: " + x.getMessage)
    })

    val (location1,location2) = Await.result(locationFuture, Duration.Inf)
    val fitStatus = fitness(location1)

    val pf: Future[(String,String,String,String,String)] = priceFuture.flatMap(a =>  Future {proRecord(nextInt(1000), a._2, host, fitStatus, time, spark)})

    pf.onComplete({
      case Success(product) => record += ("product_id" -> product._1, "product_name" -> product._2, "product_category" -> product._3, "original_price" -> product._4, "ecommerce_website_name" -> product._5)
      case Failure(t) => println("Could not process: " + t.getMessage)
    })

    Await.result(pf,Duration.Inf)
    


    ///////Below is working code without futures, comment out if you want to test the above stuff
    /*


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

    val location = cityCountryGen(spark)
    record += (
      "city" -> location._1,
      "country" -> location._2
    )

    val fitStatus = fitness(location._1)
    // create url and store
    val host = hostNameGen
    // record += ("ecommerce_website_name" -> urlGen(host, customer._2))
    // Product info generation
    val product =
      proRecord(nextInt(1000), price._2, host, fitStatus, time, spark)
    // def productFuture: Future[(String, String, String, String, String)] =
    // Future { proRecord(nextInt(1000), price._2, host, spark) }
    record += (
      "product_id" -> product._1,
      "product_name" -> product._2,
      "product_category" -> product._3,
      "original_price" -> product._4,
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

    // val location = cityCountryGen(spark)
    // def locationFuture: Future[(String, String)] = Future {
    //   cityCountryGen(spark)
    // }
    // record += (
    //   "city" -> location._1,
    //   "country" -> location._2
    // )

    //end the uncomment here for testing futures

     */



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
    if (x >= 24) {
      bool = true
    }
    if (x >= 26) {
      num = 1
    }
    if (x >= 28) {
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
