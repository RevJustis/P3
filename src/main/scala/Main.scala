import Alg._

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
      var p = mutable.Map[String, String]()
      // Order ID and timestamp generation
      p += ("order_id" -> id.toString, "datetime" -> timestampGen)
      id = id + 1
      // Customer info gen
      val customer = cusRecord(nextInt(1000))
      p += (
        "customer_id" -> customer._1,
        "customer_name" -> customer._2
      )

      // val location = locGen
      // produced += (
      //"country" -> location._1
      // "city" -> location._2,
      // )
      // Price, unit price and quantity gen
      val price = priceGen() // Total, Unit, qty
      p += ("price" -> price._1.toString())
      p += ("unitPrice" -> price._2.toString())
      p += ("qty" -> price._3.toString())


      // create url and store
      val host = hostNameGen()
      p += ("ecommerce_website_name" -> urlGen(host))
      // Product info generation
      val product = proRecord(nextInt(1000), price._2, host, spark)
      p += (
      "proName" -> product._2,
      "proType" -> product._3,
      "proID" -> product._1
      )
      // Payment info generation
      val pay = payStatusGen

      p += (
        "payment_type" -> payTypeGen,
        "payment_txn_id" -> payIdGen,
        "payment_txn_success" -> pay._1,
        "failure_reason" -> pay._2
      )
      if(pay._1 == "N") {
        val weight = nextInt(10)
        val random = new Random()
        val x = IndexedSeq(
          "Yash Dhayal",
          "Hyung Ro Yoon",
          "Betty Boyett",
          "Bryan Chou",
          "Mandeep Atwal",
          "Jacob Nottingham",
          "Brandon Conover",
          "Cameron Lim",
          "Mark Coffer",
          "Yueqi Peng",
          "Grace Alberts"
        )
        if(weight == 0) {
          val randomName = x(random.nextInt(x.length))
          //change Customer name to randomName
        }
      }

      val location = randomCityCountry(spark)
      p += (
        "city" -> location._1,
        "country" -> location._2
        )
      println(p)


      // FIXME the code currently coded to only loop once
      keepLooping = false
    }
  }
}
