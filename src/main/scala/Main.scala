import Alg._
import scala.util.Random._
import scala.collection.mutable

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

object Main {
  def main(args: Array[String]): Unit = {
//<<<<<<< Updated upstream
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
      // "country" -> location._1
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
      // val product = proRecord(price._1, host)
      // produced += (
      // "proName" -> product._1,
      // "proType" -> product._2,
      // "proID" -> product._3
      // )
      // Payment info generation
      val pay = payStatusGen
      p += (
        "payment_type" -> payTypeGen,
        "payment_txn_id" -> payIdGen,
        "payment_txn_success" -> pay._1,
        "failure_reason" -> pay._2
      )
      // FIXME the code currently coded to only loop once
      keepLooping = false
    }
//=======
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      //.enableHiveSupport()
      .getOrCreate()
    var p = Map[String, String]()
    println(id)
    val hostName = hostNameGen()
    val(a,b,c) = priceGen()
    println(proRecord(1, b, hostName, spark))
    println(p)
    // Order ID and timestamp generation
    // p = p + ("order_id" -> orderId, "dateTime" -> timestampGen)
    // Customer info gen
    /*val customer = cusRecord(nextInt(1000))
    p = p + (
      "customer_id" -> customer._1,
      "customer_name" -> customer._2
    )*/
    // val location = locGen
    // produced += (
    // "country" -> location._1
    // "city" -> location._2,
    // )
    // Price, unit price and quantity gen
    val price = priceGen() // Total, Unit, qty
    p = p + ("totPrice" -> price._1.toString())
    p = p + ("unitPrice" -> price._2.toString())
    p = p + ("quantity" -> price._3.toString())
    // create url and store
    val host = hostNameGen()
    p = p + ("url" -> urlGen(host))
    // Product info generation
    // val product = proRecord(price._1, host)
    // produced += (
    // "proName" -> product._1,
    // "proType" -> product._2,
    // "proID" -> product._3
    // )
    // Payment info generation
    val pay = payStatusGen
    p = p + (
      "payment_type" -> payTypeGen,
      "payment_txn_id" -> payIdGen,
      "payment_txn_success" -> pay._1,
      "failure_reason" -> pay._2
    )
//>>>>>>> Stashed changes
  }
}
