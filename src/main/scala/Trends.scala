import Alg._

import scala.util.Random._
import scala.collection.mutable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

import scala.util.Random

object Trends {
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
    if(rand == 0) {
      val random = new Random
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
      x(random.nextInt(x.length))
    } else "Other"
  }

  def fitness(): Unit = {


  }
}
