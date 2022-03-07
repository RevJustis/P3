import Alg._
import scala.util.Random._
import scala.collection.mutable

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

object Trends {
  def megaTrendLogic(record: mutable.Map[String, String]): String = {
    if (record("customer_name").length >= 8) "pillowT" else "placeholder"
  }
  def isEnemyName(pay: String): Boolean = {
    if (pay == "N") {
      val weight = nextInt(10)
      if (nextInt(10) == 0) true
    }
    false
  }
}
