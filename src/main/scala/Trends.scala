import Alg._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

import scala.collection.mutable
import scala.util.Random._

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
    var rand = nextInt(10)
    if(rand == 0) {
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

  def fitness(): Unit = {


  }
}


def payFailTime (time:String, status: String): Boolean = {

  if (time <= "00:01:00" && time >= "00:04:00") {

    val w = nextInt(10)
    if (w<=4) {
      status == "N"
    }
  } else {
    "Y"
  }
  false
}
