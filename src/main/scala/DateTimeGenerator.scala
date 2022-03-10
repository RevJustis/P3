import java.time.LocalDateTime
import Trends._

import scala.util.Random.nextInt

object DateTimeGenerator {

  // Code for the "Bad" timestamp string, generates a random Month
  def monthGen: String = {

    val r = scala.util.Random
    val upper = 13
    val lower = 1
    val monthNum = r.nextInt(upper - lower) + 1
    var month = ""

    monthNum match {
      case 1 => month = "Jan"
      case 2 => month = "Feb"
      case 3 => month = "Mar"
      case 4 => month = "Apr"
      case 5 => month = "May"
      case 6 => month = "Jun"
      case 7 => month = "Jul"
      case 8 => month = "Aug"
      case 9 => month = "Sep"
      case 10 => month = "Oct"
      case 11 => month = "Nov"
      case 12 => month = "Dec"
    }
    month
  }

  // Code for the "Bad" timestamp string, generates a random day based on the month
  def dayGen(month: Int): Int = {

    val r = scala.util.Random
    val lower = 1
    var x = 0

    month match {
      case 1 => x = 31
      case 2 => x = 28
      case 3 => x = 31
      case 4 => x = 30
      case 5 => x = 31
      case 6 => x = 30
      case 7 => x = 31
      case 8 => x = 31
      case 9 => x = 30
      case 10 => x = 31
      case 11 => x = 30
      case 12 => x = 31
    }

    val dayNum = r.nextInt(x-lower) + 1
    //dayNum.toString
    dayNum
  }

  // Code to generate a "Bad" timestamp for part of our bad data being sent to the other team
  def timeStampGen: String = {
    val r = scala.util.Random
    val hour = r.nextInt(24)
    val min = r.nextInt(60)
    val second = r.nextInt(60)

    var finalMin = ""
    var finalHour = ""
    var finalSecond = ""

    if(hour < 10) {
      finalHour = "0" + hour.toString
    }
    else{
      finalHour = hour.toString
    }

    if(min < 10) {
      finalMin = "0" + min.toString
    }
    else{
      finalMin = min.toString
    }

    if(second < 10) {
      finalSecond = "0" + second.toString
    }
    else{
      finalSecond = second.toString
    }

    finalHour + ":" + finalMin + ":" + finalSecond
  }

  // Adds a random timezone to the "Bad" timestamp string
  def timezoneGenerator: String = {
    val timezones = Array[String]("PST", "GMT", "CT", "EST", "UTC")
    val r = scala.util.Random
    val x = r.nextInt(5)
    var timezone = ""

    timezone = timezones(x)

    timezone
  }

  // Adds a random day to the "Bad" timestamp string, may not match the actual day of the month
  def createRandomDay: String = {
    val upper = 8
    val lower = 1
    val r = scala.util.Random
    val x = r.nextInt(upper - lower) + 1
    var day = ""

    x match {
      case 1 => day = "Mon"
      case 2 => day = "Tue"
      case 3 => day = "Wed"
      case 4 => day = "Thu"
      case 5 => day = "Fri"
      case 6 => day = "Sat"
      case 7 => day = "Sun"
    }
    day
  }

  // Method to combine parts of the "Bad" timestamp string together to be sent via the producer
  def createBadTimestamp: String = {
    val month = monthGen
    //val day = dayGen(month)
    val day = 0
    val ranDay = createRandomDay
    val time = timeStampGen
    val timezone = timezoneGenerator
    val year = "2021"

    ranDay + " " + month + " " + day + " " + time + " " + timezone + " " + year
  }

  // Method to create a "Good" LocalDateTime object that can be easily broken down and used in queries
  def createDateTime: LocalDateTime = {

    val monthStart = 1
    val monthEnd = 13
    val r = scala.util.Random
    var month = r.nextInt(monthEnd-monthStart) + 1
    val year = 2021
    var day = dayGen(month)
    val hour = hourGen
    val min = minGen
    val sec = secondGen
    println(month)

    /*
    month match {
      case 1 => day = 31
      case 2 => day = 28
      case 3 => day = 31
      case 4 => day = 30
      case 5 => day = 31
      case 6 => day = 30
      case 7 => day = 31
      case 8 => day = 31
      case 9 => day = 30
      case 10 => day = 31
      case 11 => day = 30
      case 12 => day = 31
    }
     */
    val holiday = holidayIncrease()
    if(holiday == true) {
        val h = IndexedSeq(
          11,
          12,
          1
        )
       month = h(nextInt(h.length))

      month match {
        case 11 => day = 30-r.nextInt(5)
        case 12 => day = 24-r.nextInt(23)
        case 1 => day = 3-r.nextInt(2)
      }
    }

    // Uses parameters to combine together into a Java LocalDateTime object
    // Very easy to work with for our queries and trend creation
    val newDate = LocalDateTime.of(year, month, day, hour, min, sec)

    newDate

  }

  // TODO Example code showing how to split a LocalDateTime object into easy to use variables
  def dateTimeSplitter(dateTime : LocalDateTime) : Unit = {

    val year = dateTime.getYear
    val month = dateTime.getMonthValue
    val day = dateTime.getDayOfMonth
    val hour = dateTime.getHour
    val min = dateTime.getMinute

    println(year)
    println(month)
    println(day)
    println(hour)
    println(min)
  }

  // Generates a random hour for the "Good" timestamp
  def hourGen: Int = {
    val r = scala.util.Random
    val hour = r.nextInt(24)
    hour
  }

  // Generates a random minute for the "Good" timestamp
  def minGen: Int = {
    val r = scala.util.Random
    val hour = r.nextInt(60)
    hour
  }

  // Generates a random second for the "Good" timestamp
  def secondGen: Int = {
    val r = scala.util.Random
    val hour = r.nextInt(60)
    hour
  }

  // Method that weights which type of timestamp, "Good" or "Bad" is sent via the producer,
  // Heavily favors the "Good" timestamp
  def timestampProducer: String = {
    val r = scala.util.Random
    val weight = r.nextInt(50)
    var localDateTime = ""

    if(weight == 0) {
      localDateTime = createBadTimestamp
    }
    else {
      localDateTime = createDateTime.toString
    }

    localDateTime
  }




}
