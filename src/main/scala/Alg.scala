import Trends._
import Test._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.util.{Calendar, InputMismatchException, Scanner}
import scala.collection.mutable
import scala.math.BigDecimal._
import scala.util.Random
import scala.util.Random._
import scala.io.Source
import java.io.IOException
import java.util.InputMismatchException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.Locale.Category
import Trends._
import org.apache.spark.storage.StorageLevel
import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime, LocalTime}
//Futures and Threading
import scala.concurrent._
import scala.util.{Failure, Success}
import scala.concurrent.duration.Duration
import ExecutionContext.Implicits.global

object Alg {
  // Checks if an int is already a customer id, returns an array
  // returnedArray(0) == customer id
  // returnedArray(1) == customer name
  def cusRecord(
      n: Int,
      isEnemyName: Boolean
  ): (String, String, String, String) = {
    if (isEnemyName) {
      val x: Map[String, String] = Map(
        "1001" -> "Yash Dhayal",
        "1002" -> "Hyung Ro Yoon",
        "1003" -> "Betty Chou",
        "1004" -> "Bryan Boyett",
        "1005" -> "Mandeep Atwal",
        "1006" -> "Jacob Nottingham",
        "1007" -> "Brandon Conover",
        "1008" -> "Cameron Lim",
        "1009" -> "Mark Coffer",
        "1010" -> "Yueqi Peng",
        "1011" -> "Grace Alberts"
      )
      val r = (nextInt(11) + 1001).toString
      (r, x(r), "dumbville", "USA")
    }
    try {
      var name = ""
      var exists = false
      var city = ""
      var country = ""
      val f = new File("input/customers.txt")
      val fc = Source.fromFile(f).getLines()

      while (fc.hasNext && !exists) { // Attempt to find the id in record, if found get name
        val line = fc.next()
        val s = line.split(',')
        if (s(0) == n.toString) {
          exists = true
          name = s(1)
          city = s(2)
          country = s(3)
        }
      }
      if (!exists) { // Not in the record already? Then put it in there!
        val cc = cityCountryGen(spark)
        val pw = new PrintWriter(new FileOutputStream(f, true))
        name = cusNameGen
        city = cc._1
        country = cc._2
        pw.append(s"$n,$name,${cc._1},${cc._2}\n")
        pw.close
      }
      (n.toString, name, city, country)
    } catch {
      case e: InputMismatchException =>
        println(s"Improper Input Exception:$e")
        ("ERROR", "ERROR", "ERROR", "ERROR")
    }
  }

  //randomly generates a customer name
  //used in cusRecord()
  def cusNameGen(): String = {
    val namer = fabricator.Contact()
    namer.fullName(false, false)
  }

  // Checks if an int is already a product id, returns an array
  // returnedArray(0) == product id
  // returnedArray(1) == product name
  // TODO Does not account for when the generator finds a product that is actually matched to an ID already
  def proRecord(
      n: Int,
      genPrice: Double,
      host: String,
      fitStatus: String,
      time: LocalDateTime,
      cusName: String
  ): (String, String, String, String, String) = {
    // Pillow trend implementation
    if (pillow(cusName)) {
      return ("0", "pillow", "pillow", "50.50", "cushn.com")
    }
    // try {
    val f = new File("input/products.txt")
    //f.createNewFile
    // val sc = new Scanner(f)
    var name = ""
    var pcat = ""
    var price = 0.0
    var url = ""
    var exists = false

    val fc = Source.fromFile(f).getLines()
    while (fc.hasNext && !exists) {
      val s = fc.next().split(",")
      if (s(0) == n.toString()) {
        name = s(1)
        pcat = s(2)
        price = s(3).toDouble
        url = s(4)
        exists = true
      }
    }
    if (!exists) { // Not in the record already? Then put it in there!
      val pw = new PrintWriter(new FileOutputStream(f, true))
      //name = proNameGen()
      val maxPrice = genPrice
      val (l, discount) = lastWeekDecrease(
        time
      ) //call to function for decreasing price last week of month
      host match {
        case "amazon.com" =>
          if (fitStatus == "true") {
            val a = dfA
              .where(col("Category").like("%Fitness%"))
              .where(col("SellingPrice") <= maxPrice)
              .orderBy(desc("SellingPrice"))
              .first
            name = a.getString(0)
            pcat = a.getString(1)
            price = a.getDouble(2)
            url = host
          } else {
            val a = dfA
              .where(col("SellingPrice") <= maxPrice)
              .orderBy(desc("SellingPrice"))
              .first
            name = a.getString(0)
            pcat = a.getString(1)
            price = a.getDouble(2)
            url = host
          }
        //highest is about 1000
        case "walmart.com" =>
          if (fitStatus == "true") {
            val w = dfW
              .where(col("Category").like("%Fitness%"))
              .where(col("SalePrice") <= maxPrice)
              .orderBy(desc("SalePrice"))
              .first
            name = w.getString(0)
            pcat = w.getString(1)
            price = w.getDouble(2)
            url = host
          } else {
            val w = dfW
              .where(col("SalePrice") < maxPrice)
              .orderBy(desc("SalePrice"))
              .first
            name = w.getString(0)
            pcat = w.getString(1)
            price = w.getDouble(2)
            url = host
          }
        case "ebay.com" =>
          val e = dfE
            .where(col("Price") <= maxPrice)
            .orderBy(desc("Price"))
            .first
          name = e.getString(0)
          price = e.getDouble(1)
          pcat = "n/a"
          url = host
        //highest is about 1000
      }
      //If it is the last week of the month, decrease the price
      if (l) {

        price = price * (1.0 - discount)
      }
      pw.append(s"$n,$name,$pcat,$price,$url\n")
      pw.close
    }
    (n.toString, name, pcat, price.toString, url)
    // } catch {
    // case e: Throwable =>
    // println(s"Exception with DF or PrintWriter!:\n$e")
    // ("ERROR", "ERROR", "ERROR", "ERROR", "ERROR")
    // }
  }

  //randomly picks a category from an indexed sequence
  //used in proRecord()
  def proCategoryGen(): String = {
    val random = new Random
    val x = IndexedSeq(
      "Appliances",
      "Automotive Parts & Accessories",
      "Arts & Crafts",
      "Beauty & Personal Care",
      "Books",
      "Books",
      "Books",
      "Books",
      "Electronics",
      "Electronics",
      "Garden & Outdoor",
      "Grocery & Food",
      "Health",
      "Home & Kitchen",
      "Movies & TV",
      "Movies & TV",
      "Movies & TV",
      "Toys & Games",
      "Toys & Games"
    )
    val randomCategory = x(random.nextInt(x.length))
    randomCategory
  }

  //randomly picks a payment type from an array
  def payTypeGen(): String = {
    val c = Array("card", "IB", "UPI", "Wallet")
    c(nextInt(c.length))
  }

  //generates a timestamp
  def timestampGen: String = {
    val now = Calendar.getInstance().getTime()
    val newDate = now.toString
    newDate
  }

  def priceGen(): (Double, Double, Int) = {
    //creates value for weight
    val weight = nextInt(10) + 1
    //whole number
    var unitPrice = 0.0
    if (weight > 9) { //10% of possible outcomes
      //price is anywhere from 0 to 999
      unitPrice = (nextInt(100000).toDouble / 100)
    } else { //90% of possible outcomes
      //price is anywhere from 0 to 199
      unitPrice = (nextInt(20000).toDouble / 100)
    }
    var totalPrice = 0.0 //price of transaction
    var qty = 0
    if (unitPrice > 199) { // 8% of possible outcomes
      qty = nextInt(5) + 1
    } else { // 92% of possible outcomes
      //since most purchases are going to be in smaller quantities,
      //this ensures that smaller amounts will happen more frequently.
      val weightQty = nextInt(10)
      if (weightQty > 7) { //20% of possible outcomes
        qty = nextInt(50) + 1
      } else { //80% of possible outcomes
        qty = nextInt(5) + 1
      }
    }
    totalPrice = unitPrice * qty
    (f"$totalPrice%1.2f".toDouble, f"$unitPrice%1.2f".toDouble, qty)
  }

  def readFile(filename: String): String = {
    val bufferedSource = scala.io.Source.fromFile(filename)
    val countries = (for (line <- bufferedSource.getLines()) yield line).toList
    bufferedSource.close
    val random = new Random
    var randomCountry = countries(
      random.nextInt(countries.length)
    )
    randomCountry
  }

  def payIdGen(): String = {
    val id = nextInt(100000)
    f"$id%05.0f"
  }

  def urlGen(host: String, name: String): String = {
    val g = fabricator.Internet()
    g.urlBuilder
      .scheme("https")
      .host(host)
      .path("/getNewId")
      .params(
        mutable.Map[String, Any](
          "id" -> nextInt(101),
          "name" -> name,
          "coordinates" -> nextDouble()
        )
      )
      .toString()
    // https://google.com/getNewId?id=100&name=John+Lennon&coordinates=30.03
  }

  // Limited to those sites that we have data for
  // TODO Expand and group as keys mapped to product file
  def hostNameGen(): String = {
    val random = new Random
    val x = IndexedSeq(
      "Amazon.com",
      "Amazon.com",
      "Amazon.com",
      "Walmart.com",
      "Walmart.com",
      "eBay.com"
      // "Target.com",
      // "Alibaba.com",
      // "Wish.com",
      // "Etsy.com",
      // "AliExpress.com",
      // "BestBuy.com",
      // "Microcenter.com",
      // "Newegg.com",
      // "Google.com",
      // "Intel.com",
      // "Amd.com"
    )

    val randomHost = x(random.nextInt(x.length))
    randomHost.toLowerCase
  }

  //randomly chooses a reason why a payment would have failed from an indexed seq
  def payStatusGen(): (String, String) = {
    val random = new Random
    val x = List(
      "Expired Card",
      "Invalid CVC",
      "Invalid Pin",
      "Expired Card",
      "Lost/Stolen Card",
      "Invalid CVC",
      "Withdrawal Exceeded Allowed Amount",
      "Invalid Pin",
      "Declined by Issuer",
      "Expired Card",
      "Invalid Postal Code",
      "Invalid CVC",
      "Card Not Supported",
      "Currency Not Supported",
      "Invalid Pin",
      "Expired Card",
      "Fraud Alert",
      "Purchase Restriction",
      "Expired Card",
      "Invalid Pin"
    )

    //randomly selects a status for payment success rate
    var status = ""
    val r = nextInt(10)
    status = if (r > 0) "Y" else "N"

    if (status == "N") {
      val randomFail = x(random.nextInt(x.length))

      (status, randomFail)
    } else {
      val randomSuccess = "No failure."
      (status, randomSuccess)
    }

  }

  def cityCountryGen(spark: SparkSession): (String, String) = {
    try {
      var df = spark.read
        .format("csv")
        .option("header", "true")
        .load("input/citiesCountries.csv")
      //df.show(5)
      val r = new Random()
      val id = r.nextInt(41001)
      val rand = spenderCities()
      if (rand == "Other") {
        df = df.select("city", "country").where(s"id = $id").limit(1).toDF()
        (df.first.getString(0), df.first.getString(1))
      } else {
        (rand, df.select("country").where(s"city = '$rand'").first.getString(0))
      }

    } catch {
      case e: InputMismatchException =>
        println("Improper Input Exception")
        ("ERROR", "ERROR")
    }
  }
}
