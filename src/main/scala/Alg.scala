import org.apache.spark.sql.SparkSession

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.{Calendar, Scanner}
import scala.collection.mutable
import scala.math.BigDecimal._
import scala.util.Random
import scala.util.Random._
import java.io.IOException
import java.util.InputMismatchException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.Locale.Category
import Trends._
import org.apache.spark.storage.StorageLevel

object Alg {
  // Checks if an int is already a customer id, returns an array
  // returnedArray(0) == customer id
  // returnedArray(1) == customer name
  def cusRecord(n: Int, isEnemyName: Boolean): (String, String) = {
    val t4 = System.nanoTime
    if (isEnemyName) {
      val x: Map[String, String] = Map(
        "1001" -> "Yash Dhayal",
        "1002" -> "Hyung Ro Yoon",
        "1003" -> "Betty Boyett",
        "1004" -> "Bryan Chou",
        "1005" -> "Mandeep Atwal",
        "1006" -> "Jacob Nottingham",
        "1007" -> "Brandon Conover",
        "1008" -> "Cameron Lim",
        "1009" -> "Mark Coffer",
        "1010" -> "Yueqi Peng",
        "1011" -> "Grace Alberts"
      )
      x getOrElse ((nextInt(1012) + 1001).toString, ("ERROR", "ERROR"))
    }
    try {
      val f = new File("input/customers.txt")
      //f.createNewFile
      val sc = new Scanner(f)
      var id = ""
      var name = ""
      var exists = false
      while (sc.hasNext && !exists) { // Attempt to find the id in record, if found get name
        val s = sc.next.split(',')
        id = s(0)
        if (id == n.toString) {
          exists = true
          name = s(1)
        }
      }
      if (!exists) { // Not in the record already? Then put it in there!
        val pw = new PrintWriter(new FileOutputStream(f, true))
        name = cusNameGen
        id = n.toString
        pw.append(s"$n,$name\n")
        pw.close
      }
      val dur4 = (System.nanoTime - t4) / 1e9d
      println()
      println("The execution time of the customer function is: " + dur4 + " seconds.")

      (id, name)
    } catch {
      case e: InputMismatchException =>
        println(s"Improper Input Exception:$e")
        ("ERROR", "ERROR")
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
      spark: SparkSession
  ): (String, String, String, String, String) = {
    val t5 = System.nanoTime
    try {
      val f = new File("input/products.txt")
      //f.createNewFile
      val sc = new Scanner(f)
      var name = ""
      var pcat = ""
      var price = 0.0
      var url = ""
      var exists = false
      while (sc.hasNext && !exists) { // Attempt to find the id in record, if found get name
        val s = sc.next.split(',')
        if (s(0) == n.toString) {
          exists = true
          name = s(1)
          pcat = s(2)
          price = s(3).toDouble
          url = s(4)
        }
      }
      if (!exists) { // Not in the record already? Then put it in there!
        val pw = new PrintWriter(new FileOutputStream(f, true))
        //name = proNameGen()
        var maxPrice = genPrice
        host match {
          case "amazon.com" =>
            val df = spark.read
              .parquet("input/pq/amazon.parquet")
              .withColumn(
                "SellingPrice",
                col("SellingPrice").cast(DoubleType)
              )
              .select("ProductName", "Category", "SellingPrice", "ProductUrl")
              .where(col("SellingPrice") <= maxPrice)
              .orderBy(desc("SellingPrice"))
              .first
            name = df.getString(0)
            pcat = df.getString(1)
            price = df.getDouble(2)
            url = df.getString(3)
          //highest is about 1000
          case "walmart.com" =>
            val df = spark.read
              .parquet("input/pq/walmart.parquet")
              .withColumn(
                "SalePrice",
                col("SalePrice").cast(DoubleType)
              )
              .select("ProductName", "Category", "SalePrice", "ProductUrl")
              .where(col("SalePrice") < maxPrice)
              .orderBy(desc("SalePrice"))
              .first
            name = df.getString(0)
            pcat = df.getString(1)
            price = df.getDouble(2)
            url = df.getString(3)
          case "ebay.com" =>
            val df = spark.read
              .parquet("input/pq/ebay.parquet")
              .withColumn("Price", col("Price").cast(DoubleType))
              .select("Title", "Price", "Pageurl")
              .where(col("Price") < maxPrice)
              .orderBy(desc("Price"))
              .first
            name = df.getString(0)
            price = df.getDouble(1)
            pcat = "n/a"
            url = df.getString(3)
          //highest is about 1000
        }
        pw.append(s"$n,$name,$pcat,$price,$url\n")
        pw.close
      }
      val dur5 = (System.nanoTime - t5) / 1e9d
      println()
      println("The execution time of the product function is: " + dur5 + " seconds.")
      (n.toString, name, pcat, price.toString, url)
    } catch {
      case e: Throwable =>
        println(s"Exception!:\n$e")
        ("ERROR", "ERROR", "ERROR", "ERROR", "ERROR")
    }
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
      "Electronics",
      "Garden & Outdoor",
      "Grocery & Food",
      "Health",
      "Home & Kitchen",
      "Movies & TV",
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
    val t2 = System.nanoTime
    //creates value for weight
    val weight = nextInt(10) + 1
    //whole number
    var whole = 0
    if (weight > 9) { //10% of possible outcomes
      //price is anywhere from 0 to 999
      whole = nextInt(1000)
    } else { //90% of possible outcomes
      //price is anywhere from 0 to 199
      whole = nextInt(200)
    }
    //creates random Float
    val dec = nextFloat()
    //unitPrice = whole number + decimal number
    var unitPrice = whole + dec.toDouble
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
    totalPrice = BigDecimal(unitPrice * qty)
      .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      .toDouble
    unitPrice = BigDecimal(unitPrice)
      .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      .toDouble
    val dur = (System.nanoTime - t2) / 1e9d
    println()
    println("The execution time of the price function is: " + dur + " seconds.")
    (totalPrice, unitPrice, qty)
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
    val t3 = System.nanoTime
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

    val dur3 = (System.nanoTime - t3) / 1e9d
    println()
    println("The execution time of the payment function is: " + dur3 + " seconds.")

    if (status == "N") {
      val randomFail = x(random.nextInt(x.length))

      (status, randomFail)
    } else {
      val randomSuccess = "No failure."
      (status, randomSuccess)
    }

  }

  def cityCountryGen(spark: SparkSession): (String, String) = {
    val t6 = System.nanoTime
    try {
      var df = spark.read
        .format("csv")
        .option("header", "true")
        .load("input/citiesCountries.csv")
      //df.show(5)
      val r = new Random()
      val id = r.nextInt(41001)
      val rand = spenderCities()
      val dur6 = (System.nanoTime - t6) / 1e9d
      println()
      println("The execution time of the country function is: " + dur6 + " seconds.")
      if(rand == "Other") {
        df = df.select("city", "country").where(s"id = $id").limit(1).toDF()
        println("Your city is " + df.first.getString(0))
        println("Your country is " + df.first.getString(1))
        (df.first.getString(0), df.first.getString(1))
      } else {
        val dftrend = df.select( "country").where(s"city = '$rand'").limit(1).toDF()
        (rand, dftrend.first.getString(0))
      }

    } catch {
      case e: InputMismatchException =>
        println("Improper Input Exception")
        ("ERROR", "ERROR")
    }
  }
}
