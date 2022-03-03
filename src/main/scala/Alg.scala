import org.apache.spark.sql.SparkSession

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.{Calendar, Scanner}
import scala.collection.mutable
import scala.math.BigDecimal._
import scala.util.Random
import scala.util.Random._
import java.io.IOException
import java.util.InputMismatchException

object Alg {
  // Checks if an int is already a customer id, returns an array
  // returnedArray(0) == customer id
  // returnedArray(1) == customer name
  def cusRecord(n: Int): (String, String) = {
    try {
      val f = new File("input/customers.txt")
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
      (id, name)
    } catch {
      case e: InputMismatchException => println("Improper Input Exception")
        ("Tuple", "Tuple")
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
  // TODO Overhaul this so that it just uses the name given
  // by the proNameGen()
  def proRecord(
      n: Int,
      unitPrice: Double,
      host: String,
      spark: SparkSession
  ): (String, String, String) = {
    try {
      val f = new File("input/products.txt")
      val sc = new Scanner(f)
      var pid = ""
      var name = ""
      var pcat = ""
      var price = 0.0
      var exists = false
      while (sc.hasNext && !exists) { // Attempt to find the id in record, if found get name
        val s = sc.next.split(',')
        pid = s(0)
        if (pid == n.toString) {
          exists = true
          name = s(1)
        }
      }
      if (!exists) { // Not in the record already? Then put it in there!
        val pw = new PrintWriter(new FileOutputStream(f, true))
        //name = proNameGen()
        val h = host
        price = unitPrice
        val maxPrice = price * 1.1
        val minPrice = price * 0.9
        h match {
          case "Amazon.com" =>
            val dfAmazon = spark.read.format("csv").option("header","true").load("input/amazon.csv")
            dfAmazon.select("Product Name").where(dfAmazon("Selling Price").between(minPrice,maxPrice)).first.getString(0)
          case "Walmart.com" =>
            val dfWalmart = spark.read.format("csv").option("header","true").load("input/walmartC.csv")
            dfWalmart.select("Product Name").where(dfWalmart("Sale Price").between(minPrice,maxPrice)).first.getString(0)
          case "eBay.com" =>
            val dfEbay = spark.read.format("csv").option("header","true").load("input/ebay.csv")
            dfEbay.select("Title").where(dfEbay("Price").between(minPrice,maxPrice)).first.getString(0)
        }
        pid = n.toString
        pcat = s"($proCategoryGen)"

        pw.append(s"$n,$name\n")
        pw.close
      }
      (pid, name, pcat)
    } catch {
      case e: Throwable => println("Improper Input Exception")
        ("Tuple", "Tuple", "Tuple")
    }
  }

  //creates a random product name
  //used in proRecord()
  // TODO Don't random gen, instead find a product name inside
  // one of the files, based on which url is chosen.
  /*
  def proNameGen(host: String): String = {
    //val namer = fabricator.Words()
    //namer.word
    val h = host
    h match {
      case "Amazon.com"
    }

  }

   */

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
    //creates value for weight
    val weight = nextInt(10) + 1
    //whole number
    var whole = 0
    if (weight > 9) { //10% of possible outcomes
      //price is anywhere from 0 to 9999
      whole = nextInt(10000)
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
    if (unitPrice > 199) { // 9.8% of possible outcomes
      qty = nextInt(5) + 1
    } else { // 90.2% of possible outcomes
      //since most purchases are going to be in smaller quantities,
      //this ensures that smaller amounts will happen more frequently.
      val weightQty = nextInt(10)
      if (weightQty > 7) { //80% of possible outcomes
        qty = nextInt(50) + 1
      } else { //20% of possible outcomes
        qty = nextInt(5) + 1
      }
    }
    totalPrice = BigDecimal(unitPrice * qty)
      .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      .toDouble
    unitPrice = BigDecimal(unitPrice)
      .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      .toDouble
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

  def urlGen(h: String): String = {
    val g = fabricator.Internet()
    g.urlBuilder
      .scheme("https")
      .host(h)
      .path("/getNewId")
      .params(
        mutable.Map[String, Any](
          "id" -> nextInt(101),
          "name" -> cusNameGen(),
          "coordinates" -> 30.03
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

  /*
  //randomly selects a status for payment success rate
  def paySuccessGen(status: String): Char = {
    //val r = nextInt(10)
    //if (r % 2 == 0) 'Y' else 'N'
  }
   */

  //randomly chooses a reason why a payment would have failed from an indexed seq
  def payStatusGen(): (String, String) = {
    val random = new Random
    val x = IndexedSeq(
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
    if (r % 2 == 0) status = "Y" else status = "N"
    //println("Was payment successful?: " + paySuccessGen(status))
    //print("Why did the payment fail? ")

    if (status == "N") {
      val randomFail = x(random.nextInt(x.length))

      (status, randomFail)
    } else {
      val randomSuccess = "No failure."
      (status, randomSuccess)
    }
  }

  def randomCityCountry(spark: SparkSession): (Any, Any) = {
    try {
      var df = spark.read
        .format("csv")
        .option("header", "true")
        .load("input/citiesCountries.csv")
      df.show(5)
      val r = new Random()
      val id = r.nextInt(41001)
      df = df.select("city", "country").where(s"id = $id").limit(1).toDF()
      println("Your city is " + df.first.getString(0))
      println("Your country is " + df.first.getString(1))
      (df.first.getString(0), df.first.getString(1))
    } catch {
      case e: InputMismatchException => println("Improper Input Exception")
        ("Tuple", "Tuple")
    }
  }
}
