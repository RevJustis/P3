import org.apache.spark.sql.SparkSession

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.{Calendar, Scanner}
import scala.collection.mutable
import scala.math.BigDecimal._
import scala.util.Random
import scala.util.Random._

object Alg {
  // Checks if an int is already a customer id, returns an array
  // returnedArray(0) == customer id
  // returnedArray(1) == customer name
  def cusRecord(n: Int): Array[String] = {
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
    Array(id, name)
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
  def proRecord(n: Int): Array[String] = {
    val f = new File("input/products.txt")
    val sc = new Scanner(f)
    var pid = ""
    var name = ""
    var pcat = ""
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
      name = proNameGen()
      pid = n.toString
      pcat = s"($proCategoryGen)"
      pw.append(s"$n,$name\n")
      pw.close
    }
    Array(pid, name, pcat)
  }

  //creates a random product name
  //used in proRecord()
  // TODO Don't random gen, instead find a product name inside
  // one of the files, based on which url is chosen.
  def proNameGen(): String = {
    val namer = fabricator.Words()
    namer.word
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

    return randomCategory
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
      //price is anywhere from 0 to 999
      whole = nextInt(500)
    }
    //creates random Float
    val dec = nextFloat()
    //unitPrice = whole number + decimal number
    var unitPrice = whole + dec.toDouble
    var totalPrice = 0.0 //price of transaction
    var qty = 0
    if (unitPrice > 499) { // 9.5% of possible outcomes
      qty = nextInt(5) + 1
      totalPrice = BigDecimal(unitPrice * qty)
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
        .toDouble
    } else { // 90.5% of possible outcomes
      qty = nextInt(50) + 1
      totalPrice = BigDecimal(unitPrice * qty)
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
        .toDouble
    }
    unitPrice = BigDecimal(unitPrice)
      .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      .toDouble
    return (totalPrice, unitPrice, qty)

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

  def payIdGen(): Int = {
    nextInt(90000) + 10000
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
  def urlGenHelper(): String = {
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

      return (status, randomFail)
    } else {
      val randomSuccess = "No failure."
      return (status, randomSuccess)
    }

  }

  def randomCityCountry(sparkSession: SparkSession): (Any, Any) = {
    val df1 = spark.read
      .format("csv")
      .option("header", "true")
      .load("input/citiesCountries.csv")
    df1.show(5)
    val r = new Random()
    val id = r.nextInt(41001)
    val dfCity = df1.select("city").where(s"id = $id").first()
    println("Your city is " + dfCity(0))
    val dfCountry = df1.select("country").where(s"id = $id").first()
    println("Your country is " + dfCountry(0))
    val CityString = dfCity(0)
    val CountryString = dfCountry(0)
    return (CityString, CountryString)
  }

}
