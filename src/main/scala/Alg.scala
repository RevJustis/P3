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

  def cusNameGen(): String = {
    val namer = fabricator.Contact()
    namer.fullName(false, false)
  }

  def proNameGen(): String = {
    val namer = fabricator.Words()
    namer.word
  }

  def payTypeGen(): String = {
    val c = Array("card", "IB", "UPI", "Wallet")
    c(nextInt(c.length))
  }

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
    if (weight > 8) { //20% of possible outcomes
      //price is anywhere from 0 to 9999
      whole = nextInt(10000)
    } else { //80% of possible outcomes
      //price is anywhere from 0 to 999
      whole = nextInt(1000)
    }
    //creates random Float
    val dec = nextFloat()
    //price = whole number + decimal number rounded to 2 places
    val price = whole + BigDecimal(dec).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
    var unitPrice = 0.0 //price per unit sold
    var qty = 0
    if (price > 999) { // 18% of possible outcomes
      qty = nextInt(5) + 1
      unitPrice = BigDecimal(price/qty).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
    } else { // 82% of possible outcomes
      qty = nextInt(50) + 1
      unitPrice = BigDecimal(price/qty).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
    }
    return (price, unitPrice, qty)
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

  def urlGen(): String = {
    val g = fabricator.Internet()
    g.urlBuilder
      .scheme("https")
      .host(hostGen())
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

  /*def hostGen(): String = {
    val r = nextInt(10)
    if (r % 2 == 0) "amazon.com" else "alibaba.com"
  }*/

  def hostGen(): String = {
    val random = new Random
    val x = IndexedSeq(
      "Amazon.com",
      "Alibaba.com",
      "Walmart.com",
      "Target.com",
      "eBay.com",
      "Wish.com",
      "Etsy.com",
      "AliExpress.com",
      "BestBuy.com",
      "Microcenter.com",
      "Newegg.com",
      "Google.com",
      "Intel.com",
      "Amd.com"
    )

    val randomHost = x(random.nextInt(x.length))
    return randomHost
  }


  def paySuccessGen(): Char = {
    val r = nextInt(10)
    if (r % 2 == 0) 'Y' else 'N'
  }

  def payfailgen [A] (x: IndexedSeq[A], value: A): Unit = {

    val x = IndexedSeq(
      "Expired Card",
      "Invalid CVC",
      "Invalid Pin",
      "Lost/Stolen Card",
      "Withdrawal Exceeded Allowed Amount",
      "Declined by Issuer",
      "Invalid Postal Code",
      "Card Not Supported",
      "Currency Not Supported",
      "Fraud Alert",
      "Purchase Restriction")

      val randomFail = x(Random.nextInt(x.length))

      return randomFail


  }

}

