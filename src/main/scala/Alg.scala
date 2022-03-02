import java.util.Calendar
import scala.collection.mutable
import scala.math.BigDecimal._
import scala.util.Random
import scala.util.Random._
import java.io.{File, FileOutputStream, PrintWriter}
import java.util.Scanner

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

  def priceGen(): Double = {
    //creates random Int
    val whole = nextInt(5000)
    //creates random Float
    val dec = nextFloat()
    val price = whole + dec.setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
    return price
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
          "name" -> nameGen(),
          "coordinates" -> 30.03
        )
      )
      .toString()
    // https://google.com/getNewId?id=100&name=John+Lennon&coordinates=30.03
  }

  def hostGen(): String = {
    val r = nextInt(10)
    if (r % 2 == 0) "amazon.com" else "alibaba.com"
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
      "Purchase Restriction"
    )


  }

}

