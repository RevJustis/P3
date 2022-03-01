import scala.util.Random._
import scala.util.Random
import scala.math.BigDecimal._
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.io.Source
import scala.collection.mutable

object Alg {
  def yourFunctions(): Unit = {}

  def nameGen(): String = {
    val namer = fabricator.Contact()
    namer.fullName(false, false)
  }

  def timestampGen: String = {
    val now = Calendar.getInstance().getTime()
    val newDate = now.toString

    return newDate
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
    return randomCountry

  }
  def payment_txn_id(): Int = {
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
  def payment_txn_successGen(): Char = {
    val r = nextInt(10)
    if (r % 2 == 0) 'Y' else 'N'
  }
}
