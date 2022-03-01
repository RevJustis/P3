import scala.util.Random._
import scala.util.Random
import scala.math.BigDecimal._
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.io.Source

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

  def payment_txn_successGen(): Char = {
    val r = nextInt(10)
    if (r % 2 == 0) 'Y' else 'N'
  }
}
