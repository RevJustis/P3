import scala.util.Random._
import scala.math.BigDecimal._
import java.text.SimpleDateFormat
import java.util.Calendar

object Alg {
  def yourFunctions(): Unit = {}

  def timestampGenerator: String = {
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
