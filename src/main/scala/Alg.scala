import scala.util.Random._
import scala.math.BigDecimal._

def priceGen(): Double = {
  //creates random Int
  val whole = nextInt(5000)
  //creates random Float
  val dec = nextFloat()
  val price = whole + dec.setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
  return price
}
