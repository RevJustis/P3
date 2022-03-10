import Test._
import Trends._
import spark.implicits._

class MyThread extends Thread {
  override def run() {
    val n = Thread.currentThread().getName()
    // Displaying the thread that is running
    // println("Thread " + n + " is running.")
    getMap(n).toSeq.toDF.show
  }
}
