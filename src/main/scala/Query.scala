import Streaming.spark
object Query {
  def pillowQ(): Unit = {

    while(true) {
      Thread.sleep(200)

      spark.sql("Select count(order_id) from TestTable").show()
    }

  }
}
