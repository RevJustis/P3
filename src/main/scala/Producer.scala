object Producer {

  import java.util.Properties
  import Trends._
  import org.apache.kafka.clients.producer._

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    def sendRecord(
        producer: KafkaProducer[String, String],
        topic: String
    ): Unit = {
      val result=getMap()

      val key = getKey()

      val order_id = result("order_id")
      val customer_id = result("customer_id")
      val customer_name = result("customer_name")
      val product_id = result("product_id")
      val product_name = result("product_name")
      val product_category = result("product_category")
      val payment_type = result("payment_type")
      val qty = result("qty")
      val price = result("price")
      val datetime =result("datetime")
      val country = result("country")
      val city = result("city")
      val ecommerce_website_name =result("ecommerce_website_name")
      val payment_txn_id = result("payment_txn_id")
      val payment_txn_success = result("payment_txn_success")
      val failure_reason = result("failure_reason")

      //send json
      //  val value = s"""{"order_id": "$order_id","customer_id": "$customer_id","customer_name": "$customer_name","product_id": "$product_id","product_name": "$product_name","product_category": "$product_category","payment_type": "$payment_type","qty": "$qty","price": "$price","datetime": "$datetime","country": "$country","city": "$city","ecommerce_website_name": "$ecommerce_website_name","payment_txn_id": "$payment_txn_id","payment_txn_success": "$payment_txn_success","failure_reason": "$failure_reason"}"""
      //send csv
      val value = s"$order_id," + s"$customer_id," + s"$customer_name," + s"$product_id," + s"$product_name," + s"$product_category," + s"$payment_type," + s"$qty," + s"$price," + s"$datetime," + s"$country," + s"$city," + s"$ecommerce_website_name," + s"$payment_txn_id," + s"$payment_txn_success," + s"$failure_reason,"
      val record = new ProducerRecord[String, String](
        topic,
        key,
        value
      )
      producer.send(record)
      println("New record sent...")
    }

    def getKey(): String = {
      val r = scala.util.Random
      val key = r.nextInt(1000).toString()
      key
    }

    val props = new Properties()
    //3.86.155.113:9092
    props.put("bootstrap.servers", "[::1]:9092")
    props.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    val producer = new KafkaProducer[String, String](props)

    try {
          while (true) {

      //send csv
      sendRecord(producer, "pandoras_box")
      //send json
      //  sendRecord(producer,"json")

      Thread.sleep(2000)
      //

          }

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      producer.close()
    }

  }
}
