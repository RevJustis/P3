object Producer {

  import java.util.Properties

  import org.apache.kafka.clients.producer._

  def main(args: Array[String]): Unit = {
    def sendRecord(producer: KafkaProducer[String, String], topic: String): Unit = {

      val key = getKey()

      val order_id = "23"
      val customer_id = "1011"
      val customer_name = "leo chen"
      val product_id = "1234"
      val product_name = "xbox"
      val product_category = "gaming"
      val payment_type = "visa"
      val qty = "20"
      val price = "499"
      val datetime = "2022-03-04 10:00"
      val country = "USA"
      val city = "Los Angeles"
      val ecommerce_website_name = "www.microsoft.com"
      val payment_txn_id = "56789"
      val payment_txn_success = "Y"
      val failure_reason = ""

      //hard code adding record:
      //val value = """{"order_id": "1","customer_id": "101","customer_name": "John Smith","product_id": "201","product_name": "Pen","product_category": "Stationery","payment_type": "Card","qty": "24","price": "10","datetime": "2021-01-10 10:12","country": "India","city": "Mumbai","ecommerce_website_name": "www.amazon.com","payment_txn_id": "36766","payment_txn_success": "Y","failure_reason": ""}"""
      val value = s"""{"order_id": "$order_id","customer_id": "$customer_id","customer_name": "$customer_name","product_id": "$product_id","product_name": "$product_name","product_category": "$product_category","payment_type": "$payment_type","qty": "$qty","price": "$price","datetime": "$datetime","country": "$country","city": "$city","ecommerce_website_name": "$ecommerce_website_name","payment_txn_id": "$payment_txn_id","payment_txn_success": "$payment_txn_success","failure_reason": "$failure_reason"}"""


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
    props.put("bootstrap.servers", "[::1]:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while (true) {
      try {
        //testing, sending a single record
        sendRecord(producer, "ingest")
        Thread.sleep(2000)


      } catch {
        case e: Exception => {
          e.printStackTrace()
        }
      }
      /*finally {
        producer.close()
        }
       */

    }
  }
}