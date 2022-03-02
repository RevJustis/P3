import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.util.Collections
import scala.collection.JavaConverters._

object Consumer extends App {

  import java.util.Properties


  val topics = List[String]("Topic")

  val props = new Properties()
  //for apple users / windows 11 users, change [::1] below to localhost to run
  props.put("bootstrap.servers", "[::1]:9092")


  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")


  /*val consumer = new KafkaConsumer(props)
  try {
    consumer.subscribe(topics.asJava)
    while(true) {
      val records = consumer.poll(10)
      for (record <- records.asScala) {
        println("************")

        println(s"Topic: ${record.topic()}")
        println("*************")
        println(record.value())
      }
    }

  } catch {
    case e: Exception => {
      e.printStackTrace()
    }
  }*/

  var test = ""
  val consumer = new KafkaConsumer(props)
  var count = 0

  consumer.subscribe(topics.asJava)
  while (count < 5) {
    val records = consumer.poll(10)
    for (record <- records.asScala) {
      println("************")

      println(s"Topic: ${record.topic()}")
      println("*************")
      test = record.value()
      consumer.subscribe(topics.asJava)
      println(test)
      count += 1


    }
  }
}