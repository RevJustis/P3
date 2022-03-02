import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.util.Collections
import scala.collection.JavaConverters._

object Consumer extends App {

  import java.util.Properties

  //val TOPIC="TopicNewTest1"
  //val TOPIC2="TopicNewTest2"
  val topics = List[String] ("TopicNew111","TopicNew222","TopicNew333","TopicNew444","TopicNew555")

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  /*val consumer = new KafkaConsumer[String, String](props)
  val a = Collections.singletonList(topics)

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  while(true){
    Thread.sleep(1000)
    val records=consumer.poll(100)
    for (record<-records.asScala){
      println(record)
    }
  }*/

  val consumer = new KafkaConsumer(props)
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
  }
}