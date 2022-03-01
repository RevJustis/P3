


object Producer {

  import java.util.Properties

  import org.apache.kafka.clients.producer._


  def sendRecord(producer: KafkaProducer[String, String], topic: String): Unit = {

    val key = getKey()
    val value = "TestData"

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
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  try {
    while (true) {


      sendRecord(producer, "topic1")

      sendRecord(producer, "topic2")

      sendRecord(producer, "topic3")

      sendRecord(producer, "topic4")

      sendRecord(producer, "topic5")

      sendRecord(producer, "topic6")

    }

  } catch {
    case e: Exception => {
      e.printStackTrace()
    }
  } finally {
    producer.close()
  }

  /*val mySchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("number", IntegerType),
    StructField("name", StringType),
    StructField("addressNumber", IntegerType),
    StructField("address", StringType),
    StructField("type", StringType),
    StructField("number1", IntegerType),
    StructField("number2", IntegerType),
    StructField("timestamp", StringType),
    StructField("country", StringType),
    StructField("state", StringType),
    StructField("website", StringType),
    StructField("zip", IntegerType),
    StructField("success", StringType)
  ))

  val streamingDataFrame = spark.readStream.schema(mySchema).csv("input/input.csv")


  streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").
    writeStream
    .format("kafka")
    .option("topic", "testTopic")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "path to your local dir")
    .start()*/


  /*val producer = new KafkaProducer[String, String](props)


  val TOPIC="test"

  for(i<- 1 to 50){
    val record = new ProducerRecord(TOPIC, "key", s"hello $i")
    producer.send(record)
  }

  val record = new ProducerRecord(TOPIC, "key", "the end "+new java.util.Date)
  producer.send(record)

  producer.close()*/

}