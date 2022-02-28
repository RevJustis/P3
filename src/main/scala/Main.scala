object Main {
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("program started")

    val conf = new SparkConf().setMaster("local[4]").setAppName("kafkar")
    val ssc = new StreamingContext(conf, Seconds(2))

    //my kafka topic name is 'mytest'
    val kafkaStream = KafkaUtils.createStream(
      ssc,
      "localhost:2181",
      "spark-streaming-consumer-group",
      Map("test-topic" -> 5)
    )
    kafkaStream.print()
    ssc.start
    ssc.awaitTermination()
  }
}
