import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.security.Key
import scala.collection.JavaConverters._

object streaming {

  import java.util.Properties
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:/winutils")
    val spark = SparkSession
      .builder()
      .appName("test")
      .config("spark.master","local")
      .getOrCreate()

    import spark.implicits._

    val mySchema = StructType(Array(
      StructField("order_id", StringType),
      StructField("customer_id", StringType),
      StructField("customer_name", StringType),
      StructField("product_id", StringType),
      StructField("product_name", StringType),
      StructField("product_category", StringType),
      StructField("payment_type", StringType),
      StructField("qty", StringType),
      StructField("price", StringType),
      StructField("datetime", StringType),
      StructField("country", StringType),
      StructField("city", StringType),
      StructField("ecommerce_website_name", StringType),
      StructField("payment_txn_id", StringType),
      StructField("payment_txn_success", StringType),
      StructField("failure_reason", StringType)
    ))

    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "[::1]:9092")
      .option("subscribe", "ingest")
      .option("startingOffsets", "earliest")
      .load()
      .select(col("value").cast("String"))
      .select(from_json(col("value"),mySchema).as("table"))
      .select("table.*")


    df.printSchema()
    df.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }
}

