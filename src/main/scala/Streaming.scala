import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}

import scala.collection.JavaConverters._

object streaming {

  import java.util.Properties
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:/winutils")
    val spark = SparkSession
      .builder()
      .appName("test")
      .config("spark.master","local")
      .config("spark.sql.streaming.noDataProgressEventInterval", 999999999)
      .getOrCreate()


    //json
    //    val mySchema = StructType(Array(
    //      StructField("order_id", StringType),
    //      StructField("customer_id", StringType),
    //      StructField("customer_name", StringType),
    //      StructField("product_id", StringType),
    //      StructField("product_name", StringType),
    //      StructField("product_category", StringType),
    //      StructField("payment_type", StringType),
    //      StructField("qty", StringType),
    //      StructField("price", StringType),
    //      StructField("datetime", StringType),
    //      StructField("country", StringType),
    //      StructField("city", StringType),
    //      StructField("ecommerce_website_name", StringType),
    //      StructField("payment_txn_id", StringType),
    //      StructField("payment_txn_success", StringType),
    //      StructField("failure_reason", StringType)
    //    ))
    //    val df = spark.readStream.format("kafka")
    //      //3.86.155.113:9092
    //      .option("kafka.bootstrap.servers", "[::1]:9092")
    //      .option("startingOffsets", "earliest")
    //      .option("subscribe", "json")
    //      .load()
    //      .select(col("value").cast("String"))
    //      .select(from_json(col("value"),mySchema).as("table"))
    //      .select("table.*")


    //csv

    val df = spark.readStream.format("kafka")
      //3.86.155.113:9092
      .option("kafka.bootstrap.servers", "[::1]:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "pandoras_box")
      .load()
      .select(split(col("value"),",").getItem(0).as("order_id"),
        split(col("value"),",").getItem(1).as("customer_id"),
        split(col("value"),",").getItem(2).as("customer_name"),
        split(col("value"),",").getItem(3).as("product_id"),
        split(col("value"),",").getItem(4).as("product_name"),
        split(col("value"),",").getItem(5).as("product_category"),
        split(col("value"),",").getItem(6).as("payment_type"),
        split(col("value"),",").getItem(7).as("qty"),
        split(col("value"),",").getItem(8).as("price"),
        split(col("value"),",").getItem(9).as("datetime"),
        split(col("value"),",").getItem(10).as("country"),
        split(col("value"),",").getItem(11).as("city"),
        split(col("value"),",").getItem(12).as("ecommerce_website_name"),
        split(col("value"),",").getItem(13).as("payment_txn_id"),
        split(col("value"),",").getItem(14).as("payment_txn_success"),
        split(col("value"),",").getItem(15).as("failure_reason"))

    //Both need the following
    //Sample querying
    df.printSchema()
    df.createOrReplaceTempView("test")
    spark.table("test").cache()
    spark.sql("select * from table")


    val df0=df
      .writeStream
      .outputMode("append")
      .format("console")
      .start()
    val df1=df.groupBy(col("country")).count()
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val df2=df.select(col("price").cast("int"),col("product_name"))
      .groupBy("product_name").max("price")
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
    df0.awaitTermination()
    df1.awaitTermination()
    df2.awaitTermination()
  }
}

