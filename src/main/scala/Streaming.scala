import Query._

import java.util
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{col, date_trunc, dayofmonth, from_json, hour, minute, second, split, to_timestamp, when}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.sql.DriverManager
import scala.collection.JavaConverters._

object Streaming {
  val spark = SparkSession
    .builder()
    .appName("test")
    .config("spark.master", "local")
    .config("spark.sql.streaming.noDataProgressEventInterval", 999999999)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "c:/hadoop")
    System.setProperty("hadoop.home.dir", "c:/winutils")

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

    val df = spark.readStream
      .format("kafka")
      // .option("kafka.bootstrap.servers", "[::1]:9092")
      .option("kafka.bootstrap.servers", "3.86.155.113:9092")
      // .option("kafka.bootstrap.servers", "44.200.236.7:6666")
      .option("startingOffsets", "earliest")
      // .option("subscribe", "pandoras_box")

      .option("subscribe", "Tuesday13")

      // .option("subscribe", "retention_test")
      //.option("poll", 200)
      .option(
        "maxOffsetsPerTrigger",
        150
      ) // Defines the rate that rows are appended
      .load()
      .select(
        split(col("value"), ",").getItem(0).as("order_id"),
        split(col("value"), ",").getItem(1).as("customer_id").cast("int"),
        split(col("value"), ",").getItem(2).as("customer_name"),
        split(col("value"), ",").getItem(3).as("product_id").cast("int"),
        split(col("value"), ",").getItem(4).as("product_name"),
        split(col("value"), ",").getItem(5).as("product_category"),
        split(col("value"), ",").getItem(6).as("payment_type"),
        split(col("value"), ",").getItem(7).as("qty").cast("int"),
        split(col("value"), ",").getItem(8).as("price").cast("double"),
        split(col("value"), ",").getItem(9).as("datetime"),
        split(col("value"), ",").getItem(10).as("country"),
        split(col("value"), ",").getItem(11).as("city"),
        split(col("value"), ",").getItem(12).as("ecommerce_website_name"),
        split(col("value"), ",").getItem(13).as("payment_txn_id"),
        split(col("value"), ",").getItem(14).as("payment_txn_success"),
        split(col("value"), ",").getItem(15).as("failure_reason")
      )


    val WriteToSQLQuery = df.writeStream.foreach(new ForeachWriter[Row] {
      var connection: java.sql.Connection = _
      var statement: java.sql.Statement = _

      val jdbcUsername = "admin"
      val jdbcPassword = "Password"
      val jdbcHostname = "p3.cwoofs136vmw.us-west-1.rds.amazonaws.com" //typically, this is in the form or servername.database.windows.net
      val jdbcPort = 3306
      val jdbcDatabase = "p3schema"
      val driver = "com.mysql.cj.jdbc.Driver"
      val jdbc_url = "jdbc:mysql://p3.cwoofs136vmw.us-west-1.rds.amazonaws.com/p3schema"
      //val jdbc_url = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

      def open(partitionId: Long, version: Long): Boolean = {
        Class.forName(driver)
        connection = DriverManager.getConnection(jdbc_url, jdbcUsername, jdbcPassword)
        statement = connection.createStatement
        true
      }

      def process(value: Row): Unit = {
        val order_id = value(0)
        val customer_id = value(1)
        val customer_name = value(2)
        val product_id = value(3)
        val product_name = value(4)
        val product_category = value(5)
        val payment_type = value(6)
        val qty = value(7)
        val price = value(8)
        val datetime = value(9)
        val countrys = value(10)
        val city = value(11)
        val ecommerce_website_name = value(12)
        val payment_txn_id = value(13)
        val payment_txn_success = value(14)
        val failure_reason = value(15)

        try {

          statement.execute(s"insert into DataLakeNew3 (order_id, customer_id, customer_name, product_id, product_name, " +
            s" product_category, payment_type, qty, price, datetime, country, city, ecommerce_website_name, payment_txn_id" +
            s", payment_txn_success, failure_reason) values ('$order_id','$customer_id','$customer_name','$product_id', 'Test','$product_category'," +
            s"'$payment_type','$qty','$price','$datetime','$countrys','$city','$ecommerce_website_name','$payment_txn_id','$payment_txn_success'," +
            s"'$failure_reason')")

          //connection.close
        } catch {
          case e: Exception =>
            e.printStackTrace()

          case f: Error =>
            f.printStackTrace()
        }

        /* statement.execute(s"insert into DataLakeNew2 (order_id, customer_id, customer_name, product_id, product_name," +
           s" product_category, payment_type, qty, price, datetime, country, city, ecommerce_website_name, payment_txn_id" +
           s", payment_txn_success, failure_reason) values ('$order_id','$customer_id','$customer_name','$product_id','$product_name','$product_category'," +
           s"'$payment_type','$qty','$price','$datetime','$countrys','$city','$ecommerce_website_name','$payment_txn_id','$payment_txn_success'," +
           s"'$failure_reason')")*/


        /*val valueStr = "'" + order_id + "'," + "'" + customer_id + "'," + "'" + customer_name + "'," + "'" + product_id +
          "'," + "'" + product_name + "'," + "'" + product_category + "'," + "'" + payment_type + "'" +
          "'," + "'" + qty + "'," + "'" + price + "'," + "'" + datetime + "'" +
          "'," + "'" + country + "'," + "'" + city + "'," + "'" + ecommerce_website_name + "'" +
          "'," + "'" + payment_txn_id + "'," + "'" + payment_txn_success + "'," + "'" + failure_reason + "'"
        statement.execute("INSERT INTO " + "DataLake" + " (order_id, customer_id, customer_name, product_id, product_name, product_category" +
          ", payment_type, qty, price, datetime, country, city, ecommerce_website_name, payment_txn_id, payment_txn_success, failure_reason)" + " VALUES (" + valueStr + ")")*/
      }

      def close(errorOrNull: Throwable): Unit = {
        connection.close
      }
    })
    //.option("checkpointLocation", "checkpoint")


    while (true) {

      WriteToSQLQuery.start()
    }
  }
}

















    /*val df1 = df

    val df1 = df

      .withColumn("convert", to_timestamp(col("datetime")))
      .withColumn("hours", hour(col("convert")))
      .withColumn("minutes", minute(col("convert")))
      .withColumn("seconds", second(col("convert")))
      //.withColumn("hour", date_trunc("hour", col("convert")))
      .withColumn("minute", date_trunc("minute", col("convert")))

    // CSV or JSON need the following
    df.printSchema()
    df1.printSchema()

    val df0 = df1
      .limit(50000) // hard limit on number of rows
      .writeStream
      .outputMode("append")
      .format("memory")
      .queryName("Test")
      // .option("maxRowsInMemory", 3000)
      // .option("maxBytesInMemory", 25000)
      // .option("maxTotalRows", 3000)
      //.trigger(Trigger.ProcessingTime(1000))
      .start()

    while (df0.isActive) {
      Thread.sleep(5000)
      val t = System.nanoTime
      selectAllQ
      rowCountQ
      //jacobQ() // A collection of queries written by Jacob
      priceByCountryQ() // Written by Abby
      pillowQ()
      // orderCountByCategory()
      categoriesByCountry()

      println(
        "Time to query is: " + (System.nanoTime - t) / 1e9 + " seconds."
      )
    }

    df0.awaitTermination()

    //Sample querying
    /*df.createOrReplaceTempView("test")
    spark.table("test").cache()
    spark.sql("select * from table")

    val df0 = df.writeStream
      .outputMode("append")
      .format("console")
      .start()
    val df1 = df
      .groupBy(col("country"))
      .count()
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val df2 = df
      .select(col("price").cast("int"), col("product_name"))
      .groupBy("product_name")
      .max("price")
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
    df0.awaitTermination()
    df1.awaitTermination()
    df2.awaitTermination()*/
  }
}
