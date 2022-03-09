import Alg._
import Trends._

import scala.util.Random._
import scala.collection.mutable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object Main {
  //System.setProperty("hadoop.home.dir", "C:\\hadoop")
  //System.setProperty("hadoop.home.dir", "c:/winutils")

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("P3")
    .getOrCreate()

  // Create the DataFrames at global scope so that they are made once and used many times
  val dfA = spark.read
    .parquet("input/pq/amazon.parquet")
    .withColumn(
      "SellingPrice",
      col("SellingPrice").cast(DoubleType)
    )
    .select("ProductName", "Category", "SellingPrice", "ProductUrl")

  val dfW = spark.read
    .parquet("input/pq/walmart.parquet")
    .withColumn(
      "SalePrice",
      col("SalePrice").cast(DoubleType)
    )
    .select("ProductName", "Category", "SalePrice", "ProductUrl")

  val dfE = spark.read
    .parquet("input/pq/ebay.parquet")
    .withColumn("Price", col("Price").cast(DoubleType))
    .select("Title", "Price", "Pageurl")

  var ID = 1 // starting point for the incrementing id

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    dfA.persist(StorageLevel.MEMORY_ONLY_SER_2)
    dfW.persist(StorageLevel.MEMORY_ONLY_SER_2)
    dfE.persist(StorageLevel.MEMORY_ONLY_SER_2)

    var i = 0
    while (i < 30) {
      println(getMap())
      println("*************************")
      i += 1
    }
  }
}
