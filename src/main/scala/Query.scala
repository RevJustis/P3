import Streaming.spark
object Query {
  def pillowQ(): Unit = {
    println("Pillow Query")
    spark
      .sql(
        "SELECT * FROM Test WHERE LENGTH(substring_index(customer_name, ' ', 1)) > 8"
      )
      .show
  }

  def abby(): Unit = {
    println("Select * Query")
    spark.sql("Select * from Test").show
  }
}
