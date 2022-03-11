import Streaming.spark
object Query {
  def pillowQ(): Unit = {



  }


  def abby(): Unit = {

    spark.sql("Select * from Test")

  }
  def jacobQ(): Unit = {
//    //test to be sure it's working
//    spark.sql(
//      """
//        |SELECT * FROM Test
//        |""".stripMargin)
//    //test for volume of purchases higher in certain cities
//          spark.sql(
//            """
//              |SELECT city, count(order_id) AS Num_Orders
//              |FROM Test
//              |GROUP BY city
//              |ORDER BY Num_Orders desc
//              |""".stripMargin).show()
//    //test for failed payments for enemies
//          spark.sql(
//            """
//              |SELECT customer_name, city, payment_txn_success
//              |FROM Test
//              |WHERE city = 'dumbville'
//              |""".stripMargin).show()
//    //test for host name purchase volume
//          spark.sql(
//            """
//              |SELECT ecommerce_website_name, count(order_id) AS Num_Orders
//              |FROM Test
//              |GROUP BY ecommerce_website_name
//              |ORDER BY Num_Orders
//              |""".stripMargin).show()
//    //test for food purchase volume
//          spark.sql(
//            """
//              |SELECT ecommerce_website_name, count(order_id) AS Num_Orders
//              |FROM Test
//              |WHERE product_category LIKE '%food%'
//              |OR product_category LIKE '%Food%'
//              |OR product_category LIKE '%FOOD%'
//              |GROUP BY ecommerce_website_name
//              |ORDER BY Num_Orders
//              |""".stripMargin).show()
  }
}
