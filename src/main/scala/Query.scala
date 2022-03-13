import Streaming.spark
object Query {
  def selectAllQ: Unit = {
    println("select *")
    spark.sql("select * from test")
  }

  def pillowQ(): Unit = {
    println("Pillow Query")
    spark
      .sql(
        "SELECT * FROM Test WHERE LENGTH(substring_index(customer_name, ' ', 1)) > 8"
      )
      .show
  }
  def orderCountByCategory(): Unit = {
    println("order of category popularity (raw)")
    // (clothing > books > movies > music > games > consumer electronics > food and drinks
    //    > cosmetics/body care > accessories > toys > health products > furniture/home products
    //    > sports > household appliances)
    // SHOULD WE DO POPULARITY WITH EACH CATEGORY - Justis
    spark
      .sql(
        "select product_category, count(*) as count from Test group by product_category order by count desc"
      )
      .show(50, false)

    println("order of category popularity (simplified specific)")
    spark
      .sql(
        "select 'Toys' as Category, count(*) as Count from Test " +
          "where product_category like '%Toys%' " +
          "union " +
          "select 'Hobbies', count(*) as Count from Test " +
          "where product_category like '%Hobbies%' " +
          "union " +
          "select 'Statues', count(*) as Count from Test " +
          "where product_category like '%Statues%' " +
          "union " +
          "select 'Sports', count(*) as Count from Test " +
          "where product_category like '%Sports & Outdoor Play%' " +
          "union " +
          "select 'Furniture', count(*) as Count from Test " +
          "where product_category like '%Furniture%' " +
          "union " +
          "select 'Food and Snacks', count(*) as Count from Test " +
          "where product_category like '%Food%' " +
          "or product_category like '%Snack%' " +
          "union " +
          "select 'Exercise and Health', count(*) as Count from Test " +
          "where product_category like '%Exercise%' " +
          "or product_category like '%Health%' " +
          "union " +
          "select 'Jewelry', count(*) as Count from Test " +
          "where product_category like '%Jewelry%' " +
          "union " +
          "select 'Decor', count(*) as Count from Test " +
          "where product_category like '%Decor%' " +
          "union " +
          "select 'Electronics and Computing', count(*) as Count from Test " +
          "where product_category like '%Electronic%' " +
          "or product_category like '%Computer%' " +
          "or product_category like '%Computing%' " +
          "union " +
          "select 'Skincare', count(*) as Count from Test " +
          "where product_category like '%Skin%' " +
          "and product_category like '%care%' " +
          "union " +
          "select 'Baby', count(*) as Count from Test " +
          "where product_category like '%Baby%' " +
          "union " +
          "select 'Shoes', count(*) as Count from Test " +
          "where product_category like '%Shoe%' " +
          "union " +
          "select 'Clothing', count(*) as Count from Test " +
          "where product_category like '%Clothes%' " +
          "or product_category like '%Clothing%' " +
          "order by Count desc"
      )
      .show(100, false)

  }

  def priceByCountryQ(): Unit = {
    println("max price by country")
    spark
      .sql(
        "Select country, max(price / qty) as MaxPrice FROM Test GROUP BY COUNTRY ORDER BY MaxPrice DESC"
      )
      .show()
  }

  def jacobQ(): Unit = {
    /*
   //test to be sure it's working
   spark.sql(
     """
       |SELECT * FROM Test
       |""".stripMargin)
   //test for volume of purchases higher in certain cities
         spark.sql(
           """
             |SELECT city, count(order_id) AS Num_Orders
             |FROM Test
             |GROUP BY city
             |ORDER BY Num_Orders desc
             |""".stripMargin).show()
   //test for failed payments for enemies
         spark.sql(
           """
             |SELECT customer_name, city, payment_txn_success
             |FROM Test
             |WHERE city = 'dumbville'
             |""".stripMargin).show()
   //test for host name purchase volume
         spark.sql(
           """
             |SELECT ecommerce_website_name, count(order_id) AS Num_Orders
             |FROM Test
             |GROUP BY ecommerce_website_name
             |ORDER BY Num_Orders
             |""".stripMargin).show()
   //test for food purchase volume
         spark.sql(
           """
             |SELECT ecommerce_website_name, count(order_id) AS Num_Orders
             |FROM Test
             |WHERE product_category LIKE '%food%'
             |OR product_category LIKE '%Food%'
             |OR product_category LIKE '%FOOD%'
             |GROUP BY ecommerce_website_name
             |ORDER BY Num_Orders
             |""".stripMargin).show()
     */
  }
}
