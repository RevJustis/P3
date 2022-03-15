import Streaming.spark
import org.apache.spark.sql.functions.{
  col,
  date_trunc,
  dayofmonth,
  hour,
  minute,
  second,
  to_timestamp,
  when
}

object Query {
  def selectAllQ: Unit = {
    println("select *")
    spark.sql("select * from test").show(10, false)
  }

  def rowCountQ: Unit = {
    spark.sql("select count(*) as RowCount from test").show
  }

  def pillowQ(): Unit = {
    println("Pillow Query")
    spark
      .sql(
        "SELECT * FROM Test WHERE LENGTH(substring_index(customer_name, ' ', 1)) > 9"
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
          "or product_category like '%Fitness%' " +
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
          "or product_category like '%Cosmetic%' " +
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
          "union " +
          "select 'Books', count(*) as Count from Test " +
          "where product_category like '%Book%' " +
          "union " +
          "select 'Movies', count(*) as Count from Test " +
          "where product_category like '%Movie%' " +
          "union " +
          "select 'Music', count(*) as Count from Test " +
          "where product_category like '%Music%' " +
          "union " +
          "select 'Music', count(*) as Count from Test " +
          "where product_category like '%Music%' " +
          "union " +
          "select 'Accessories', count(*) as Count from Test " +
          "where product_category like '%Accessories%' " +
          "union " +
          "select 'Appliances', count(*) as Count from Test " +
          "where product_category like '%Appliance%' " +
          "order by Count desc"
      )
      .show(100, false)

  }

  def categoriesByCountry(): Unit = {
    println("order of category popularity (grouped by country)")
    // spark.sql("DROP TABLE cc")
    // spark.sql(
    //   "CREATE TEMP VIEW cc AS " +
    //     "select product_category, country from test where product_category != 'n/a' and product_category != NULL"
    // )
    spark
      .sql(
        "select country, 'Toys' as Category, count(*) as Count from test " +
          "where product_category like '%Toys%' " +
          "group by country " +
          "union " +
          "select country, 'Hobbies', count(*) as Count from test " +
          "where product_category like '%Hobbies%' " +
          "group by country " +
          "union " +
          "select country, 'Statues', count(*) as Count from test " +
          "where product_category like '%Statues%' " +
          "group by country " +
          "union " +
          "select country, 'Sports', count(*) as Count from test " +
          "where product_category like '%Sports & Outdoor Play%' " +
          "group by country " +
          "union " +
          "select country, 'Furniture', count(*) as Count from test " +
          "where product_category like '%Furniture%' " +
          "group by country " +
          "union " +
          "select country, 'Food and Snacks', count(*) as Count from test " +
          "where product_category like '%Food%' " +
          "or product_category like '%Snack%' " +
          "group by country " +
          "union " +
          "select country, 'Exercise and Health', count(*) as Count from test " +
          "where product_category like '%Exercise%' " +
          "or product_category like '%Health%' " +
          "group by country " +
          "union " +
          "select country, 'Jewelry', count(*) as Count from test " +
          "where product_category like '%Jewelry%' " +
          "group by country " +
          "union " +
          "select country, 'Decor', count(*) as Count from test " +
          "where product_category like '%Decor%' " +
          "group by country " +
          "union " +
          "select country, 'Electronics and Computing', count(*) as Count from test " +
          "where product_category like '%Electronic%' " +
          "or product_category like '%Computer%' " +
          "or product_category like '%Computing%' " +
          "group by country " +
          "union " +
          "select country, 'Skincare', count(*) as Count from test " +
          "where product_category like '%Skin%' " +
          "and product_category like '%care%' " +
          "or product_category like '%Cosmetic%' " +
          "group by country " +
          "union " +
          "select country, 'Baby', count(*) as Count from test " +
          "where product_category like '%Baby%' " +
          "group by country " +
          "union " +
          "select country, 'Shoes', count(*) as Count from test " +
          "where product_category like '%Shoe%' " +
          "group by country " +
          "union " +
          "select country, 'Clothing', count(*) as Count from test " +
          "where product_category like '%Clothes%' " +
          "or product_category like '%Clothing%' " +
          "group by country " +
          "union " +
          "select country, 'Books', count(*) as Count from test " +
          "where product_category like '%Book%' " +
          "group by country " +
          "union " +
          "select country, 'Movies', count(*) as Count from test " +
          "where product_category like '%Movie%' " +
          "group by country " +
          "union " +
          "select country, 'Music', count(*) as Count from test " +
          "where product_category like '%Music%' " +
          "group by country " +
          "union " +
          "select country, 'Music', count(*) as Count from test " +
          "where product_category like '%Music%' " +
          "group by country " +
          "union " +
          "select country, 'Accessories', count(*) as Count from test " +
          "where product_category like '%Accessories%' " +
          "group by country " +
          "union " +
          "select country, 'Appliances', count(*) as Count from test " +
          "where product_category like '%Appliance%' " +
          "group by country " +
          "order by Country asc, Count desc"
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
  //below codes only work when df is already read
  //Leo-when price drops (e.g.last week of month), quantity bought increases for clothing and food category
  /* val df4=df.select(col("price"),col("product_category"),col("qty").cast("int"),col("datetime"))
    .filter(col("product_category").contains("Clothing")||col("product_category").contains("Food"))
    .withColumn("date",to_timestamp(col("datetime")))
    .withColumn("isLastWeek", when(dayofmonth(col("date"))>23,"isLastWeek").otherwise("NotLastWeek") )
    .groupBy("isLastWeek").avg("qty")
    .writeStream.outputMode("complete").format("console").start()

  //Leo-Just how to split datetime(example)
  val df3=df.select(col("datetime"))
    .withColumn("convert", to_timestamp(col("datetime")))
    .withColumn("hour",hour(col("convert")))
    .withColumn("minutes", minute(col("convert")))
    .withColumn("seconds",second(col("convert")))
    .withColumn("hour",date_trunc("hour",col("convert")))
    .withColumn("minute",date_trunc("minute",col("convert")))
    .writeStream.outputMode("append").format("console").start() */
}
