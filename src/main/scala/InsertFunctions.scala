import java.sql.PreparedStatement

object InsertFunctions {

  def addNewUser(username: String, password: String) : Boolean = {

    val connection = DatabaseConnector.dbConnection()
    val rs = "insert into DataLake (order_id, customer_id, customer_name, product_id, product_name, product_category, " +
      "payment_type, qty, price, country, city, ecommerce_website_name, payment_txn_id, payment_txn_success, " +
      "failure_reason ) values (?,?)"

    try {
      val preparedStmt: PreparedStatement = connection.prepareStatement(rs)
      preparedStmt.setString(1, username)
      preparedStmt.setString(2,password)
      preparedStmt.execute()
    }
    catch {
      case e => e.printStackTrace
    }
    return true
  }

}
