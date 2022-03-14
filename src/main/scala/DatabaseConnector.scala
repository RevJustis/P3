import java.sql.{Connection, DriverManager, PreparedStatement}


object DatabaseConnector {
  def dbConnection(): Connection = {
    val mySQLURL = "jdbc:mysql://p3.cwoofs136vmw.us-west-1.rds.amazonaws.com/p3schema"
    val databaseUserName = "admin"
    val databasePassword = "Password"
    val driver = "com.mysql.cj.jdbc.Driver"
    var con = DriverManager.getConnection(mySQLURL, databaseUserName, databasePassword)
    try{
      Class.forName(driver)
      con = DriverManager.getConnection(mySQLURL, databaseUserName, databasePassword)
    } catch {
      case e: Exception =>
        e.printStackTrace()
      case f: Error =>
        f.printStackTrace()
    }
    return con
  }


}