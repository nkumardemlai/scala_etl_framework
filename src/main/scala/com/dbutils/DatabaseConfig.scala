package com.dbutils

import java.sql.{Connection, DriverManager}

object DatabaseConfig {
  val url = "jdbc:mysql://localhost:3306/etl_audit_db"
  val username = "root"
  val password = "root"

  def getConnection: Connection = {
    DriverManager.getConnection(url, username, password)
  }
}
