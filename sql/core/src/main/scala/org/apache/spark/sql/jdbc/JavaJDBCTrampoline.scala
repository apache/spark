package org.apache.spark.sql.jdbc

import org.apache.spark.sql.DataFrame

class JavaJDBCTrampoline {
  def createJDBCTable(rdd: DataFrame, url: String, table: String, allowExisting: Boolean) {
    rdd.createJDBCTable(url, table, allowExisting);
  }

  def insertIntoJDBC(rdd: DataFrame, url: String, table: String, overwrite: Boolean) {
    rdd.insertIntoJDBC(url, table, overwrite);
  }
}
