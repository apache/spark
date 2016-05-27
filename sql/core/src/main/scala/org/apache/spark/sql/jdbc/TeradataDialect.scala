package org.apache.spark.sql.jdbc

import org.apache.spark.sql.types.{BinaryType, StringType, DataType}

/**
 * Created by lhl on 5/27/2016.
 */
private object TeradataDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:teradata")

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Option(JdbcType("VARCHAR(255)", java.sql.Types.CLOB))
    case BinaryType => Option(JdbcType("VARBYTE(4)", java.sql.Types.BLOB))
    case _ => None
  }
  override def getTableExistsQuery(table: String): String = {
    s"SELECT TOP 1 1 FROM $table"
  }
}
