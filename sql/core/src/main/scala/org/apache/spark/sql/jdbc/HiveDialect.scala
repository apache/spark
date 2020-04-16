package org.apache.spark.sql.jdbc

import java.util.Locale

import org.apache.spark.sql.types.{DataType, MetadataBuilder}

private case object HiveDialect extends JdbcDialect {

  override def canHandle(url : String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:hive")

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    None
  }

  override def quoteIdentifier(colName: String): String = {
    colName
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)
}
