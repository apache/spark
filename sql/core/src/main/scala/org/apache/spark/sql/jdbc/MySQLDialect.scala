/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.jdbc

import java.sql.{Connection, SQLException, Types}
import java.util
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{IndexAlreadyExistsException, NoSuchIndexException}
import org.apache.spark.sql.connector.catalog.index.TableIndex
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types.{BooleanType, DataType, FloatType, LongType, MetadataBuilder}

private case object MySQLDialect extends JdbcDialect with SQLConfHelper {

  override def canHandle(url : String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:mysql")

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.VARBINARY && typeName.equals("BIT") && size != 1) {
      // This could instead be a BinaryType if we'd rather return bit-vectors of up to 64 bits as
      // byte arrays instead of longs.
      md.putLong("binarylong", 1)
      Option(LongType)
    } else if (sqlType == Types.BIT && typeName.equals("TINYINT")) {
      Option(BooleanType)
    } else None
  }

  override def quoteIdentifier(colName: String): String = {
    s"`$colName`"
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

  // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
  override def getUpdateColumnTypeQuery(
      tableName: String,
      columnName: String,
      newDataType: String): String = {
    s"ALTER TABLE $tableName MODIFY COLUMN ${quoteIdentifier(columnName)} $newDataType"
  }

  // See Old Syntax: https://dev.mysql.com/doc/refman/5.6/en/alter-table.html
  // According to https://dev.mysql.com/worklog/task/?id=10761 old syntax works for
  // both versions of MySQL i.e. 5.x and 8.0
  // The old syntax requires us to have type definition. Since we do not have type
  // information, we throw the exception for old version.
  override def getRenameColumnQuery(
      tableName: String,
      columnName: String,
      newName: String,
      dbMajorVersion: Int): String = {
    if (dbMajorVersion >= 8) {
      s"ALTER TABLE $tableName RENAME COLUMN ${quoteIdentifier(columnName)} TO" +
        s" ${quoteIdentifier(newName)}"
    } else {
      throw QueryExecutionErrors.renameColumnUnsupportedForOlderMySQLError()
    }
  }

  // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
  // require to have column data type to change the column nullability
  // ALTER TABLE tbl_name MODIFY [COLUMN] col_name column_definition
  // column_definition:
  //    data_type [NOT NULL | NULL]
  // e.g. ALTER TABLE t1 MODIFY b INT NOT NULL;
  // We don't have column data type here, so throw Exception for now
  override def getUpdateColumnNullabilityQuery(
      tableName: String,
      columnName: String,
      isNullable: Boolean): String = {
    throw QueryExecutionErrors.unsupportedUpdateColumnNullabilityError()
  }

  // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
  override def getTableCommentQuery(table: String, comment: String): String = {
    s"ALTER TABLE $table COMMENT = '$comment'"
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    // See SPARK-35446: MySQL treats REAL as a synonym to DOUBLE by default
    // We override getJDBCType so that FloatType is mapped to FLOAT instead
    case FloatType => Option(JdbcType("FLOAT", java.sql.Types.FLOAT))
    case _ => JdbcUtils.getCommonJDBCType(dt)
  }

  // CREATE INDEX syntax
  // https://dev.mysql.com/doc/refman/8.0/en/create-index.html
  override def createIndex(
      indexName: String,
      indexType: String,
      tableName: String,
      columns: Array[NamedReference],
      columnsProperties: Array[util.Map[NamedReference, util.Properties]],
      properties: util.Properties): String = {
    val columnList = columns.map(col => quoteIdentifier(col.fieldNames.head))
    var indexProperties: String = ""
    val scalaProps = properties.asScala
    if (!properties.isEmpty) {
      scalaProps.foreach { case (k, v) =>
        indexProperties = indexProperties + " " + s"$k $v"
      }
    }
    val iType = if (indexType.isEmpty) {
      ""
    } else {
      if (indexType.length > 1 && !indexType.equalsIgnoreCase("BTREE") &&
        !indexType.equalsIgnoreCase("HASH")) {
        throw new UnsupportedOperationException(s"Index Type $indexType is not supported." +
          " The supported Index Types are: BTREE and HASH")
      }
      s"USING $indexType"
    }
    // columnsProperties doesn't apply to MySQL so it is ignored
    s"CREATE INDEX ${quoteIdentifier(indexName)} $iType ON" +
      s" ${quoteIdentifier(tableName)} (${columnList.mkString(", ")}) $indexProperties"
  }

  // SHOW INDEX syntax
  // https://dev.mysql.com/doc/refman/8.0/en/show-index.html
  override def indexExists(
      conn: Connection,
      indexName: String,
      tableName: String,
      options: JDBCOptions): Boolean = {
    val sql = s"SHOW INDEXES FROM ${quoteIdentifier(tableName)}"
    try {
      val rs = JdbcUtils.executeQuery(conn, options, sql)
      while (rs.next()) {
        val retrievedIndexName = rs.getString("key_name")
        if (conf.resolver(retrievedIndexName, indexName)) {
          return true
        }
      }
      false
    } catch {
      case _: Exception =>
        logWarning("Cannot retrieved index info.")
        false
    }
  }

  override def dropIndex(indexName: String, tableName: String): String = {
    s"DROP INDEX ${quoteIdentifier(indexName)} ON $tableName"
  }

  // SHOW INDEX syntax
  // https://dev.mysql.com/doc/refman/8.0/en/show-index.html
  override def listIndexes(
      conn: Connection,
      tableName: String,
      options: JDBCOptions): Array[TableIndex] = {
    val sql = s"SHOW INDEXES FROM $tableName"
    var indexMap: Map[String, TableIndex] = Map()
    try {
      val rs = JdbcUtils.executeQuery(conn, options, sql)
      while (rs.next()) {
        val indexName = rs.getString("key_name")
        val colName = rs.getString("column_name")
        val indexType = rs.getString("index_type")
        val indexComment = rs.getString("Index_comment")
        if (indexMap.contains(indexName)) {
          val index = indexMap.get(indexName).get
          val newIndex = new TableIndex(indexName, indexType,
            index.columns() :+ FieldReference(colName),
            index.columnProperties, index.properties)
          indexMap += (indexName -> newIndex)
        } else {
          // The only property we are building here is `COMMENT` because it's the only one
          // we can get from `SHOW INDEXES`.
          val properties = new util.Properties();
          if (indexComment.nonEmpty) properties.put("COMMENT", indexComment)
          val index = new TableIndex(indexName, indexType, Array(FieldReference(colName)),
            new util.HashMap[NamedReference, util.Properties](), properties)
          indexMap += (indexName -> index)
        }
      }
    } catch {
      case _: Exception =>
        logWarning("Cannot retrieved index info.")
    }
    indexMap.values.toArray
  }

  override def classifyException(message: String, e: Throwable): AnalysisException = {
    e match {
      case sqlException: SQLException =>
        sqlException.getErrorCode match {
          // ER_DUP_KEYNAME
          case 1061 =>
            throw new IndexAlreadyExistsException(message, cause = Some(e))
          case 1091 =>
            throw new NoSuchIndexException(message, cause = Some(e))
          case _ => super.classifyException(message, e)
        }
      case unsupported: UnsupportedOperationException => throw unsupported
      case _ => super.classifyException(message, e)
    }
  }
}
