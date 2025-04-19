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

import scala.collection.mutable.ArrayBuilder
import scala.util.control.NonFatal

import org.apache.spark.{SparkThrowable, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{IndexAlreadyExistsException, NoSuchIndexException}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.index.TableIndex
import org.apache.spark.sql.connector.expressions.{Expression, Extract, FieldReference, NamedReference, NullOrdering, SortDirection}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types._

private case class MySQLDialect() extends JdbcDialect with SQLConfHelper with NoLegacyJDBCError {

  override def canHandle(url : String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:mysql")

  private val distinctUnsupportedAggregateFunctions =
    Set("VAR_POP", "VAR_SAMP", "STDDEV_POP", "STDDEV_SAMP")

  // See https://dev.mysql.com/doc/refman/8.0/en/aggregate-functions.html
  private val supportedAggregateFunctions =
    Set("MAX", "MIN", "SUM", "COUNT", "AVG") ++ distinctUnsupportedAggregateFunctions
  private val supportedFunctions = supportedAggregateFunctions ++ Set("DATE_ADD", "DATE_DIFF")

  override def isSupportedFunction(funcName: String): Boolean =
    supportedFunctions.contains(funcName)

  class MySQLSQLBuilder extends JDBCSQLBuilder {

    override def visitExtract(extract: Extract): String = {
      val field = extract.field
      field match {
        case "DAY_OF_YEAR" => s"DAYOFYEAR(${build(extract.source())})"
        case "WEEK" => s"WEEKOFYEAR(${build(extract.source())})"
        // MySQL does not support the date field YEAR_OF_WEEK.
        // We can't push down SECOND due to the difference in result types between Spark and
        // MySQL. Spark returns decimal(8, 6), but MySQL returns integer.
        case "YEAR_OF_WEEK" | "SECOND" =>
          visitUnexpectedExpr(extract)
        // WEEKDAY uses Monday = 0, Tuesday = 1, ... and ISO standard is Monday = 1, ...,
        // so we use the formula (WEEKDAY + 1) to follow the ISO standard.
        case "DAY_OF_WEEK" => s"(WEEKDAY(${build(extract.source())}) + 1)"
        // MINUTE, HOUR, DAY, MONTH, QUARTER, YEAR are identical on MySQL and Spark for
        // both datetime and interval types.
        case _ => super.visitExtract(field, build(extract.source()))
      }
    }

    override def visitSQLFunction(funcName: String, inputs: Array[String]): String = {
      funcName match {
        case "DATE_ADD" =>
          s"DATE_ADD(${inputs(0)}, INTERVAL ${inputs(1)} DAY)"
        case "DATE_DIFF" =>
          s"DATEDIFF(${inputs(0)}, ${inputs(1)})"
        case _ => super.visitSQLFunction(funcName, inputs)
      }
    }

    override def visitSortOrder(
        sortKey: String, sortDirection: SortDirection, nullOrdering: NullOrdering): String = {
      (sortDirection, nullOrdering) match {
        case (SortDirection.ASCENDING, NullOrdering.NULLS_FIRST) =>
          s"$sortKey $sortDirection"
        case (SortDirection.ASCENDING, NullOrdering.NULLS_LAST) =>
          s"CASE WHEN $sortKey IS NULL THEN 1 ELSE 0 END, $sortKey $sortDirection"
        case (SortDirection.DESCENDING, NullOrdering.NULLS_FIRST) =>
          s"CASE WHEN $sortKey IS NULL THEN 0 ELSE 1 END, $sortKey $sortDirection"
        case (SortDirection.DESCENDING, NullOrdering.NULLS_LAST) =>
          s"$sortKey $sortDirection"
      }
    }

    override def visitStartsWith(l: String, r: String): String = {
      val value = r.substring(1, r.length() - 1)
      s"$l LIKE '${escapeSpecialCharsForLikePattern(value)}%' ESCAPE '\\\\'"
    }

    override def visitEndsWith(l: String, r: String): String = {
      val value = r.substring(1, r.length() - 1)
      s"$l LIKE '%${escapeSpecialCharsForLikePattern(value)}' ESCAPE '\\\\'"
    }

    override def visitContains(l: String, r: String): String = {
      val value = r.substring(1, r.length() - 1)
      s"$l LIKE '%${escapeSpecialCharsForLikePattern(value)}%' ESCAPE '\\\\'"
    }

    override def visitAggregateFunction(
        funcName: String, isDistinct: Boolean, inputs: Array[String]): String =
      if (isDistinct && distinctUnsupportedAggregateFunctions.contains(funcName)) {
        throw new SparkUnsupportedOperationException(
          errorClass = "_LEGACY_ERROR_TEMP_3184",
          messageParameters = Map(
            "class" -> this.getClass.getSimpleName,
            "funcName" -> funcName))
      } else {
        super.visitAggregateFunction(funcName, isDistinct, inputs)
      }
  }

  override def compileExpression(expr: Expression): Option[String] = {
    val mysqlSQLBuilder = new MySQLSQLBuilder()
    try {
      Some(mysqlSQLBuilder.build(expr))
    } catch {
      case NonFatal(e) =>
        logWarning("Error occurs while compiling V2 expression", e)
        None
    }
  }

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    def getCatalystTypeForBitArray: Option[DataType] = {
      md.putLong("binarylong", 1)
      if (conf.legacyMySqlBitArrayMappingEnabled) {
        Some(LongType)
      } else {
        Some(BinaryType)
      }
    }
    sqlType match {
      case Types.VARBINARY if "BIT".equalsIgnoreCase(typeName) && size != 1 =>
        // MariaDB connector behaviour
        getCatalystTypeForBitArray
      case Types.BIT if size > 1 =>
        // MySQL connector behaviour
        getCatalystTypeForBitArray
      case Types.VARCHAR if "TINYTEXT".equalsIgnoreCase(typeName) =>
        // TINYTEXT is Types.VARCHAR(63) from mysql jdbc, but keep it AS-IS for historical reason
        Some(StringType)
      case Types.VARCHAR | Types.CHAR if "JSON".equalsIgnoreCase(typeName) =>
        // scalastyle:off line.size.limit
        // Some MySQL JDBC drivers convert JSON type into Types.VARCHAR(-1) or Types.CHAR(Int.Max).
        // MySQL Connector/J 5.x as an example:
        // https://github.com/mysql/mysql-connector-j/blob/release/5.1/src/com/mysql/jdbc/MysqlDefs.java#L295
        // Explicitly converts it into StringType here.
        // scalastyle:on line.size.limit
        Some(StringType)
      case Types.TINYINT =>
        if (md.build().getBoolean("isSigned")) {
          Some(ByteType)
        } else {
          Some(ShortType)
        }
      case Types.SMALLINT =>
        if (md.build().getBoolean("isSigned")) {
          Some(ShortType)
        } else {
          Some(IntegerType)
        }
      case Types.INTEGER if "MEDIUMINT UNSIGNED".equalsIgnoreCase(typeName) =>
        // Signed values in [-8388608, 8388607] and unsigned values in [0, 16777215],
        // both of them fit IntegerType
        Some(IntegerType)
      case Types.REAL | Types.FLOAT =>
        if (md.build().getBoolean("isSigned")) Some(FloatType) else Some(DoubleType)
      case Types.TIMESTAMP if "DATETIME".equalsIgnoreCase(typeName) =>
        // scalastyle:off line.size.limit
        // In MYSQL, DATETIME is TIMESTAMP WITHOUT TIME ZONE
        // https://github.com/mysql/mysql-connector-j/blob/8.3.0/src/main/core-api/java/com/mysql/cj/MysqlType.java#L251
        // scalastyle:on line.size.limit
        Some(getTimestampType(md.build()))
      case Types.TIMESTAMP if !conf.legacyMySqlTimestampNTZMappingEnabled => Some(TimestampType)
      case _ => None
    }
  }

  override def quoteIdentifier(colName: String): String = {
    s"`$colName`"
  }

  override def schemasExists(conn: Connection, options: JDBCOptions, schema: String): Boolean = {
    listSchemas(conn, options).exists(_.head == schema)
  }

  override def listSchemas(conn: Connection, options: JDBCOptions): Array[Array[String]] = {
    val schemaBuilder = ArrayBuilder.make[Array[String]]
    try {
      JdbcUtils.executeQuery(conn, options, "SHOW SCHEMAS") { rs =>
        while (rs.next()) {
          schemaBuilder += Array(rs.getString("Database"))
        }
      }
    } catch {
      case _: Exception =>
        logWarning("Cannot show schemas.")
    }
    schemaBuilder.result()
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
    case StringType => Option(JdbcType("LONGTEXT", java.sql.Types.LONGVARCHAR))
    case ByteType => Option(JdbcType("TINYINT", java.sql.Types.TINYINT))
    case ShortType => Option(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    // scalastyle:off line.size.limit
    // In MYSQL, DATETIME is TIMESTAMP WITHOUT TIME ZONE
    // https://github.com/mysql/mysql-connector-j/blob/8.3.0/src/main/core-api/java/com/mysql/cj/MysqlType.java#L251
    // scalastyle:on line.size.limit
    case TimestampNTZType if !conf.legacyMySqlTimestampNTZMappingEnabled =>
      Option(JdbcType("DATETIME", java.sql.Types.TIMESTAMP))
    case _ => JdbcUtils.getCommonJDBCType(dt)
  }

  override def getSchemaCommentQuery(schema: String, comment: String): String = {
    throw QueryExecutionErrors.unsupportedCommentNamespaceError(schema)
  }

  override def removeSchemaCommentQuery(schema: String): String = {
    throw QueryExecutionErrors.unsupportedRemoveNamespaceCommentError(schema)
  }

  // CREATE INDEX syntax
  // https://dev.mysql.com/doc/refman/8.0/en/create-index.html
  override def createIndex(
      indexName: String,
      tableIdent: Identifier,
      columns: Array[NamedReference],
      columnsProperties: util.Map[NamedReference, util.Map[String, String]],
      properties: util.Map[String, String]): String = {
    val columnList = columns.map(col => quoteIdentifier(col.fieldNames.head))
    val (indexType, indexPropertyList) = JdbcUtils.processIndexProperties(properties, "mysql")

    // columnsProperties doesn't apply to MySQL so it is ignored
    s"CREATE INDEX ${quoteIdentifier(indexName)} $indexType ON" +
      s" ${quoteIdentifier(tableIdent.name())} (${columnList.mkString(", ")})" +
      s" ${indexPropertyList.mkString(" ")}"
  }

  // SHOW INDEX syntax
  // https://dev.mysql.com/doc/refman/8.0/en/show-index.html
  override def indexExists(
      conn: Connection,
      indexName: String,
      tableIdent: Identifier,
      options: JDBCOptions): Boolean = {
    val sql = s"SHOW INDEXES FROM ${quoteIdentifier(tableIdent.name())} " +
      s"WHERE key_name = '$indexName'"
    JdbcUtils.checkIfIndexExists(conn, sql, options)
  }

  override def dropIndex(indexName: String, tableIdent: Identifier): String = {
    s"DROP INDEX ${quoteIdentifier(indexName)} ON ${tableIdent.name()}"
  }

  // SHOW INDEX syntax
  // https://dev.mysql.com/doc/refman/8.0/en/show-index.html
  override def listIndexes(
      conn: Connection,
      tableIdent: Identifier,
      options: JDBCOptions): Array[TableIndex] = {
    val sql = s"SHOW INDEXES FROM ${tableIdent.name()}"
    var indexMap: Map[String, TableIndex] = Map()
    try {
      JdbcUtils.executeQuery(conn, options, sql) { rs =>
        while (rs.next()) {
          val indexName = rs.getString("key_name")
          val colName = rs.getString("column_name")
          val indexType = rs.getString("index_type")
          val indexComment = rs.getString("index_comment")
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
      }
    } catch {
      case _: Exception =>
        logWarning("Cannot retrieved index info.")
    }
    indexMap.values.toArray
  }

  override def classifyException(
      e: Throwable,
      condition: String,
      messageParameters: Map[String, String],
      description: String,
      isRuntime: Boolean): Throwable with SparkThrowable = {
    e match {
      case sqlException: SQLException =>
        sqlException.getErrorCode match {
          // ER_DUP_KEYNAME
          case 1050 if condition == "FAILED_JDBC.RENAME_TABLE" =>
            val newTable = messageParameters("newName")
            throw QueryCompilationErrors.tableAlreadyExistsError(newTable)
          case 1061 if condition == "FAILED_JDBC.CREATE_INDEX" =>
            val indexName = messageParameters("indexName")
            val tableName = messageParameters("tableName")
            throw new IndexAlreadyExistsException(indexName, tableName, cause = Some(e))
          case 1091 if condition == "FAILED_JDBC.DROP_INDEX" =>
            val indexName = messageParameters("indexName")
            val tableName = messageParameters("tableName")
            throw new NoSuchIndexException(indexName, tableName, cause = Some(e))
          case _ =>
            super.classifyException(e, condition, messageParameters, description, isRuntime)
        }
      case unsupported: UnsupportedOperationException => throw unsupported
      case _ => super.classifyException(e, condition, messageParameters, description, isRuntime)
    }
  }

  override def dropSchema(schema: String, cascade: Boolean): String = {
    if (cascade) {
      s"DROP SCHEMA ${quoteIdentifier(schema)}"
    } else {
      throw QueryExecutionErrors.unsupportedDropNamespaceError(schema)
    }
  }

  class MySQLSQLQueryBuilder(dialect: JdbcDialect, options: JDBCOptions)
    extends JdbcSQLQueryBuilder(dialect, options) {

    override def build(): String = {
      val limitOrOffsetStmt = if (limit > 0) {
        if (offset > 0) {
          s"LIMIT $offset, $limit"
        } else {
          dialect.getLimitClause(limit)
        }
      } else if (offset > 0) {
        // MySQL doesn't support OFFSET without LIMIT. According to the suggestion of MySQL
        // official website, in order to retrieve all rows from a certain offset up to the end of
        // the result set, you can use some large number for the second parameter. Please refer:
        // https://dev.mysql.com/doc/refman/8.0/en/select.html
        s"LIMIT $offset, 18446744073709551615"
      } else {
        ""
      }

      options.prepareQuery +
        s"SELECT $hintClause$columnList FROM ${options.tableOrQuery} $tableSampleClause" +
        s" $whereClause $groupByClause $orderByClause $limitOrOffsetStmt"
    }
  }

  override def getJdbcSQLQueryBuilder(options: JDBCOptions): JdbcSQLQueryBuilder =
    new MySQLSQLQueryBuilder(this, options)

  override def supportsLimit: Boolean = true

  override def supportsOffset: Boolean = true

  override def supportsHint: Boolean = true
}
