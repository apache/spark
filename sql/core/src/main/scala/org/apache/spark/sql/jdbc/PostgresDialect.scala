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

import java.sql.{Connection, SQLException, Timestamp, Types}
import java.time.{LocalDateTime, ZoneOffset}
import java.util
import java.util.Locale

import scala.util.control.NonFatal

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{IndexAlreadyExistsException, NonEmptyNamespaceException, NoSuchIndexException}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.{Expression, NamedReference}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.types._


private object PostgresDialect extends JdbcDialect with SQLConfHelper {

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:postgresql")

  // See https://www.postgresql.org/docs/8.4/functions-aggregate.html
  private val supportedAggregateFunctions = Set("MAX", "MIN", "SUM", "COUNT", "AVG",
    "VAR_POP", "VAR_SAMP", "STDDEV_POP", "STDDEV_SAMP", "COVAR_POP", "COVAR_SAMP", "CORR",
    "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXY")
  private val supportedFunctions = supportedAggregateFunctions

  override def isSupportedFunction(funcName: String): Boolean =
    supportedFunctions.contains(funcName)

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.REAL) {
      Some(FloatType)
    } else if (sqlType == Types.SMALLINT) {
      Some(ShortType)
    } else if (sqlType == Types.BIT && typeName == "bit" && size != 1) {
      Some(BinaryType)
    } else if (sqlType == Types.DOUBLE && typeName == "money") {
      // money type seems to be broken but one workaround is to handle it as string.
      // See SPARK-34333 and https://github.com/pgjdbc/pgjdbc/issues/100
      Some(StringType)
    } else if (sqlType == Types.OTHER) {
      Some(StringType)
    } else if ("text".equalsIgnoreCase(typeName)) {
      Some(StringType) // sqlType is  Types.VARCHAR
    } else if (sqlType == Types.ARRAY) {
      val scale = md.build.getLong("scale").toInt
      // postgres array type names start with underscore
      toCatalystType(typeName.drop(1), size, scale).map(ArrayType(_))
    } else None
  }

  private def toCatalystType(
      typeName: String,
      precision: Int,
      scale: Int): Option[DataType] = typeName match {
    case "bool" => Some(BooleanType)
    case "bit" => Some(BinaryType)
    case "int2" => Some(ShortType)
    case "int4" => Some(IntegerType)
    case "int8" | "oid" => Some(LongType)
    case "float4" => Some(FloatType)
    case "float8" => Some(DoubleType)
    case "varchar" => Some(VarcharType(precision))
    case "char" | "bpchar" => Some(CharType(precision))
    case "text" | "cidr" | "inet" | "json" | "jsonb" | "uuid" |
         "xml" | "tsvector" | "tsquery" | "macaddr" | "macaddr8" | "txid_snapshot" | "point" |
         "line" | "lseg" | "box" | "path" | "polygon" | "circle" | "pg_lsn" | "varbit" |
         "interval" | "pg_snapshot" =>
      Some(StringType)
    case "bytea" => Some(BinaryType)
    case "timestamp" | "timestamptz" | "time" | "timetz" => Some(TimestampType)
    case "date" => Some(DateType)
    case "numeric" | "decimal" if precision > 0 => Some(DecimalType.bounded(precision, scale))
    case "numeric" | "decimal" =>
      // SPARK-26538: handle numeric without explicit precision and scale.
      Some(DecimalType.SYSTEM_DEFAULT)
    case "money" =>
      // money[] type seems to be broken and difficult to handle.
      // So this method returns None for now.
      // See SPARK-34333 and https://github.com/pgjdbc/pgjdbc/issues/1405
      None
    case _ =>
      // SPARK-43267: handle unknown types in array as string, because there are user-defined types
      Some(StringType)
  }

  override def convertJavaTimestampToTimestampNTZ(t: Timestamp): LocalDateTime = {
    t.toLocalDateTime
  }

  override def convertTimestampNTZToJavaTimestamp(ldt: LocalDateTime): Timestamp = {
    Timestamp.valueOf(ldt)
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("TEXT", Types.VARCHAR))
    case BinaryType => Some(JdbcType("BYTEA", Types.BINARY))
    case BooleanType => Some(JdbcType("BOOLEAN", Types.BOOLEAN))
    case FloatType => Some(JdbcType("FLOAT4", Types.FLOAT))
    case DoubleType => Some(JdbcType("FLOAT8", Types.DOUBLE))
    case ShortType | ByteType => Some(JdbcType("SMALLINT", Types.SMALLINT))
    case t: DecimalType => Some(
      JdbcType(s"NUMERIC(${t.precision},${t.scale})", java.sql.Types.NUMERIC))
    case ArrayType(et, _) if et.isInstanceOf[AtomicType] =>
      getJDBCType(et).map(_.databaseTypeDefinition)
        .orElse(JdbcUtils.getCommonJDBCType(et).map(_.databaseTypeDefinition))
        .map(typeName => JdbcType(s"$typeName[]", java.sql.Types.ARRAY))
    case _ => None
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

  /**
   * The SQL query used to truncate a table. For Postgres, the default behaviour is to
   * also truncate any descendant tables. As this is a (possibly unwanted) side-effect,
   * the Postgres dialect adds 'ONLY' to truncate only the table in question
   * @param table The table to truncate
   * @param cascade Whether or not to cascade the truncation. Default value is the value of
   *                isCascadingTruncateTable(). Cascading a truncation will truncate tables
    *               with a foreign key relationship to the target table. However, it will not
    *               truncate tables with an inheritance relationship to the target table, as
    *               the truncate query always includes "ONLY" to prevent this behaviour.
   * @return The SQL query to use for truncating a table
   */
  override def getTruncateQuery(
      table: String,
      cascade: Option[Boolean] = isCascadingTruncateTable): String = {
    cascade match {
      case Some(true) => s"TRUNCATE TABLE ONLY $table CASCADE"
      case _ => s"TRUNCATE TABLE ONLY $table"
    }
  }

  override def beforeFetch(connection: Connection, properties: Map[String, String]): Unit = {
    super.beforeFetch(connection, properties)

    // According to the postgres jdbc documentation we need to be in autocommit=false if we actually
    // want to have fetchsize be non 0 (all the rows).  This allows us to not have to cache all the
    // rows inside the driver when fetching.
    //
    // See: https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
    //
    if (properties.getOrElse(JDBCOptions.JDBC_BATCH_FETCH_SIZE, "0").toInt > 0) {
      connection.setAutoCommit(false)
    }
  }

  // See https://www.postgresql.org/docs/12/sql-altertable.html
  override def getUpdateColumnTypeQuery(
      tableName: String,
      columnName: String,
      newDataType: String): String = {
    s"ALTER TABLE $tableName ALTER COLUMN ${quoteIdentifier(columnName)} TYPE $newDataType"
  }

  // See https://www.postgresql.org/docs/12/sql-altertable.html
  override def getUpdateColumnNullabilityQuery(
      tableName: String,
      columnName: String,
      isNullable: Boolean): String = {
    val nullable = if (isNullable) "DROP NOT NULL" else "SET NOT NULL"
    s"ALTER TABLE $tableName ALTER COLUMN ${quoteIdentifier(columnName)} $nullable"
  }

  // CREATE INDEX syntax
  // https://www.postgresql.org/docs/14/sql-createindex.html
  override def createIndex(
      indexName: String,
      tableIdent: Identifier,
      columns: Array[NamedReference],
      columnsProperties: util.Map[NamedReference, util.Map[String, String]],
      properties: util.Map[String, String]): String = {
    val columnList = columns.map(col => quoteIdentifier(col.fieldNames.head))
    var indexProperties = ""
    val (indexType, indexPropertyList) = JdbcUtils.processIndexProperties(properties, "postgresql")

    if (indexPropertyList.nonEmpty) {
      indexProperties = "WITH (" + indexPropertyList.mkString(", ") + ")"
    }

    s"CREATE INDEX ${quoteIdentifier(indexName)} ON ${quoteIdentifier(tableIdent.name())}" +
      s" $indexType (${columnList.mkString(", ")}) $indexProperties"
  }

  // SHOW INDEX syntax
  // https://www.postgresql.org/docs/14/view-pg-indexes.html
  override def indexExists(
      conn: Connection,
      indexName: String,
      tableIdent: Identifier,
      options: JDBCOptions): Boolean = {
    val sql = s"SELECT * FROM pg_indexes WHERE tablename = '${tableIdent.name()}' AND" +
      s" indexname = '$indexName'"
    JdbcUtils.checkIfIndexExists(conn, sql, options)
  }

  // DROP INDEX syntax
  // https://www.postgresql.org/docs/14/sql-dropindex.html
  override def dropIndex(indexName: String, tableIdent: Identifier): String = {
    s"DROP INDEX ${quoteIdentifier(indexName)}"
  }

  override def classifyException(message: String, e: Throwable): AnalysisException = {
    e match {
      case sqlException: SQLException =>
        sqlException.getSQLState match {
          // https://www.postgresql.org/docs/14/errcodes-appendix.html
          case "42P07" =>
            // Message patterns defined at caller sides of spark
            val indexRegex = "(?s)Failed to create index (.*) in (.*)".r
            val renameRegex = "(?s)Failed table renaming from (.*) to (.*)".r
            // Message pattern defined by postgres specification
            val pgRegex = """(?:.*)relation "(.*)" already exists""".r

            message match {
              case indexRegex(index, table) =>
                throw new IndexAlreadyExistsException(
                  indexName = index, tableName = table, cause = Some(e))
              case renameRegex(_, newTable) =>
                throw QueryCompilationErrors.tableAlreadyExistsError(newTable)
              case _ if pgRegex.findFirstMatchIn(sqlException.getMessage).nonEmpty =>
                val tableName = pgRegex.findFirstMatchIn(sqlException.getMessage).get.group(1)
                throw QueryCompilationErrors.tableAlreadyExistsError(tableName)
              case _ => super.classifyException(message, e)
            }
          case "42704" =>
            // The message is: Failed to drop index indexName in tableName
            val regex = "(?s)Failed to drop index (.*) in (.*)".r
            val indexName = regex.findFirstMatchIn(message).get.group(1)
            val tableName = regex.findFirstMatchIn(message).get.group(2)
            throw new NoSuchIndexException(indexName, tableName, cause = Some(e))
          case "2BP01" => throw NonEmptyNamespaceException(message, cause = Some(e))
          case _ => super.classifyException(message, e)
        }
      case unsupported: UnsupportedOperationException => throw unsupported
      case _ => super.classifyException(message, e)
    }
  }

  class PostgresSQLBuilder extends JDBCSQLBuilder {
    override def visitBinaryArithmetic(name: String, l: String, r: String): String = {
      l + " " + name.replace('^', '#') + " " + r
    }
  }

  override def compileExpression(expr: Expression): Option[String] = {
    val postgresSQLBuilder = new PostgresSQLBuilder()
    try {
      Some(postgresSQLBuilder.build(expr))
    } catch {
      case NonFatal(e) =>
        logWarning("Error occurs while compiling V2 expression", e)
        None
    }
  }

  override def supportsLimit: Boolean = true

  override def supportsOffset: Boolean = true

  override def supportsTableSample: Boolean = true

  override def getTableSample(sample: TableSampleInfo): String = {
    // hard-coded to BERNOULLI for now because Spark doesn't have a way to specify sample
    // method name
    "TABLESAMPLE BERNOULLI" +
      s" (${(sample.upperBound - sample.lowerBound) * 100}) REPEATABLE (${sample.seed})"
  }

  override def renameTable(oldTable: Identifier, newTable: Identifier): String = {
    if (!oldTable.namespace().sameElements(newTable.namespace())) {
      throw QueryCompilationErrors.cannotRenameTableAcrossSchemaError()
    }
    s"ALTER TABLE ${getFullyQualifiedQuotedTableName(oldTable)} RENAME TO ${newTable.name()}"
  }

  /**
   * java.sql timestamps are measured with millisecond accuracy (from Long.MinValue
   * milliseconds to Long.MaxValue milliseconds), while Spark timestamps are measured
   * at microseconds accuracy. For the "infinity values" in PostgreSQL (represented by
   * big constants), we need clamp them to avoid overflow. If it is not one of the infinity
   * values, fall back to default behavior.
   */
  override def convertJavaTimestampToTimestamp(t: Timestamp): Timestamp = {
    // Variable names come from PostgreSQL "constant field docs":
    // https://jdbc.postgresql.org/documentation/publicapi/index.html?constant-values.html
    val POSTGRESQL_DATE_NEGATIVE_INFINITY = -9223372036832400000L
    val POSTGRESQL_DATE_NEGATIVE_SMALLER_INFINITY = -185543533774800000L
    val POSTGRESQL_DATE_POSITIVE_INFINITY = 9223372036825200000L
    val POSTGRESQL_DATE_DATE_POSITIVE_SMALLER_INFINITY = 185543533774800000L

    val minTimeStamp = LocalDateTime.of(1, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
    val maxTimestamp =
      LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999999).toInstant(ZoneOffset.UTC).toEpochMilli

    val time = t.getTime
    if (time == POSTGRESQL_DATE_POSITIVE_INFINITY ||
      time == POSTGRESQL_DATE_DATE_POSITIVE_SMALLER_INFINITY) {
      new Timestamp(maxTimestamp)
    } else if (time == POSTGRESQL_DATE_NEGATIVE_INFINITY ||
      time == POSTGRESQL_DATE_NEGATIVE_SMALLER_INFINITY) {
      new Timestamp(minTimeStamp)
    } else {
      t
    }
  }
}
