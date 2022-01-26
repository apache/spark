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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{IndexAlreadyExistsException, NoSuchIndexException}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, GeneralAggregateFunc}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.types._


private object PostgresDialect extends JdbcDialect with SQLConfHelper {

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:postgresql")

  override def compileAggregate(aggFunction: AggregateFunc): Option[String] = {
    super.compileAggregate(aggFunction).orElse(
      aggFunction match {
        case f: GeneralAggregateFunc if f.name() == "VAR_POP" =>
          assert(f.inputs().length == 1)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"VAR_POP($distinct${f.inputs().head})")
        case f: GeneralAggregateFunc if f.name() == "VAR_SAMP" =>
          assert(f.inputs().length == 1)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"VAR_SAMP($distinct${f.inputs().head})")
        case f: GeneralAggregateFunc if f.name() == "STDDEV_POP" =>
          assert(f.inputs().length == 1)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"STDDEV_POP($distinct${f.inputs().head})")
        case f: GeneralAggregateFunc if f.name() == "STDDEV_SAMP" =>
          assert(f.inputs().length == 1)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"STDDEV_SAMP($distinct${f.inputs().head})")
        case f: GeneralAggregateFunc if f.name() == "COVAR_POP" =>
          assert(f.inputs().length == 2)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"COVAR_POP($distinct${f.inputs().head}, ${f.inputs().last})")
        case f: GeneralAggregateFunc if f.name() == "COVAR_SAMP" =>
          assert(f.inputs().length == 2)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"COVAR_SAMP($distinct${f.inputs().head}, ${f.inputs().last})")
        case f: GeneralAggregateFunc if f.name() == "CORR" =>
          assert(f.inputs().length == 2)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"CORR($distinct${f.inputs().head}, ${f.inputs().last})")
        case _ => None
      }
    )
  }

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
    case "text" | "varchar" | "char" | "bpchar" | "cidr" | "inet" | "json" | "jsonb" | "uuid" |
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
      Some(DecimalType. SYSTEM_DEFAULT)
    case "money" =>
      // money[] type seems to be broken and difficult to handle.
      // So this method returns None for now.
      // See SPARK-34333 and https://github.com/pgjdbc/pgjdbc/issues/1405
      None
    case _ => None
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("TEXT", Types.CHAR))
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

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
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

  override def supportsTableSample: Boolean = true

  override def getTableSample(sample: TableSampleInfo): String = {
    // hard-coded to BERNOULLI for now because Spark doesn't have a way to specify sample
    // method name
    s"TABLESAMPLE BERNOULLI" +
      s" (${(sample.upperBound - sample.lowerBound) * 100}) REPEATABLE (${sample.seed})"
  }

  // CREATE INDEX syntax
  // https://www.postgresql.org/docs/14/sql-createindex.html
  override def createIndex(
      indexName: String,
      tableName: String,
      columns: Array[NamedReference],
      columnsProperties: util.Map[NamedReference, util.Map[String, String]],
      properties: util.Map[String, String]): String = {
    val columnList = columns.map(col => quoteIdentifier(col.fieldNames.head))
    var indexProperties = ""
    val (indexType, indexPropertyList) = JdbcUtils.processIndexProperties(properties, "postgresql")

    if (indexPropertyList.nonEmpty) {
      indexProperties = "WITH (" + indexPropertyList.mkString(", ") + ")"
    }

    s"CREATE INDEX ${quoteIdentifier(indexName)} ON ${quoteIdentifier(tableName)}" +
      s" $indexType (${columnList.mkString(", ")}) $indexProperties"
  }

  // SHOW INDEX syntax
  // https://www.postgresql.org/docs/14/view-pg-indexes.html
  override def indexExists(
      conn: Connection,
      indexName: String,
      tableName: String,
      options: JDBCOptions): Boolean = {
    val sql = s"SELECT * FROM pg_indexes WHERE tablename = '$tableName' AND" +
      s" indexname = '$indexName'"
    JdbcUtils.checkIfIndexExists(conn, sql, options)
  }

  // DROP INDEX syntax
  // https://www.postgresql.org/docs/14/sql-dropindex.html
  override def dropIndex(indexName: String, tableName: String): String = {
    s"DROP INDEX ${quoteIdentifier(indexName)}"
  }

  override def classifyException(message: String, e: Throwable): AnalysisException = {
    e match {
      case sqlException: SQLException =>
        sqlException.getSQLState match {
          // https://www.postgresql.org/docs/14/errcodes-appendix.html
          case "42P07" => throw new IndexAlreadyExistsException(message, cause = Some(e))
          case "42704" => throw new NoSuchIndexException(message, cause = Some(e))
          case _ => super.classifyException(message, e)
        }
      case unsupported: UnsupportedOperationException => throw unsupported
      case _ => super.classifyException(message, e)
    }
  }
}
