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

import java.sql.{Connection, PreparedStatement, Types}

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types._


private object PostgresDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:postgresql")

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.REAL) {
      Some(FloatType)
    } else if (sqlType == Types.SMALLINT) {
      Some(ShortType)
    } else if (sqlType == Types.BIT && typeName.equals("bit") && size != 1) {
      Some(BinaryType)
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
    case "money" | "float8" => Some(DoubleType)
    case "text" | "varchar" | "char" | "cidr" | "inet" | "json" | "jsonb" | "uuid" =>
      Some(StringType)
    case "bytea" => Some(BinaryType)
    case "timestamp" | "timestamptz" | "time" | "timetz" => Some(TimestampType)
    case "date" => Some(DateType)
    case "numeric" | "decimal" => Some(DecimalType.bounded(precision, scale))
    case _ => None
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("TEXT", Types.CHAR))
    case BinaryType => Some(JdbcType("BYTEA", Types.BINARY))
    case BooleanType => Some(JdbcType("BOOLEAN", Types.BOOLEAN))
    case FloatType => Some(JdbcType("FLOAT4", Types.FLOAT))
    case DoubleType => Some(JdbcType("FLOAT8", Types.DOUBLE))
    case ShortType => Some(JdbcType("SMALLINT", Types.SMALLINT))
    case t: DecimalType => Some(
      JdbcType(s"NUMERIC(${t.precision},${t.scale})", java.sql.Types.NUMERIC))
    case ArrayType(et, _) if et.isInstanceOf[AtomicType] =>
      getJDBCType(et).map(_.databaseTypeDefinition)
        .orElse(JdbcUtils.getCommonJDBCType(et).map(_.databaseTypeDefinition))
        .map(typeName => JdbcType(s"$typeName[]", java.sql.Types.ARRAY))
    case ByteType => throw new IllegalArgumentException(s"Unsupported type in postgresql: $dt");
    case _ => None
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
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

  override def isCascadingTruncateTable(): Option[Boolean] = Some(true)

  /**
   * Returns a PreparedStatement that does Insert/Update table
   */
  override def upsertStatement(
      conn: Connection,
      table: String,
      rddSchema: StructType,
      upsertParam: UpsertInfo =
      UpsertInfo(Array.empty[String], Array.empty[String])): PreparedStatement = {
    require(upsertParam.upsertConditionColumns.nonEmpty,
      "Upsert option requires column names on which duplicate rows are identified. " +
        "Please specify option(\"upsertConditionColumn\", \"c1, c2, ...\")")
    require(conn.getMetaData.getDatabaseProductVersion.compareToIgnoreCase("9.5") > 0,
      "INSERT INTO with ON CONFLICT clause only support by PostgreSQL 9.5 and up.")

    val insertColumns = rddSchema.fields.map(_.name).mkString(", ")
    val conflictTarget = upsertParam.upsertConditionColumns.mkString(", ")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    val updateColumns = if (upsertParam.upsertUpdateColumns.nonEmpty)
    { upsertParam.upsertUpdateColumns} else {rddSchema.fields.map(_.name)}
    val updateClause = updateColumns
      .filterNot(upsertParam.upsertConditionColumns.contains(_))
      .map(x => s"$x = EXCLUDED.$x").mkString(", ")

    // In the case where condition columns are the whole set of the rddSchema columns
    // and rddSchema columns may be a subset of the target table schema.
    // We need to do nothing for matched rows
    val sql = if (updateClause != null && updateClause.nonEmpty) {
      s"""
         |INSERT INTO $table ($insertColumns)
         |VALUES ( $placeholders )
         |ON CONFLICT ($conflictTarget)
         |DO UPDATE SET $updateClause
      """.stripMargin
    } else {
      s"""
         |INSERT INTO $table ($insertColumns)
         |VALUES ( $placeholders )
         |ON CONFLICT ($conflictTarget)
         |DO NOTHING
      """.stripMargin
    }
    conn.prepareStatement(sql)
  }
}
