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

import org.apache.spark.sql.types.{BooleanType, DataType, LongType, MetadataBuilder, StructType}

private case object MySQLDialect extends JdbcDialect {

  override def canHandle(url : String): Boolean = url.startsWith("jdbc:mysql")

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

  /**
   * Returns a PreparedStatement that does Insert/Update table
   */
  override def upsertStatement(
      conn: Connection,
      table: String,
      rddSchema: StructType,
      upsertParam: UpsertInfo = UpsertInfo(Array.empty, Array.empty)): PreparedStatement = {
    require(upsertParam.upsertUpdateColumns.nonEmpty,
      "Upsert option requires update column names." +
        "Please specify option(\"upsertUpdateColumn\", \"c1, c2, ...\")")


    val updateClause = if (upsertParam.upsertUpdateColumns.nonEmpty) {
      upsertParam.upsertUpdateColumns.map(x => s"$x = VALUES($x)").mkString(", ")
    }
    else {
      rddSchema.fields.map(x => s"$x = VALUES($x)").mkString(", ")
    }
    val insertColumns = rddSchema.fields.map(_.name).mkString(", ")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")

    val sql =
      s"""
         |INSERT INTO $table ($insertColumns)
         |VALUES ( $placeholders )
         |ON DUPLICATE KEY UPDATE $updateClause
      """.stripMargin
    conn.prepareStatement(sql)
  }
}
