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

import java.sql.Types

import org.apache.spark.sql.types._


private object PostgresDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:postgresql")

  override def getCatalystType(sqlType: Int, typeName: String, size: Int, scale: Int,
                               md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.BIT && typeName.equals("bit") && size != 1) {
      Option(BinaryType)
    } else if (sqlType == Types.OTHER && typeName.equals("cidr")) {
      Option(StringType)
    } else if (sqlType == Types.OTHER && typeName.equals("inet")) {
      Option(StringType)
    } else if (sqlType == Types.OTHER && typeName.equals("json")) {
      Option(StringType)
    } else if (sqlType == Types.OTHER && typeName.equals("jsonb")) {
      Option(StringType)
    } else if (sqlType == Types.OTHER && typeName.equals("uuid")) {
      Some(StringType)
    } else if (sqlType == Types.ARRAY) {
      typeName match {
        case "_bit" | "_bool" => Option(ArrayType(BooleanType))
        case "_int2" => Option(ArrayType(ShortType))
        case "_int4" => Option(ArrayType(IntegerType))
        case "_int8" | "_oid" => Option(ArrayType(LongType))
        case "_float4" => Option(ArrayType(FloatType))
        case "_money" | "_float8" => Option(ArrayType(DoubleType))
        case "_text" | "_varchar" | "_char" | "_bpchar" | "_name" => Option(ArrayType(StringType))
        case "_bytea" => Option(ArrayType(BinaryType))
        case "_timestamp" | "_timestamptz" | "_time" | "_timetz" => Option(ArrayType(TimestampType))
        case "_date" => Option(ArrayType(DateType))
        case "_numeric"
          if size != 0 || scale != 0 => Option(ArrayType(DecimalType(size, scale)))
        case "_numeric" => Option(ArrayType(DecimalType.SYSTEM_DEFAULT))
        case _ => throw new IllegalArgumentException(s"Unhandled postgres array type $typeName")
      }
    } else None
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("TEXT", java.sql.Types.CHAR))
    case BinaryType => Some(JdbcType("BYTEA", java.sql.Types.BINARY))
    case BooleanType => Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case ArrayType(t, _) =>
      val subtype = getJDBCType(t).map(_.databaseTypeDefinition).getOrElse(
        getCommonJDBCType(t).map(_.databaseTypeDefinition).getOrElse(
          throw new IllegalArgumentException(s"Unexpected JDBC array subtype $t")
        )
      )
      Some(JdbcType(s"$subtype[]", java.sql.Types.ARRAY))
    case _ => None
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }
}
