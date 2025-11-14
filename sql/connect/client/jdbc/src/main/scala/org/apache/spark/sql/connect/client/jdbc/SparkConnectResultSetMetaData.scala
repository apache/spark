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

package org.apache.spark.sql.connect.client.jdbc

import java.sql.{Array => _, _}
import java.sql.ResultSetMetaData.{columnNoNulls, columnNullable}

import org.apache.spark.sql.connect.client.jdbc.util.JdbcTypeUtils
import org.apache.spark.sql.types._

class SparkConnectResultSetMetaData(schema: StructType) extends ResultSetMetaData {

  override def getColumnCount: Int = schema.length

  override def isAutoIncrement(column: Int): Boolean = false

  override def isCaseSensitive(column: Int): Boolean = false

  override def isSearchable(column: Int): Boolean = false

  override def isCurrency(column: Int): Boolean = false

  override def isNullable(column: Int): Int =
    if (schema(column - 1).nullable) columnNullable else columnNoNulls

  override def isSigned(column: Int): Boolean =
    JdbcTypeUtils.isSigned(schema(column - 1))

  override def getColumnDisplaySize(column: Int): Int =
    JdbcTypeUtils.getDisplaySize(schema(column - 1))

  override def getColumnLabel(column: Int): String = getColumnName(column)

  override def getColumnName(column: Int): String = schema(column - 1).name

  override def getColumnType(column: Int): Int =
    JdbcTypeUtils.getColumnType(schema(column - 1))

  override def getColumnTypeName(column: Int): String = schema(column - 1).dataType.sql

  override def getColumnClassName(column: Int): String =
    JdbcTypeUtils.getColumnTypeClassName(schema(column - 1))

  override def getPrecision(column: Int): Int =
    JdbcTypeUtils.getPrecision(schema(column - 1))

  override def getScale(column: Int): Int =
    JdbcTypeUtils.getScale(schema(column - 1))

  override def getCatalogName(column: Int): String = ""

  override def getSchemaName(column: Int): String = ""

  override def getTableName(column: Int): String = ""

  override def isReadOnly(column: Int): Boolean = true

  override def isWritable(column: Int): Boolean = false

  override def isDefinitelyWritable(column: Int): Boolean = false

  override def unwrap[T](iface: Class[T]): T = if (isWrapperFor(iface)) {
    iface.asInstanceOf[T]
  } else {
    throw new SQLException(s"${this.getClass.getName} not unwrappable from ${iface.getName}")
  }

  override def isWrapperFor(iface: Class[_]): Boolean = iface.isInstance(this)
}
