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

import org.apache.spark.sql.types._

class SparkConnectResultSetMetaData(schema: StructType) extends ResultSetMetaData {

  override def getColumnCount: Int = schema.length

  override def isAutoIncrement(column: Int): Boolean = false

  override def isCaseSensitive(column: Int): Boolean = false

  override def isSearchable(column: Int): Boolean = true

  override def isCurrency(column: Int): Boolean = false

  override def isNullable(column: Int): Int = {
    if (schema(column - 1).nullable) {
      ResultSetMetaData.columnNullable
    } else {
      ResultSetMetaData.columnNoNulls
    }
  }

  override def isSigned(column: Int): Boolean = true

  override def getColumnDisplaySize(column: Int): Int = Integer.MAX_VALUE

  override def getColumnLabel(column: Int): String = schema(column - 1).name

  override def getColumnName(column: Int): String = schema(column - 1).name

  override def getColumnType(column: Int): Int = {
    schema(column - 1).dataType match {
      case NullType => Types.NULL
      case BooleanType => Types.BOOLEAN
      case ByteType => Types.TINYINT
      case ShortType => Types.SMALLINT
      case IntegerType => Types.INTEGER
      case LongType => Types.BIGINT
      case FloatType => Types.FLOAT
      case DoubleType => Types.DOUBLE
      case DateType => throw new SQLFeatureNotSupportedException()
      case TimestampType => throw new SQLFeatureNotSupportedException()
      case TimestampNTZType => throw new SQLFeatureNotSupportedException()
      case StringType => Types.VARCHAR
      // handling CHAR/VARCHAR requires move this class from catalyst to common-utils
      //   org.apache.spark.sql.catalyst.util.CharVarcharUtils
      case c: CharType => throw new SQLFeatureNotSupportedException()
      case vc: VarcharType => throw new SQLFeatureNotSupportedException()
      case BinaryType => throw new SQLFeatureNotSupportedException()
      case dt: DecimalType => throw new SQLFeatureNotSupportedException()
      case _: YearMonthIntervalType => throw new SQLFeatureNotSupportedException()
      case _: DayTimeIntervalType => throw new SQLFeatureNotSupportedException()
      case CalendarIntervalType => throw new SQLFeatureNotSupportedException()
      case VariantType => throw new SQLFeatureNotSupportedException()
      case at: ArrayType => throw new SQLFeatureNotSupportedException()
      case st: StructType => throw new SQLFeatureNotSupportedException()
      case mt: MapType => throw new SQLFeatureNotSupportedException()
      case unknown => throw new SQLFeatureNotSupportedException()
    }
  }

  override def getColumnTypeName(column: Int): String = {
    schema(column - 1).dataType.sql
  }

  override def getColumnClassName(column: Int): String = ???

  override def getPrecision(column: Int): Int = ???

  override def getScale(column: Int): Int = ???

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
