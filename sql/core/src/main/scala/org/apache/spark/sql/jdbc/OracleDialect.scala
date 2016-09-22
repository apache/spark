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


private case object OracleDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle")

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.NUMERIC) {
      val scale = if (null != md) md.build().getLong("scale") else 0L
      size match {
        // Handle NUMBER fields that have no precision/scale in special way
        // because JDBC ResultSetMetaData converts this to 0 precision and -127 scale
        // For more details, please see
        // https://github.com/apache/spark/pull/8780#issuecomment-145598968
        // and
        // https://github.com/apache/spark/pull/8780#issuecomment-144541760
        case 0 => Option(DecimalType(DecimalType.MAX_PRECISION, 10))
        // Handle FLOAT fields in a special way because JDBC ResultSetMetaData converts
        // this to NUMERIC with -127 scale
        // Not sure if there is a more robust way to identify the field as a float (or other
        // numeric types that do not specify a scale.
        case _ if scale == -127L => Option(DecimalType(DecimalType.MAX_PRECISION, 10))
        case 1 => Option(BooleanType)
        case 3 | 5 | 10 => Option(IntegerType)
        case 19 if scale == 0L => Option(LongType)
        case 19 if scale == 4L => Option(FloatType)
        case _ => None
      }
    } else {
      None
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    // For more details, please see
    // https://docs.oracle.com/cd/E19501-01/819-3659/gcmaz/
    case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.BOOLEAN))
    case IntegerType => Some(JdbcType("NUMBER(10)", java.sql.Types.INTEGER))
    case LongType => Some(JdbcType("NUMBER(19)", java.sql.Types.BIGINT))
    case FloatType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.FLOAT))
    case DoubleType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.DOUBLE))
    case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.SMALLINT))
    case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.SMALLINT))
    case StringType => Some(JdbcType("VARCHAR2(255)", java.sql.Types.VARCHAR))
    case _ => None
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)
}
