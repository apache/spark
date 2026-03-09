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

/**
 * Dialect for Presto/Trino database.
 * Handles Presto-specific type mappings, including the TIME type.
 */
private object PrestoDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = {
    url.startsWith("jdbc:presto:") || url.startsWith("jdbc:trino:")
  }

  /**
   * Maps JDBC types from Presto to Spark SQL types.
   * Key mapping: java.sql.Types.TIME (92) -> TimeType
   */
  override def getCatalystType(
      sqlType: Int,
      typeName: String,
      size: Int,
      md: MetadataBuilder): Option[DataType] = {
    sqlType match {
      // Map Presto TIME type to Spark TimeType
      case Types.TIME => Some(TimeType)

      // Map Presto DATE type to Spark DateType
      case Types.DATE => Some(DateType)

      // Map Presto TIMESTAMP to appropriate Spark type
      case Types.TIMESTAMP =>
        // Presto TIMESTAMP is without timezone by default
        if (md != null && md.build().contains("isTimestampNTZ") &&
            md.build().getBoolean("isTimestampNTZ")) {
          Some(TimestampNTZType)
        } else {
          Some(TimestampType)
        }

      // Map Presto VARCHAR to StringType
      case Types.VARCHAR | Types.LONGVARCHAR => Some(StringType)

      // Map Presto VARBINARY to BinaryType
      case Types.VARBINARY | Types.LONGVARBINARY => Some(BinaryType)

      // Use default mappings for other types
      case _ => None
    }
  }

  /**
   * Maps Spark SQL types to Presto JDBC types for writing.
   */
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    // Map Spark TimeType to Presto TIME
    case TimeType => Some(JdbcType("TIME", Types.TIME))

    // Map Spark TimestampNTZType to Presto TIMESTAMP
    case TimestampNTZType => Some(JdbcType("TIMESTAMP", Types.TIMESTAMP))

    // Map Spark TimestampType to Presto TIMESTAMP WITH TIME ZONE
    case TimestampType => Some(JdbcType("TIMESTAMP WITH TIME ZONE", Types.TIMESTAMP))

    // Map Spark StringType to Presto VARCHAR
    case StringType => Some(JdbcType("VARCHAR", Types.VARCHAR))

    // Map Spark BinaryType to Presto VARBINARY
    case BinaryType => Some(JdbcType("VARBINARY", Types.VARBINARY))

    // Map Spark BooleanType to Presto BOOLEAN
    case BooleanType => Some(JdbcType("BOOLEAN", Types.BOOLEAN))

    // Map Spark numeric types
    case ByteType => Some(JdbcType("TINYINT", Types.TINYINT))
    case ShortType => Some(JdbcType("SMALLINT", Types.SMALLINT))
    case IntegerType => Some(JdbcType("INTEGER", Types.INTEGER))
    case LongType => Some(JdbcType("BIGINT", Types.BIGINT))
    case FloatType => Some(JdbcType("REAL", Types.FLOAT))
    case DoubleType => Some(JdbcType("DOUBLE", Types.DOUBLE))

    // Map Spark DecimalType to Presto DECIMAL
    case d: DecimalType =>
      Some(JdbcType(s"DECIMAL(${d.precision},${d.scale})", Types.DECIMAL))

    // Map Spark DateType to Presto DATE
    case DateType => Some(JdbcType("DATE", Types.DATE))

    // Use default mappings for other types
    case _ => None
  }

  /**
   * Quotes identifiers for Presto (uses double quotes).
   */
  override def quoteIdentifier(colName: String): String = {
    s""""$colName""""
  }

  /**
   * Returns true if Presto supports cascading truncate.
   */
  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)
}

// Made with Bob
