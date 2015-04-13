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

import org.apache.spark.sql.types._

import java.sql.Types


/**
 * Encapsulates workarounds for the extensions, quirks, and bugs in various
 * databases.  Lots of databases define types that aren't explicitly supported
 * by the JDBC spec.  Some JDBC drivers also report inaccurate
 * information---for instance, BIT(n>1) being reported as a BIT type is quite
 * common, even though BIT in JDBC is meant for single-bit values.  Also, there
 * does not appear to be a standard name for an unbounded string or binary
 * type; we use BLOB and CLOB by default but override with database-specific
 * alternatives when these are absent or do not behave correctly.
 *
 * Currently, the only thing DriverQuirks does is handle type mapping.
 * `getCatalystType` is used when reading from a JDBC table and `getJDBCType`
 * is used when writing to a JDBC table.  If `getCatalystType` returns `null`,
 * the default type handling is used for the given JDBC type.  Similarly,
 * if `getJDBCType` returns `(null, None)`, the default type handling is used
 * for the given Catalyst type.
 */
abstract class DriverQuirks {
  def canHandle(url : String): Boolean
  def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): DataType
  def getJDBCType(dt: DataType): (String, Option[Int])
}

object DriverQuirks {

  private var quirks = List[DriverQuirks]()

  def registerQuirks(quirk: DriverQuirks) {
    quirks = quirk :: quirks
  }

  def unregisterQuirks(quirk : DriverQuirks) {
    quirks = quirks.filterNot(_ == quirk)
  }

  registerQuirks(new MySQLQuirks())
  registerQuirks(new PostgresQuirks())

  /**
   * Fetch the DriverQuirks class corresponding to a given database url.
   */
  def get(url: String): DriverQuirks = {
    val matchingQuirks = quirks.filter(_.canHandle(url))
    matchingQuirks.length match {
      case 0 => new NoQuirks()
      case 1 => matchingQuirks.head
      case _ => new AggregatedQuirks(matchingQuirks)
    }
  }
}

class AggregatedQuirks(quirks: List[DriverQuirks]) extends DriverQuirks {
  def canHandle(url : String): Boolean =
    quirks.foldLeft(true)((l,r) => l && r.canHandle(url))
  def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder) : DataType =
    quirks.foldLeft(null.asInstanceOf[DataType])((l,r) =>
      if (l != null) {
        l
      } else {
        r.getCatalystType(sqlType, typeName, size, md)
      }
    )
  def getJDBCType(dt: DataType): (String, Option[Int]) =
    quirks.foldLeft(null.asInstanceOf[(String, Option[Int])])((l,r) =>
      if (l != null) {
        l
      } else {
        r.getJDBCType(dt)
      }
    )
}

class NoQuirks extends DriverQuirks {
  def canHandle(url : String): Boolean = true
  def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): DataType =
    null
  def getJDBCType(dt: DataType): (String, Option[Int]) = (null, None)
}

class PostgresQuirks extends DriverQuirks {
  def canHandle(url: String): Boolean = url.startsWith("jdbc:postgresql")
  def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): DataType = {
    if (sqlType == Types.BIT && typeName.equals("bit") && size != 1) {
      BinaryType
    } else if (sqlType == Types.OTHER && typeName.equals("cidr")) {
      StringType
    } else if (sqlType == Types.OTHER && typeName.equals("inet")) {
      StringType
    } else null
  }

  def getJDBCType(dt: DataType): (String, Option[Int]) = dt match {
    case StringType => ("TEXT", Some(java.sql.Types.CHAR))
    case BinaryType => ("BYTEA", Some(java.sql.Types.BINARY))
    case BooleanType => ("BOOLEAN", Some(java.sql.Types.BOOLEAN))
    case _ => (null, None)
  }
}

class MySQLQuirks extends DriverQuirks {
  def canHandle(url : String): Boolean = url.startsWith("jdbc:mysql")
  def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): DataType = {
    if (sqlType == Types.VARBINARY && typeName.equals("BIT") && size != 1) {
      // This could instead be a BinaryType if we'd rather return bit-vectors of up to 64 bits as
      // byte arrays instead of longs.
      md.putLong("binarylong", 1)
      LongType
    } else if (sqlType == Types.BIT && typeName.equals("TINYINT")) {
      BooleanType
    } else null
  }
  def getJDBCType(dt: DataType): (String, Option[Int]) = (null, None)
}
