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

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.types._

private[sql] object JdbcTypes extends Logging {

  /**
   * More about JDBC types mapped to Java types:
   *   http://docs.oracle.com/javase/6/docs/technotes/guides/jdbc/getstart/mapping.html#1051555
   *
   * Compatibility of ResultSet getter Methods defined in JDBC spec:
   *   http://download.oracle.com/otn-pub/jcp/jdbc-4_1-mrel-spec/jdbc4.1-fr-spec.pdf
   *   page 211
   */
  def toPrimitiveDataType(jdbcType: Int): DataType =
    jdbcType match {
      case java.sql.Types.LONGVARCHAR
         | java.sql.Types.VARCHAR
         | java.sql.Types.CHAR        => StringType
      case java.sql.Types.NUMERIC
         | java.sql.Types.DECIMAL     => DecimalType
      case java.sql.Types.BIT         => BooleanType
      case java.sql.Types.TINYINT     => ByteType
      case java.sql.Types.SMALLINT    => ShortType
      case java.sql.Types.INTEGER     => IntegerType
      case java.sql.Types.BIGINT      => LongType
      case java.sql.Types.REAL        => FloatType
      case java.sql.Types.FLOAT
         | java.sql.Types.DOUBLE      => DoubleType
      case java.sql.Types.LONGVARBINARY
         | java.sql.Types.VARBINARY
         | java.sql.Types.BINARY      => BinaryType
      // Timestamp's getter should also be able to get DATE and TIME according to JDBC spec
      case java.sql.Types.TIMESTAMP
         | java.sql.Types.DATE
         | java.sql.Types.TIME        => TimestampType

      // TODO: CLOB only works with getClob or getAscIIStream
      // case java.sql.Types.CLOB

      // TODO: BLOB only works with getBlob or getBinaryStream
      // case java.sql.Types.BLOB

      // TODO: nested types
      // case java.sql.Types.ARRAY     => ArrayType
      // case java.sql.Types.STRUCT    => StructType

      // TODO: unsupported types
      // case java.sql.Types.DISTINCT
      // case java.sql.Types.REF

      // TODO: more about JAVA_OBJECT:
      //   http://docs.oracle.com/javase/6/docs/technotes/guides/jdbc/getstart/mapping.html#1038181
      // case java.sql.Types.JAVA_OBJECT => BinaryType

      case _ => sys.error(
        s"Unsupported jdbc datatype")
  }
}
