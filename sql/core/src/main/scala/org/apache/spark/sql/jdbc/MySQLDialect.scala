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

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.{BooleanType, DataType, LongType, MetadataBuilder}

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

  override def validateFetchSize(size: Int): Unit = {
    require(size >= 0 || size == Integer.MIN_VALUE,
      s"Invalid value `${size.toString}` for parameter " +
        s"`${JDBCOptions.JDBC_BATCH_FETCH_SIZE}` for MySQL. " +
        s"The value should be >= 0 or equals Integer.MIN_VALUE; " +
        s"When the value is 0, the JDBC driver ignores the value and does the estimates; " +
        s"When the value is Integer.MIN_VALUE, the data will be fetched in streaming manner, " +
        s"namely, fetch one row at a time.")
  }
}
