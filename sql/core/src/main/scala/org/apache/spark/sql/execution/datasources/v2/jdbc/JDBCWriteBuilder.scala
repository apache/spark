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
package org.apache.spark.sql.execution.datasources.v2.jdbc

import org.apache.spark.sql._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructType

case class JDBCWriteBuilder(schema: StructType, options: JdbcOptionsInWrite) extends WriteBuilder
  with SupportsTruncate {

  private var isTruncate = false

  override def truncate(): WriteBuilder = {
    isTruncate = true
    this
  }

  override def build(): V1Write = new V1Write {
    override def toInsertableRelation: InsertableRelation = (data: DataFrame, _: Boolean) => {
      // TODO (SPARK-32595): do truncate and append atomically.
      if (isTruncate) {
        val dialect = JdbcDialects.get(options.url)
        val conn = dialect.createConnectionFactory(options)(-1)
        JdbcUtils.truncateTable(conn, options)
      }
      JdbcUtils.saveTable(data, Some(schema), SQLConf.get.caseSensitiveAnalysis, options)
    }
  }
}
