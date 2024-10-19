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

import java.util

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog.index.{SupportsIndex, TableIndex}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.errors.DataTypeErrorsBase
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class JDBCTable(ident: Identifier, schema: StructType, jdbcOptions: JDBCOptions)
  extends Table
  with SupportsRead
  with SupportsWrite
  with SupportsIndex
  with DataTypeErrorsBase {

  override def name(): String = ident.toString

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(BATCH_READ, V1_BATCH_WRITE, TRUNCATE)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): JDBCScanBuilder = {
    val mergedOptions = new JDBCOptions(
      jdbcOptions.parameters.originalMap ++ options.asCaseSensitiveMap().asScala)
    JDBCScanBuilder(SparkSession.active, schema, mergedOptions)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val mergedOptions = new JdbcOptionsInWrite(
      jdbcOptions.parameters.originalMap ++ info.options.asCaseSensitiveMap().asScala)
    JDBCWriteBuilder(schema, mergedOptions)
  }

  override def createIndex(
      indexName: String,
      columns: Array[NamedReference],
      columnsProperties: util.Map[NamedReference, util.Map[String, String]],
      properties: util.Map[String, String]): Unit = {
    JdbcUtils.withConnection(jdbcOptions) { conn =>
      JdbcUtils.classifyException(
        errorClass = "FAILED_JDBC.CREATE_INDEX",
        messageParameters = Map(
          "url" -> jdbcOptions.getRedactUrl(),
          "indexName" -> toSQLId(indexName),
          "tableName" -> toSQLId(name)),
        dialect = JdbcDialects.get(jdbcOptions.url),
        description = s"Failed to create index $indexName in ${name()}",
        isRuntime = false) {
        JdbcUtils.createIndex(
          conn, indexName, ident, columns, columnsProperties, properties, jdbcOptions)
      }
    }
  }

  override def indexExists(indexName: String): Boolean = {
    JdbcUtils.withConnection(jdbcOptions) { conn =>
      JdbcUtils.indexExists(conn, indexName, ident, jdbcOptions)
    }
  }

  override def dropIndex(indexName: String): Unit = {
    JdbcUtils.withConnection(jdbcOptions) { conn =>
      JdbcUtils.classifyException(
        errorClass = "FAILED_JDBC.DROP_INDEX",
        messageParameters = Map(
          "url" -> jdbcOptions.getRedactUrl(),
          "indexName" -> toSQLId(indexName),
          "tableName" -> toSQLId(name)),
        dialect = JdbcDialects.get(jdbcOptions.url),
        description = s"Failed to drop index $indexName in ${name()}",
        isRuntime = false) {
        JdbcUtils.dropIndex(conn, indexName, ident, jdbcOptions)
      }
    }
  }

  override def listIndexes(): Array[TableIndex] = {
    JdbcUtils.withConnection(jdbcOptions) { conn =>
      JdbcUtils.listIndexes(conn, ident, jdbcOptions)
    }
  }
}
