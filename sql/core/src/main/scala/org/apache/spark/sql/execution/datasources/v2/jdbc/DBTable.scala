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

import java.sql.Connection

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.JDBC_TABLE_NAME
import org.apache.spark.sql.sources.v2.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.sources.v2.TableCapability.{BATCH_READ, BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE}
import org.apache.spark.sql.sources.v2.reader.ScanBuilder
import org.apache.spark.sql.sources.v2.writer.WriteBuilder
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, SchemaUtils}

case class DBTable (sparkSession: SparkSession,
                    options: CaseInsensitiveStringMap,
                    userSchema: Option[StructType])
  extends Table with SupportsWrite with SupportsRead with Logging{

  private val userOptions = new JDBCOptions(options.asScala.toMap)
  private val tableName = userOptions.parameters(JDBC_TABLE_NAME)
  private val conn : Connection = JdbcUtils.createConnectionFactory(userOptions)()

  override def name: String = {
    logInfo("***dsv2-flows*** name called. Table name is " + tableName)
    tableName
  }

  override def schema: StructType = {
    /* TODO - check why a schema request?
     *  1. Will this be called for every 'append' request to get schema from DBTable.
     *  If so, schema check in append should not be required.
     *  2. This is called in overwrite as well. What if the table does not exist. A empty
     *     schema would be returned resulting in an exception. Should Overwrite semantics
     *     not create a table.
     */
    logInfo("***dsv2-flows*** schema called")
    val schemaInDB = JdbcUtils.getSchemaOption(conn, userOptions)
    Utils.logSchema("schema from DB", schemaInDB)
    schemaInDB.getOrElse(StructType(Nil))
  }

  override def capabilities: java.util.Set[TableCapability] = DBTable.CAPABILITIES

  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = {
    logInfo("***dsv2-flows*** newWriteBuilder called")
    Utils.logSchema("Schema passed to DBTable", userSchema)
    new JDBCWriteBuilder(
      new JdbcOptionsInWrite(options.asScala.toMap), userSchema)
  }
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    logInfo("***dsv2-flows*** newScanBuilder called")
    new JDBCScanBuilder(new JDBCOptions(options.asScala.toMap), userSchema)
  }
}

object DBTable {
  private val CAPABILITIES = Set(BATCH_READ, BATCH_WRITE, TRUNCATE, OVERWRITE_BY_FILTER).asJava
}
