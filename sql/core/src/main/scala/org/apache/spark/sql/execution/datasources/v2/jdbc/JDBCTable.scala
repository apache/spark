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

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class JDBCTable(ident: Identifier, schema: StructType, jdbcOptions: JDBCOptions)
  extends Table with SupportsRead with SupportsWrite {

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
}
