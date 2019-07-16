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

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.execution.datasources.v2.csv.CSVWriteBuilder
import org.apache.spark.sql.sources.v2.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.sources.v2.TableCapability.{BATCH_READ, BATCH_WRITE, TRUNCATE}
import org.apache.spark.sql.sources.v2.reader.ScanBuilder
import org.apache.spark.sql.sources.v2.writer.WriteBuilder
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class DBTable (sparkSession: SparkSession,
                     options: CaseInsensitiveStringMap,
                     userSpecifiedSchema: Option[StructType])
  extends Table with SupportsWrite with SupportsRead with Logging{


  override def name: String = {
    // TODO - Should come from user options

    logInfo("***dsv2-flows*** name called")

    "mysqltable"
  }

  def schema: StructType = {
    // TODO - Remove hardcoded schema
    logInfo("***dsv2-flows*** schema called")
    StructType(Seq(
      StructField("name", StringType, true),
      StructField("rollnum", StringType, true),
      StructField("occupation", StringType, true)))
  }

  override def capabilities: java.util.Set[TableCapability] = DBTable.CAPABILITIES

  def supportsDataType(dataType: DataType): Boolean = true

  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = {
    logInfo("***dsv2-flows*** newWriteBuilder called")
    new JDBCWriteBuilder()
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    logInfo("***dsv2-flows*** newScanBuilder called")
    new JDBCScanBuilder()
  }

}

object DBTable {
  private val CAPABILITIES = Set(BATCH_WRITE, BATCH_READ, TRUNCATE).asJava
}



