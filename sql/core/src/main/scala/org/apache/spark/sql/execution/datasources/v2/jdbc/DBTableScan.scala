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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.reader.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class DBTableScan extends Scan with Batch with Logging {

  val table_schema = StructType(Seq(
    StructField("name", StringType, true),
    StructField("rollnum", StringType, true),
    StructField("occupation", StringType, true)))

  def readSchema: StructType = {
    logInfo("***dsv2-flows*** readSchema called")
    table_schema

  }

  override def toBatch() : Batch = {
    this
  }

  def planInputPartitions: Array[InputPartition] = {
    Array(PartitionScheme)
  }

  def createReaderFactory: PartitionReaderFactory = {

    logInfo("***dsv2-flows*** createReaderFactory called")
    new DBPartitionReaderFactory(table_schema)

  }

}

object PartitionScheme extends InputPartition {

}
