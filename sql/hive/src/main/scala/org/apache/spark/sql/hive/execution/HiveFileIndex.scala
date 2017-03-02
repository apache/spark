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

package org.apache.spark.sql.hive.execution

import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.InputFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.client.HiveClientImpl

/**
 * A [[FileIndex]] for a Hive serde table.
 *
 * @param sparkSession a [[SparkSession]]
 * @param table the metadata of the table
 * @param sizeInBytes the table's data size in bytes
 */
class HiveFileIndex(
    sparkSession: SparkSession,
    table: CatalogTable,
    sizeInBytes: Long)
  extends CatalogFileIndex(sparkSession, table, sizeInBytes) {

  @transient private val hiveQlTable = HiveClientImpl.toHiveTable(table)

  override protected def buildPartitionPath(p: CatalogTablePartition): PartitionPath = {
    val hivePart = HiveClientImpl.toHivePartition(p, hiveQlTable)
    val partDesc = Utilities.getPartitionDesc(hivePart)

    val partDeser = hivePart.getDeserializer.getClass.asInstanceOf[Class[Deserializer]]
    val partInputFormat = partDesc.getInputFileFormatClass
      .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
    val partProps = partDesc.getProperties

    PartitionPath(
      values = p.toRow(partitionSchema, sparkSession.sessionState.conf.sessionLocalTimeZone),
      path = hivePart.getDataLocation,
      metadata = (partDeser, partInputFormat, partProps))
  }
}
