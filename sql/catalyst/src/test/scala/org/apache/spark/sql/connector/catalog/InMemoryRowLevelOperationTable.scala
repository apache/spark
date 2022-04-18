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

package org.apache.spark.sql.connector.catalog

import java.util

import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{FieldReference, LogicalExpressions, NamedReference, SortDirection, SortOrder, Transform}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, RequiresDistributionAndOrdering, RowLevelOperation, RowLevelOperationBuilder, RowLevelOperationInfo, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class InMemoryRowLevelOperationTable(
    name: String,
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryTable(name, schema, partitioning, properties) with SupportsRowLevelOperations {

  override def newRowLevelOperationBuilder(
      info: RowLevelOperationInfo): RowLevelOperationBuilder = {
    () => PartitionBasedOperation(info.command)
  }

  case class PartitionBasedOperation(command: Command) extends RowLevelOperation {
    private final val PARTITION_COLUMN_REF = FieldReference(PartitionKeyColumn.name)

    var configuredScan: InMemoryBatchScan = _

    override def requiredMetadataAttributes(): Array[NamedReference] = {
      Array(PARTITION_COLUMN_REF)
    }

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new InMemoryScanBuilder(schema) {
        override def build: Scan = {
          val scan = super.build()
          configuredScan = scan.asInstanceOf[InMemoryBatchScan]
          scan
        }
      }
    }

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new WriteBuilder {

      override def build(): Write = new Write with RequiresDistributionAndOrdering {
        override def requiredDistribution(): Distribution = {
          Distributions.clustered(Array(PARTITION_COLUMN_REF))
        }

        override def requiredOrdering(): Array[SortOrder] = {
          Array[SortOrder](
            LogicalExpressions.sort(
              PARTITION_COLUMN_REF,
              SortDirection.ASCENDING,
              SortDirection.ASCENDING.defaultNullOrdering())
          )
        }

        override def toBatch: BatchWrite = PartitionBasedReplaceData(configuredScan)

        override def description(): String = "InMemoryWrite"
      }
    }

    override def description(): String = "InMemoryPartitionReplaceOperation"
  }

  private case class PartitionBasedReplaceData(scan: InMemoryBatchScan) extends TestBatchWrite {

    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      val readRows = scan.data.flatMap(_.asInstanceOf[BufferedRows].rows)
      val readPartitions = readRows.map(r => getKey(r, schema))
      dataMap --= readPartitions
      withData(newData, schema)
    }
  }
}
