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
package org.apache.spark.sql.execution.datasources.v2.state.metadata

import java.util

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.{MetadataColumn, SupportsMetadataColumns, SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.execution.datasources.v2.state.StateDataSourceErrors
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions.PATH
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.state.{OperatorStateMetadata, OperatorStateMetadataReader, OperatorStateMetadataV1, OperatorStateMetadataV2}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

case class StateMetadataTableEntry(
    version: Int,
    operatorId: Long,
    operatorName: String,
    stateStoreName: String,
    numPartitions: Int,
    minBatchId: Long,
    maxBatchId: Long,
    numColsPrefixKey: Option[Int],
    operatorProperties: Option[String]) {
  def toRow(): InternalRow = {
    new GenericInternalRow(
      Array[Any](operatorId,
        UTF8String.fromString(operatorName),
        UTF8String.fromString(stateStoreName),
        numPartitions,
        minBatchId,
        maxBatchId,
        UTF8String.fromString(operatorProperties.getOrElse("")),
        numColsPrefixKey.getOrElse(0)))
  }
}

object StateMetadataTableEntry {
  private[sql] val schema: StructType = {
    new StructType()
      .add("operatorId", LongType)
      .add("operatorName", StringType)
      .add("stateStoreName", StringType)
      .add("numPartitions", IntegerType)
      .add("minBatchId", LongType)
      .add("maxBatchId", LongType)
      .add("operatorProperties", StringType)
  }
}

class StateMetadataSource extends TableProvider with DataSourceRegister {

  override def shortName(): String = "state-metadata"

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    new StateMetadataTable(schema)
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (!options.containsKey("path")) {
      throw StateDataSourceErrors.requiredOptionUnspecified(PATH)
    }

    StateMetadataTableEntry.schema
  }
}


class StateMetadataTable(override val schema: StructType) extends Table
  with SupportsRead with SupportsMetadataColumns {
  override def name(): String = "state-metadata-table"

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    () => {
      new StateMetadataScan(options.get("path"), schema)
    }
  }

  private object NumColsPrefixKeyColumn extends MetadataColumn {
    override def name: String = "_numColsPrefixKey"
    override def dataType: DataType = IntegerType
    override def comment: String = "Number of columns in prefix key of the state store instance"
  }

  override val metadataColumns: Array[MetadataColumn] = Array(NumColsPrefixKeyColumn)
}

case class StateMetadataInputPartition(checkpointLocation: String) extends InputPartition

class StateMetadataScan(
    checkpointLocation: String,
    schema: StructType) extends Scan {
  override def readSchema: StructType = schema

  override def toBatch: Batch = {
    new Batch {
      override def planInputPartitions(): Array[InputPartition] = {
        Array(StateMetadataInputPartition(checkpointLocation))
      }

      override def createReaderFactory(): PartitionReaderFactory = {
        // Don't need to broadcast the hadoop conf because this source only has one partition.
        val conf = new SerializableConfiguration(SparkSession.active.sessionState.newHadoopConf())
        StateMetadataPartitionReaderFactory(conf)
      }
    }
  }
}

case class StateMetadataPartitionReaderFactory(hadoopConf: SerializableConfiguration)
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new StateMetadataPartitionReader(
      partition.asInstanceOf[StateMetadataInputPartition].checkpointLocation, hadoopConf)
  }
}

class StateMetadataPartitionReader(
    checkpointLocation: String,
    serializedHadoopConf: SerializableConfiguration) extends PartitionReader[InternalRow] {

  override def next(): Boolean = {
    stateMetadata.hasNext
  }

  override def get(): InternalRow = {
    stateMetadata.next().toRow()
  }

  override def close(): Unit = {}

  private def pathToLong(path: Path) = {
    path.getName.toLong
  }

  private def pathNameCanBeParsedAsLong(path: Path) = {
    try {
      pathToLong(path)
      true
    } catch {
      case _: NumberFormatException => false
    }
  }

  // Return true when the filename can be parsed as long integer.
  private val pathNameCanBeParsedAsLongFilter = new PathFilter {
    override def accept(path: Path): Boolean = pathNameCanBeParsedAsLong(path)
  }

  private lazy val hadoopConf: Configuration = serializedHadoopConf.value

  private lazy val fileManager =
    CheckpointFileManager.create(new Path(checkpointLocation), hadoopConf)

  // List the commit log entries to find all the available batch ids.
  private def batchIds: Array[Long] = {
    val commitLog = new Path(checkpointLocation, "commits")
    if (fileManager.exists(commitLog)) {
      fileManager
        .list(commitLog, pathNameCanBeParsedAsLongFilter).map(f => pathToLong(f.getPath)).sorted
    } else Array.empty
  }

  private def allOperatorStateMetadata: Array[OperatorStateMetadata] = {
    val stateDir = new Path(checkpointLocation, "state")
    val opIds = fileManager
      .list(stateDir, pathNameCanBeParsedAsLongFilter).map(f => pathToLong(f.getPath)).sorted
    opIds.map { opId =>
      new OperatorStateMetadataReader(new Path(stateDir, opId.toString), hadoopConf).read()
    }
  }

  private[sql] lazy val stateMetadata: Iterator[StateMetadataTableEntry] = {
    allOperatorStateMetadata.flatMap { operatorStateMetadata =>
      operatorStateMetadata.version match {
        case 1 =>
          val operatorStateMetadataV1 = operatorStateMetadata.asInstanceOf[OperatorStateMetadataV1]
          operatorStateMetadataV1.stateStoreInfo.map { stateStoreMetadata =>
            StateMetadataTableEntry(
              operatorStateMetadata.version,
              operatorStateMetadataV1.operatorInfo.operatorId,
              operatorStateMetadataV1.operatorInfo.operatorName,
              stateStoreMetadata.storeName,
              stateStoreMetadata.numPartitions,
              if (batchIds.nonEmpty) batchIds.head else -1,
              if (batchIds.nonEmpty) batchIds.last else -1,
              Some(stateStoreMetadata.numColsPrefixKey),
              None
            )
          }

        case 2 =>
          val operatorStateMetadataV2 = operatorStateMetadata.asInstanceOf[OperatorStateMetadataV2]
          operatorStateMetadataV2.stateStoreInfo.map { stateStoreMetadata =>
            StateMetadataTableEntry(
              operatorStateMetadata.version,
              operatorStateMetadataV2.operatorInfo.operatorId,
              operatorStateMetadataV2.operatorInfo.operatorName,
              stateStoreMetadata.storeName,
              stateStoreMetadata.numPartitions,
              if (batchIds.nonEmpty) batchIds.head else -1,
              if (batchIds.nonEmpty) batchIds.last else -1,
              None,
              Some(operatorStateMetadataV2.operatorInfo.operatorProperties)
            )
          }

        case _ =>
          throw new IllegalStateException(s"Failed to process unknown operator metadata " +
            s"version=${operatorStateMetadata.version}")
      }
    }
  }.iterator
}
