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

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.{MetadataColumn, SupportsMetadataColumns, SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.execution.datasources.v2.state.StateDataSourceErrors
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions.PATH
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.state.{OperatorStateMetadata, OperatorStateMetadataReader, OperatorStateMetadataUtils, OperatorStateMetadataV1, OperatorStateMetadataV2}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

case class StateMetadataTableEntry(
    operatorId: Long,
    operatorName: String,
    stateStoreName: String,
    numPartitions: Int,
    minBatchId: Long,
    maxBatchId: Long,
    version: Int,
    operatorPropertiesJson: String,
    numColsPrefixKey: Int,
    stateSchemaFilePaths: List[String]) {
  def toRow(): InternalRow = {
    new GenericInternalRow(
      Array[Any](operatorId,
        UTF8String.fromString(operatorName),
        UTF8String.fromString(stateStoreName),
        numPartitions,
        minBatchId,
        maxBatchId,
        UTF8String.fromString(operatorPropertiesJson),
        numColsPrefixKey))
  }
}

object StateMetadataTableEntry {
  private[sql] val schema = {
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
    new StateMetadataTable
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // The schema of state metadata table is static.
    StateMetadataTableEntry.schema
  }
}


class StateMetadataTable extends Table with SupportsRead with SupportsMetadataColumns {
  override def name(): String = "state-metadata-table"

  override def schema(): StructType = StateMetadataTableEntry.schema

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    () => {
      if (!options.containsKey("path")) {
        throw StateDataSourceErrors.requiredOptionUnspecified(PATH)
      }

      val checkpointLocation = options.get("path")

      val batchIdOpt = Option(options.get("batchId")).map(_.toLong)
      // if a batchId is provided, use it. Otherwise, use the last committed batch. If there is no
      // committed batch, use batchId 0.
      val batchId = batchIdOpt.getOrElse(OperatorStateMetadataUtils
        .getLastCommittedBatch(SparkSession.active, checkpointLocation).getOrElse(0L))

      new StateMetadataScan(options.get("path"), batchId)
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

class StateMetadataScan(checkpointLocation: String, batchId: Long) extends Scan {
  override def readSchema: StructType = StateMetadataTableEntry.schema

  override def toBatch: Batch = {
    new Batch {
      override def planInputPartitions(): Array[InputPartition] = {
        Array(StateMetadataInputPartition(checkpointLocation))
      }

      override def createReaderFactory(): PartitionReaderFactory = {
        // Don't need to broadcast the hadoop conf because this source only has one partition.
        val conf = new SerializableConfiguration(SparkSession.active.sessionState.newHadoopConf())
        StateMetadataPartitionReaderFactory(conf, batchId)
      }
    }
  }
}

case class StateMetadataPartitionReaderFactory(
    hadoopConf: SerializableConfiguration,
    batchId: Long) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new StateMetadataPartitionReader(
      partition.asInstanceOf[StateMetadataInputPartition].checkpointLocation, hadoopConf, batchId)
  }
}

class StateMetadataPartitionReader(
    checkpointLocation: String,
    serializedHadoopConf: SerializableConfiguration,
    batchId: Long) extends PartitionReader[InternalRow] with Logging {

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

  // Need this to be accessible from IncrementalExecution for the planning rule.
  private[sql] def allOperatorStateMetadata: Array[OperatorStateMetadata] = {
    try {
      val stateDir = new Path(checkpointLocation, "state")
      val opIds = fileManager
        .list(stateDir, pathNameCanBeParsedAsLongFilter).map(f => pathToLong(f.getPath)).sorted
      opIds.map { opId =>
        val operatorIdPath = new Path(stateDir, opId.toString)
        // check if OperatorStateMetadataV2 path exists, if it does, read it
        // otherwise, fall back to OperatorStateMetadataV1
        val operatorStateMetadataV2Path = OperatorStateMetadataV2.metadataDirPath(operatorIdPath)
        val operatorStateMetadataVersion = if (fileManager.exists(operatorStateMetadataV2Path)) {
          2
        } else {
          1
        }
        OperatorStateMetadataReader.createReader(
          operatorIdPath, hadoopConf, operatorStateMetadataVersion, batchId).read() match {
          case Some(metadata) => metadata
          case None => throw StateDataSourceErrors.failedToReadOperatorMetadata(checkpointLocation,
            batchId)
        }
      }
    } catch {
      // if the operator metadata is not present, catch the exception
      // and return an empty array
      case ex: Exception =>
        logWarning(log"Failed to find operator metadata for " +
          log"path=${MDC(LogKeys.CHECKPOINT_LOCATION, checkpointLocation)} " +
          log"with exception=${MDC(LogKeys.EXCEPTION, ex)}")
        Array.empty
    }
  }

  // From v2, we also need to populate the operatorProperties and stateSchemaFilePath fields
  // for use with the state data source reader
  private[sql] lazy val stateMetadata: Iterator[StateMetadataTableEntry] = {
    allOperatorStateMetadata.flatMap { operatorStateMetadata =>
      require(operatorStateMetadata.version == 1 || operatorStateMetadata.version == 2)
      operatorStateMetadata match {
        case v1: OperatorStateMetadataV1 =>
          v1.stateStoreInfo.map { stateStoreMetadata =>
            StateMetadataTableEntry(v1.operatorInfo.operatorId,
              v1.operatorInfo.operatorName,
              stateStoreMetadata.storeName,
              stateStoreMetadata.numPartitions,
              if (batchIds.nonEmpty) batchIds.head else -1,
              if (batchIds.nonEmpty) batchIds.last else -1,
              operatorStateMetadata.version,
              null,
              stateStoreMetadata.numColsPrefixKey,
              List.empty
            )
          }
        case v2: OperatorStateMetadataV2 =>
          v2.stateStoreInfo.map { stateStoreMetadata =>
            StateMetadataTableEntry(v2.operatorInfo.operatorId,
              v2.operatorInfo.operatorName,
              stateStoreMetadata.storeName,
              stateStoreMetadata.numPartitions,
              if (batchIds.nonEmpty) batchIds.head else -1,
              if (batchIds.nonEmpty) batchIds.last else -1,
              operatorStateMetadata.version,
              v2.operatorPropertiesJson,
              -1, // numColsPrefixKey is not available in OperatorStateMetadataV2
              stateStoreMetadata.stateSchemaFilePaths
            )
          }
        }
      }
    }.iterator
}
