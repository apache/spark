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
package org.apache.spark.sql.execution.datasources.v2.state

import java.util.UUID

import scala.util.Try

import org.apache.hadoop.fs.{Path, PathFilter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions.JoinSideValues
import org.apache.spark.sql.execution.datasources.v2.state.metadata.StateMetadataTableEntry
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.{LeftSide, RightSide}
import org.apache.spark.sql.execution.streaming.state.{StateStoreConf, StateStoreErrors}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/** An implementation of [[ScanBuilder]] for State Store data source. */
class StateScanBuilder(
    session: SparkSession,
    schema: StructType,
    sourceOptions: StateSourceOptions,
    stateStoreConf: StateStoreConf,
    stateStoreMetadata: Array[StateMetadataTableEntry]) extends ScanBuilder {
  override def build(): Scan = new StateScan(session, schema, sourceOptions, stateStoreConf,
    stateStoreMetadata)
}

/** An implementation of [[InputPartition]] for State Store data source. */
class StateStoreInputPartition(
    val partition: Int,
    val queryId: UUID,
    val sourceOptions: StateSourceOptions) extends InputPartition

/** An implementation of [[Scan]] with [[Batch]] for State Store data source. */
class StateScan(
    session: SparkSession,
    schema: StructType,
    sourceOptions: StateSourceOptions,
    stateStoreConf: StateStoreConf,
    stateStoreMetadata: Array[StateMetadataTableEntry]) extends Scan with Batch {

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  private val hadoopConfBroadcast = session.sparkContext.broadcast(
    new SerializableConfiguration(session.sessionState.newHadoopConf()))

  override def readSchema(): StructType = schema

  override def planInputPartitions(): Array[InputPartition] = {
    val fs = stateCheckpointPartitionsLocation.getFileSystem(hadoopConfBroadcast.value.value)
    val partitions = fs.listStatus(stateCheckpointPartitionsLocation, new PathFilter() {
      override def accept(path: Path): Boolean = {
        fs.getFileStatus(path).isDirectory &&
        Try(path.getName.toInt).isSuccess && path.getName.toInt >= 0
      }
    })

    if (partitions.headOption.isEmpty) {
      throw StateDataSourceErrors.noPartitionDiscoveredInStateStore(sourceOptions)
    } else {
      // just a dummy query id because we are actually not running streaming query
      val queryId = UUID.randomUUID()

      val partitionsSorted = partitions.sortBy(fs => fs.getPath.getName.toInt)
      val partitionNums = partitionsSorted.map(_.getPath.getName.toInt)
      // assuming no same number - they're directories hence no same name
      val head = partitionNums.head
      val tail = partitionNums(partitionNums.length - 1)
      assert(head == 0, "Partition should start with 0")
      assert((tail - head + 1) == partitionNums.length,
        s"No continuous partitions in state: ${partitionNums.mkString("Array(", ", ", ")")}")

      sourceOptions.fromSnapshotOptions match {
        case None => partitionNums.map { pn =>
          new StateStoreInputPartition(pn, queryId, sourceOptions)
        }.toArray

        case Some(fromSnapshotOptions) =>
          if (partitionNums.contains(fromSnapshotOptions.snapshotPartitionId)) {
            Array(new StateStoreInputPartition(
                fromSnapshotOptions.snapshotPartitionId, queryId, sourceOptions))
          } else {
            throw StateStoreErrors.stateStoreSnapshotPartitionNotFound(
              fromSnapshotOptions.snapshotPartitionId, sourceOptions.operatorId,
              sourceOptions.stateCheckpointLocation.toString)
          }
      }
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = sourceOptions.joinSide match {
    case JoinSideValues.left =>
      val userFacingSchema = schema
      val stateSchema = StreamStreamJoinStateHelper.readSchema(session,
        sourceOptions.stateCheckpointLocation.toString, sourceOptions.operatorId, LeftSide,
        excludeAuxColumns = false)
      new StreamStreamJoinStatePartitionReaderFactory(stateStoreConf,
        hadoopConfBroadcast.value, userFacingSchema, stateSchema)

    case JoinSideValues.right =>
      val userFacingSchema = schema
      val stateSchema = StreamStreamJoinStateHelper.readSchema(session,
        sourceOptions.stateCheckpointLocation.toString, sourceOptions.operatorId, RightSide,
        excludeAuxColumns = false)
      new StreamStreamJoinStatePartitionReaderFactory(stateStoreConf,
        hadoopConfBroadcast.value, userFacingSchema, stateSchema)

    case JoinSideValues.none =>
      new StatePartitionReaderFactory(stateStoreConf, hadoopConfBroadcast.value, schema,
        stateStoreMetadata)
  }

  override def toBatch: Batch = this

  override def description(): String = {
    var desc = s"StateScan " +
      s"[stateCkptLocation=${sourceOptions.stateCheckpointLocation}]" +
      s"[batchId=${sourceOptions.batchId}][operatorId=${sourceOptions.operatorId}]" +
      s"[storeName=${sourceOptions.storeName}]"

    if (sourceOptions.joinSide != JoinSideValues.none) {
      desc += s"[joinSide=${sourceOptions.joinSide}]"
    }
    sourceOptions.fromSnapshotOptions match {
      case Some(fromSnapshotOptions) =>
        desc += s"[snapshotStartBatchId=${fromSnapshotOptions.snapshotStartBatchId}]"
        desc += s"[snapshotPartitionId=${fromSnapshotOptions.snapshotPartitionId}]"
      case _ =>
    }
    sourceOptions.readChangeFeedOptions match {
      case Some(fromSnapshotOptions) =>
        desc += s"[changeStartBatchId=${fromSnapshotOptions.changeStartBatchId}"
        desc += s"[changeEndBatchId=${fromSnapshotOptions.changeEndBatchId}"
      case _ =>
    }
    desc
  }

  private def stateCheckpointPartitionsLocation: Path = {
    new Path(sourceOptions.stateCheckpointLocation, sourceOptions.operatorId.toString)
  }
}
