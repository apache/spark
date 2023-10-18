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
import org.apache.spark.sql.execution.datasources.v2.state.StateDataSourceV2.JoinSideValues
import org.apache.spark.sql.execution.datasources.v2.state.StateDataSourceV2.JoinSideValues.JoinSideValues
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.{LeftSide, RightSide}
import org.apache.spark.sql.execution.streaming.state.StateStoreConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class StateScanBuilder(
    session: SparkSession,
    schema: StructType,
    stateCheckpointRootLocation: String,
    batchId: Long,
    operatorId: Long,
    storeName: String,
    joinSide: JoinSideValues,
    stateStoreConf: StateStoreConf) extends ScanBuilder {
  override def build(): Scan = new StateScan(session, schema, stateCheckpointRootLocation,
    batchId, operatorId, storeName, joinSide, stateStoreConf)
}

class StateStoreInputPartition(
    val partition: Int,
    val queryId: UUID,
    val stateCheckpointRootLocation: String,
    val batchId: Long,
    val operatorId: Long,
    val storeName: String,
    val joinSide: JoinSideValues) extends InputPartition

class StateScan(
    session: SparkSession,
    schema: StructType,
    stateCheckpointRootLocation: String,
    batchId: Long,
    operatorId: Long,
    storeName: String,
    joinSide: JoinSideValues,
    stateStoreConf: StateStoreConf) extends Scan with Batch {

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  private val hadoopConfBroadcast = session.sparkContext.broadcast(
    new SerializableConfiguration(session.sessionState.newHadoopConf()))

  override def readSchema(): StructType = schema

  override def planInputPartitions(): Array[InputPartition] = {
    val fs = stateCheckpointPartitionsLocation.getFileSystem(hadoopConfBroadcast.value.value)
    val partitions = fs.listStatus(stateCheckpointPartitionsLocation, new PathFilter() {
      override def accept(path: Path): Boolean = {
        fs.isDirectory(path) && Try(path.getName.toInt).isSuccess && path.getName.toInt >= 0
      }
    })

    if (partitions.headOption.isEmpty) {
      Array.empty[InputPartition]
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

      partitionNums.map {
        pn => new StateStoreInputPartition(pn, queryId, stateCheckpointRootLocation,
          batchId, operatorId, storeName, joinSide)
      }.toArray
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = joinSide match {
    case JoinSideValues.left =>
      val userFacingSchema = schema
      val stateSchema = StreamStreamJoinStateHelper.readSchema(session,
        stateCheckpointRootLocation, operatorId.toInt, LeftSide, excludeAuxColumns = false)
      new StreamStreamJoinStatePartitionReaderFactory(stateStoreConf,
        hadoopConfBroadcast.value, userFacingSchema, stateSchema)

    case JoinSideValues.right =>
      val userFacingSchema = schema
      val stateSchema = StreamStreamJoinStateHelper.readSchema(session,
        stateCheckpointRootLocation, operatorId.toInt, RightSide, excludeAuxColumns = false)
      new StreamStreamJoinStatePartitionReaderFactory(stateStoreConf,
        hadoopConfBroadcast.value, userFacingSchema, stateSchema)

    case JoinSideValues.none =>
      new StatePartitionReaderFactory(stateStoreConf, hadoopConfBroadcast.value, schema)
  }

  override def toBatch: Batch = this

  // FIXME: show more configs?
  override def description(): String = s"StateScan " +
    s"[stateCkptLocation=$stateCheckpointRootLocation]" +
    s"[batchId=$batchId][operatorId=$operatorId][storeName=$storeName]" +
    s"[joinSide=$joinSide]"

  private def stateCheckpointPartitionsLocation: Path = {
    new Path(stateCheckpointRootLocation, s"$operatorId")
  }
}
