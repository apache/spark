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
import org.apache.spark.sql.execution.streaming.state.StateStoreConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class StateScanBuilder(
    session: SparkSession,
    schema: StructType,
    stateCheckpointRootLocation: String,
    version: Long,
    operatorId: Long,
    storeName: String) extends ScanBuilder {
  override def build(): Scan = new StateScan(session, schema, stateCheckpointRootLocation,
    version, operatorId, storeName)
}

class StateStoreInputPartition(
    val partition: Int,
    val queryId: UUID,
    val stateCheckpointRootLocation: String,
    val version: Long,
    val operatorId: Long,
    val storeName: String) extends InputPartition

class StateScan(
    session: SparkSession,
    schema: StructType,
    stateCheckpointRootLocation: String,
    version: Long,
    operatorId: Long,
    storeName: String) extends Scan with Batch {

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  private val hadoopConfBroadcast = session.sparkContext.broadcast(
    new SerializableConfiguration(session.sessionState.newHadoopConf()))

  private val resolvedCpLocation = {
    val checkpointPath = new Path(stateCheckpointRootLocation)
    val fs = checkpointPath.getFileSystem(session.sessionState.newHadoopConf())
    checkpointPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toUri.toString
  }

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
        s"No continuous partitions in state: $partitionNums")

      partitionNums.map {
        pn => new StateStoreInputPartition(pn, queryId, stateCheckpointRootLocation,
          version, operatorId, storeName)
      }.toArray
    }
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new StatePartitionReaderFactory(new StateStoreConf(session.sessionState.conf),
      hadoopConfBroadcast.value, schema)

  override def toBatch: Batch = this

  override def description(): String = s"StateScan [stateCpLocation=$stateCheckpointRootLocation]" +
    s"[version=$version][operatorId=$operatorId][storeName=$storeName]"

  private def stateCheckpointPartitionsLocation: Path = {
    new Path(resolvedCpLocation, s"$operatorId")
  }
}
