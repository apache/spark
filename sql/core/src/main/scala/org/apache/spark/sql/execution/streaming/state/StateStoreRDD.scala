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

package org.apache.spark.sql.execution.streaming.state

import java.util.UUID

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

trait StateStoreRDDProvider {
  def getStateStoreForPartition(partitionId: Int): Option[ReadStateStore]
}

abstract class BaseStateStoreRDD[T: ClassTag, U: ClassTag](
    dataRDD: RDD[T],
    checkpointLocation: String,
    queryRunId: UUID,
    operatorId: Long,
    sessionState: SessionState,
    @transient private val storeCoordinator: Option[StateStoreCoordinatorRef],
    extraOptions: Map[String, String] = Map.empty) extends RDD[U](dataRDD) {

  protected val storeConf = new StateStoreConf(sessionState.conf, extraOptions)

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  protected val hadoopConfBroadcast = dataRDD.context.broadcast(
    new SerializableConfiguration(sessionState.newHadoopConf()))

  /**
   * Set the preferred location of each partition using the executor that has the related
   * [[StateStoreProvider]] already loaded.
   *
   * Implementations can simply call this method in getPreferredLocations.
   */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val stateStoreProviderId = getStateProviderId(partition)
    storeCoordinator.flatMap(_.getLocation(stateStoreProviderId)).toSeq
  }

  protected def getStateProviderId(partition: Partition): StateStoreProviderId = {
    StateStoreProviderId(
      StateStoreId(checkpointLocation, operatorId, partition.index),
      queryRunId)
  }
}

/**
 * An RDD that allows computations to be executed against [[ReadStateStore]]s. It
 * uses the [[StateStoreCoordinator]] to get the locations of loaded state stores
 * and use that as the preferred locations.
 */
class ReadStateStoreRDD[T: ClassTag, U: ClassTag](
    dataRDD: RDD[T],
    storeReadFunction: (ReadStateStore, Iterator[T]) => Iterator[U],
    checkpointLocation: String,
    queryRunId: UUID,
    operatorId: Long,
    storeVersion: Long,
    stateStoreCkptIds: Option[Array[Array[String]]],
    stateSchemaBroadcast: Option[StateSchemaBroadcast],
    keySchema: StructType,
    valueSchema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    sessionState: SessionState,
    @transient private val storeCoordinator: Option[StateStoreCoordinatorRef],
    useColumnFamilies: Boolean = false,
    extraOptions: Map[String, String] = Map.empty)
  extends BaseStateStoreRDD[T, U](dataRDD, checkpointLocation, queryRunId, operatorId,
    sessionState, storeCoordinator, extraOptions) with StateStoreRDDProvider {

  // Using a ConcurrentHashMap to track state stores by partition ID
  @transient private lazy val partitionStores =
    new java.util.concurrent.ConcurrentHashMap[Int, ReadStateStore]()

  override def getStateStoreForPartition(partitionId: Int): Option[ReadStateStore] = {
    Option(partitionStores.get(partitionId))
  }

  override protected def getPartitions: Array[Partition] = dataRDD.partitions

  override def compute(partition: Partition, ctxt: TaskContext): Iterator[U] = {
    val storeProviderId = getStateProviderId(partition)
    val partitionId = partition.index

    val inputIter = dataRDD.iterator(partition, ctxt)
    val store = StateStore.getReadOnly(
      storeProviderId, keySchema, valueSchema, keyStateEncoderSpec, storeVersion,
      stateStoreCkptIds.map(_.apply(partitionId).head),
      stateSchemaBroadcast,
      useColumnFamilies, storeConf, hadoopConfBroadcast.value.value)

    // Store reference for this partition
    partitionStores.put(partitionId, store)

    // Register a cleanup callback to be executed when the task completes
    ctxt.addTaskCompletionListener[Unit](_ => {
      partitionStores.remove(partitionId)
    })

    storeReadFunction(store, inputIter)
  }
}
/**
 * An RDD that allows computations to be executed against [[StateStore]]s. It
 * uses the [[StateStoreCoordinator]] to get the locations of loaded state stores
 * and use that as the preferred locations.
 */
class StateStoreRDD[T: ClassTag, U: ClassTag](
    dataRDD: RDD[T],
    storeUpdateFunction: (StateStore, Iterator[T]) => Iterator[U],
    checkpointLocation: String,
    queryRunId: UUID,
    operatorId: Long,
    storeVersion: Long,
    uniqueId: Option[Array[Array[String]]],
    stateSchemaBroadcast: Option[StateSchemaBroadcast],
    keySchema: StructType,
    valueSchema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    sessionState: SessionState,
    @transient private val storeCoordinator: Option[StateStoreCoordinatorRef],
    useColumnFamilies: Boolean = false,
    extraOptions: Map[String, String] = Map.empty,
    useMultipleValuesPerKey: Boolean = false)
  extends BaseStateStoreRDD[T, U](dataRDD, checkpointLocation, queryRunId, operatorId,
    sessionState, storeCoordinator, extraOptions) {

  override protected def getPartitions: Array[Partition] = dataRDD.partitions

  // Recursively find a state store provider in the RDD lineage
  private def findStateStoreProvider(rdd: RDD[_]): Option[StateStoreRDDProvider] = {
    rdd match {
      case null => None
      case provider: StateStoreRDDProvider => Some(provider)
      case _ if rdd.dependencies.isEmpty => None
      case _ =>
        // Search all dependencies
        rdd.dependencies.view
          .map(dep => findStateStoreProvider(dep.rdd))
          .find(_.isDefined)
          .flatten
    }
  }

  override def compute(partition: Partition, ctxt: TaskContext): Iterator[U] = {
    val storeProviderId = getStateProviderId(partition)
    val inputIter = dataRDD.iterator(partition, ctxt)

    // Try to find a state store provider in the RDD lineage
    val store = findStateStoreProvider(dataRDD).flatMap { provider =>
      provider.getStateStoreForPartition(partition.index)
    } match {
      case Some(readStore) =>
        // Convert the read store to a writable store
        StateStore.getWriteStore(
          readStore,
          storeProviderId,
          keySchema,
          valueSchema,
          keyStateEncoderSpec,
          storeVersion,
          uniqueId.map(_.apply(partition.index).head),
          stateSchemaBroadcast,
          useColumnFamilies,
          storeConf,
          hadoopConfBroadcast.value.value,
          useMultipleValuesPerKey)

      case None =>
        // Fall back to creating a new store
        StateStore.get(
          storeProviderId,
          keySchema,
          valueSchema,
          keyStateEncoderSpec,
          storeVersion,
          uniqueId.map(_.apply(partition.index).head),
          stateSchemaBroadcast,
          useColumnFamilies,
          storeConf,
          hadoopConfBroadcast.value.value,
          useMultipleValuesPerKey)
    }
    storeUpdateFunction(store, inputIter)
  }
}
