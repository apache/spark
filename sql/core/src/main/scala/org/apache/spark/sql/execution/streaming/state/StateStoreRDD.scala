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

/**
 * A trait that provides access to state stores for specific partitions in an RDD.
 *
 * This trait enables the state store optimization pattern for stateful operations
 * where the same partition needs to be accessed for both reading and writing.
 * Implementing classes maintain a mapping between partition IDs and their associated
 * state stores, allowing lookups without creating duplicate connections.
 *
 * The primary use case is enabling the common pattern for stateful operations:
 * 1. A read-only state store is opened to retrieve existing state
 * 2. The same state store is then converted to read-write mode for updates
 * 3. This avoids having two separate open connections to the same state store
 *    which would cause blocking or contention issues
 *
 * Classes implementing this trait typically:
 * - Track state stores by partition ID in a thread-safe collection
 * - Provide cleanup mechanisms when tasks complete
 * - Handle proper state store lifecycle to prevent resource leakage
 */
trait StateStoreRDDProvider {
  /**
   * Returns the state store associated with the specified partition, if one exists.
   *
   * @param partitionId The ID of the partition whose state store should be retrieved
   * @return Some(store) if a state store exists for the given partition, None otherwise
   */
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

  /**
   * Recursively searches the RDD lineage to find a StateStoreRDDProvider containing
   * an already-opened state store for the current partition.
   *
   * This method helps implement the read-then-write pattern for stateful operations
   * without creating contention issues. Instead of opening separate read and write
   * stores that would block each other (since a state store provider can only handle
   * one open store at a time), this allows us to:
   *   1. Find an existing read store in the RDD lineage
   *   2. Convert it to a write store using getWriteStore()
   *
   * This is particularly important for stateful aggregations where StateStoreRestoreExec
   * first reads previous state and StateStoreSaveExec then updates it.
   *
   * The method performs a depth-first search through the RDD dependency graph.
   *
   * @param rdd The starting RDD to search from
   * @return Some(provider) if a StateStoreRDDProvider is found in the lineage, None otherwise
   */
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
