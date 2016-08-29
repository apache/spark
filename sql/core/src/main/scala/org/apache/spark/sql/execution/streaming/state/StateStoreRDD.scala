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

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * An RDD that allows computations to be executed against [[StateStore]]s. It
 * uses the [[StateStoreCoordinator]] to get the locations of loaded state stores
 * and use that as the preferred locations.
 */
class StateStoreRDD[T: ClassTag, U: ClassTag](
    dataRDD: RDD[T],
    storeUpdateFunction: (StateStore, Iterator[T]) => Iterator[U],
    checkpointLocation: String,
    operatorId: Long,
    storeVersion: Long,
    keySchema: StructType,
    valueSchema: StructType,
    sessionState: SessionState,
    @transient private val storeCoordinator: Option[StateStoreCoordinatorRef])
  extends RDD[U](dataRDD) {

  private val storeConf = new StateStoreConf(sessionState.conf)

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  private val confBroadcast = dataRDD.context.broadcast(
    new SerializableConfiguration(sessionState.newHadoopConf()))

  override protected def getPartitions: Array[Partition] = dataRDD.partitions

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val storeId = StateStoreId(checkpointLocation, operatorId, partition.index)
    storeCoordinator.flatMap(_.getLocation(storeId)).toSeq
  }

  override def compute(partition: Partition, ctxt: TaskContext): Iterator[U] = {
    var store: StateStore = null
    val storeId = StateStoreId(checkpointLocation, operatorId, partition.index)
    store = StateStore.get(
      storeId, keySchema, valueSchema, storeVersion, storeConf, confBroadcast.value.value)
    val inputIter = dataRDD.iterator(partition, ctxt)
    storeUpdateFunction(store, inputIter)
  }
}
