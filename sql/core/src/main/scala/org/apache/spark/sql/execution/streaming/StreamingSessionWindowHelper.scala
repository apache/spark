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

package org.apache.spark.sql.execution.streaming

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.state.{StateStoreCoordinatorRef, StateStoreProviderId}

object StreamingSessionWindowHelper extends Logging {

  class StateStoreAwareRDD[A: ClassTag, V: ClassTag](
      var f: (Int, Iterator[A]) => Iterator[V],
      var rdd: RDD[A],
      stateInfo: StatefulOperatorStateInfo,
      stateStoreNames: Seq[String],
      @transient private val storeCoordinator: Option[StateStoreCoordinatorRef])
    extends RDD[V](rdd) {

    override protected def getPartitions: Array[Partition] = rdd.partitions

    /**
     * Set the preferred location of each partition using the executor that has the related
     * [[StateStoreProvider]] already loaded.
     */
    override def getPreferredLocations(partition: Partition): Seq[String] = {
      stateStoreNames.flatMap { storeName =>
        val stateStoreProviderId = StateStoreProviderId(stateInfo, partition.index, storeName)
        storeCoordinator.flatMap(_.getLocation(stateStoreProviderId))
      }.distinct
    }

    override def compute(p: Partition, context: TaskContext): Iterator[V] = {
      f(p.index, rdd.iterator(p, context))
    }

    override def clearDependencies(): Unit = {
      super.clearDependencies()
      rdd = null
      f = null
    }
  }

  implicit class StateStoreAwareHelper[T: ClassTag](dataRDD: RDD[T]) {
    def mapPartitionsWithStateStoreAwareRDD[U: ClassTag](
        stateInfo: StatefulOperatorStateInfo,
        storeNames: Seq[String],
        storeCoordinator: StateStoreCoordinatorRef)
        (f: (Int, Iterator[T]) => Iterator[U]): RDD[U] = {
      new StateStoreAwareRDD(f, dataRDD, stateInfo, storeNames, Some(storeCoordinator))
    }
  }
}
