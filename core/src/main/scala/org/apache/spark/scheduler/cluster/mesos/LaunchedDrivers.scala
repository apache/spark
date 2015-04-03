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

package org.apache.spark.scheduler.cluster.mesos

import scala.collection.mutable
import org.apache.mesos.Protos.SlaveID

/**
 * Tracks all the launched or running drivers in the Mesos cluster scheduler.
 * This class is not thread-safe, and we expect the caller to handle synchronizing state.
 * @param state Persistence engine to store tasks state.
 */
private[mesos] class LaunchedDrivers(state: MesosClusterPersistenceEngine) {
  private val drivers = new mutable.HashMap[String, MesosClusterTaskState]

  // Holds the list of tasks that needs to reconciliation from the master.
  // All states that are loaded after failover are added here.
  val pendingRecover = new mutable.HashMap[String, SlaveID]

  initialize()

  def initialize(): Unit = {
    state.fetchAll[MesosClusterTaskState]().foreach { case state =>
      drivers(state.taskId.getValue) = state
      pendingRecover(state.taskId.getValue) = state.slaveId
    }
  }

  def size: Int = drivers.size

  def get(submissionId: String): MesosClusterTaskState = drivers(submissionId)

  def states: Iterable[MesosClusterTaskState] = {
    drivers.values.map(_.copy()).toList
  }

  def contains(submissionId: String): Boolean = drivers.contains(submissionId)

  def remove(submissionId: String): Option[MesosClusterTaskState] = {
    pendingRecover.remove(submissionId)
    val removedState = drivers.remove(submissionId)
    state.expunge(submissionId)
    removedState
  }

  def set(submissionId: String, newState: MesosClusterTaskState): Unit = {
    pendingRecover.remove(newState.taskId.getValue)
    drivers(submissionId) = newState
    state.persist(submissionId, newState)
  }

  def isEmpty: Boolean = drivers.isEmpty

}
