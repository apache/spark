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

package org.apache.spark

/**
 * A client that communicates with the cluster manager to request or kill executors.
 * This is currently supported only in YARN mode.
 */
private[spark] trait ExecutorAllocationClient {


  /** Get the list of currently active executors */
  private[spark] def getExecutorIds(): Seq[String]

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
   * @param numExecutors The total number of executors we'd like to have. The cluster manager
   *                     shouldn't kill any running executor to reach this number, but,
   *                     if all existing executors were to die, this is the number of executors
   *                     we'd want to be allocated.
   * @param localityAwareTasks The number of tasks in all active stages that have a locality
   *                           preferences. This includes running, pending, and completed tasks.
   * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
   *                             that would like to like to run on that host.
   *                             This includes running, pending, and completed tasks.
   * @return whether the request is acknowledged by the cluster manager.
   */
  private[spark] def requestTotalExecutors(
      numExecutors: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int]): Boolean

  /**
   * Request an additional number of executors from the cluster manager.
   * @return whether the request is acknowledged by the cluster manager.
   */
  def requestExecutors(numAdditionalExecutors: Int): Boolean

  /**
   * Request that the cluster manager kill the specified executors.
   * @return the ids of the executors acknowledged by the cluster manager to be removed.
   */
  def killExecutors(executorIds: Seq[String]): Seq[String]

  /**
   * Request that the cluster manager kill the specified executor.
   * @return whether the request is acknowledged by the cluster manager.
   */
  def killExecutor(executorId: String): Boolean = {
    val killedExecutors = killExecutors(Seq(executorId))
    killedExecutors.nonEmpty && killedExecutors(0).equals(executorId)
  }
}
