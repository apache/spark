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

  /**
   * Express a preference to the cluster manager for a given total number of executors.
   * This can result in canceling pending requests or filing additional requests.
   * Return whether the request is acknowledged by the cluster manager.
   */
  private[spark] def requestTotalExecutors(numExecutors: Int): Boolean

  /**
   * Request an additional number of executors from the cluster manager.
   * Return whether the request is acknowledged by the cluster manager.
   */
  def requestExecutors(numAdditionalExecutors: Int): Boolean

  /**
   * Request that the cluster manager kill the specified executors.
   * Return whether the request is acknowledged by the cluster manager.
   */
  def killExecutors(executorIds: Seq[String]): Boolean

  /**
   * Request that the cluster manager kill the specified executor.
   * Return whether the request is acknowledged by the cluster manager.
   */
  def killExecutor(executorId: String): Boolean = killExecutors(Seq(executorId))
}
