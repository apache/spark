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

package org.apache.spark.deploy.client

import scala.concurrent.Future

import org.apache.spark.scheduler.ExecutorDecommissionInfo

/**
 * Callbacks invoked by deploy client when various events happen. There are currently six events:
 * connecting to the cluster, disconnecting, being given an executor, having an executor removed
 * (either due to failure or due to revocation), having a worker removed, and being asked by the
 * Master to hold or resume the application.
 *
 * Users of this API should *not* block inside the callback methods.
 */
private[spark] trait StandaloneAppClientListener {
  def connected(appId: String): Unit

  /** Disconnection may be a temporary state, as we fail over to a new Master. */
  def disconnected(): Unit

  /** An application death is an unrecoverable failure condition. */
  def dead(reason: String): Unit

  def executorAdded(
      fullId: String, workerId: String, hostPort: String, cores: Int, memory: Int): Unit

  def executorRemoved(
      fullId: String, message: String, exitStatus: Option[Int], workerHost: Option[String]): Unit

  def executorDecommissioned(fullId: String, decommissionInfo: ExecutorDecommissionInfo): Unit

  def workerRemoved(workerId: String, host: String, message: String): Unit

  /**
   * Hold or resume the whole application on behalf of the Master. Holding drains the executors
   * and talks to the cluster manager, so the work is done asynchronously and the returned future
   * completes with whether the request was acknowledged.
   */
  def holdApplication(hold: Boolean): Future[Boolean]
}
