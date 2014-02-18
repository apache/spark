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

/**
 * Callbacks invoked by deploy client when various events happen. There are currently four events:
 * connecting to the cluster, disconnecting, being given an executor, and having an executor
 * removed (either due to failure or due to revocation).
 *
 * Users of this API should *not* block inside the callback methods.
 */
private[spark] trait AppClientListener {
  def connected(appId: String): Unit

  /** Disconnection may be a temporary state, as we fail over to a new Master. */
  def disconnected(): Unit

  /** Dead means that we couldn't find any Masters to connect to, and have given up. */
  def dead(): Unit

  def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int, memory: Int)

  def executorRemoved(fullId: String, message: String, exitStatus: Option[Int]): Unit
}
