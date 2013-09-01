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

package org.apache.spark.deploy.master

import akka.actor.ActorRef
import scala.collection.mutable
import org.apache.spark.util.Utils

private[spark] class WorkerInfo(
  val id: String,
  val host: String,
  val port: Int,
  val cores: Int,
  val memory: Int,
  val actor: ActorRef,
  val webUiPort: Int,
  val publicAddress: String) {

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  var executors = new mutable.HashMap[String, ExecutorInfo]  // fullId => info
  var state: WorkerState.Value = WorkerState.ALIVE
  var coresUsed = 0
  var memoryUsed = 0

  var lastHeartbeat = System.currentTimeMillis()

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  def hostPort: String = {
    assert (port > 0)
    host + ":" + port
  }

  def addExecutor(exec: ExecutorInfo) {
    executors(exec.fullId) = exec
    coresUsed += exec.cores
    memoryUsed += exec.memory
  }

  def removeExecutor(exec: ExecutorInfo) {
    if (executors.contains(exec.fullId)) {
      executors -= exec.fullId
      coresUsed -= exec.cores
      memoryUsed -= exec.memory
    }
  }

  def hasExecutor(app: ApplicationInfo): Boolean = {
    executors.values.exists(_.application == app)
  }

  def webUiAddress : String = {
    "http://" + this.publicAddress + ":" + this.webUiPort
  }

  def setState(state: WorkerState.Value) = {
    this.state = state
  }
}
