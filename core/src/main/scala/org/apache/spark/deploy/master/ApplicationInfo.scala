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

import java.util.Date

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.deploy.ApplicationDescription
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

private[spark] class ApplicationInfo(
    val startTime: Long,
    val id: String,
    val desc: ApplicationDescription,
    val submitDate: Date,
    val driver: RpcEndpointRef,
    defaultCores: Int)
  extends Serializable {

  @transient var state: ApplicationState.Value = _
  @transient var executors: mutable.HashMap[Int, ExecutorDesc] = _
  @transient var removedExecutors: ArrayBuffer[ExecutorDesc] = _
  @transient var coresGranted: Int = _
  @transient var endTime: Long = _
  @transient var appSource: ApplicationSource = _

  // A cap on the number of executors this application can have at any given time.
  // By default, this is infinite. Only after the first allocation request is issued by the
  // application will this be set to a finite value. This is used for dynamic allocation.
  @transient private[master] var executorLimit: Int = _

  @transient private var nextExecutorId: Int = _

  init()

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init() {
    state = ApplicationState.WAITING
    executors = new mutable.HashMap[Int, ExecutorDesc]
    coresGranted = 0
    endTime = -1L
    appSource = new ApplicationSource(this)
    nextExecutorId = 0
    removedExecutors = new ArrayBuffer[ExecutorDesc]
    executorLimit = desc.initialExecutorLimit.getOrElse(Integer.MAX_VALUE)
  }

  private def newExecutorId(useID: Option[Int] = None): Int = {
    useID match {
      case Some(id) =>
        nextExecutorId = math.max(nextExecutorId, id + 1)
        id
      case None =>
        val id = nextExecutorId
        nextExecutorId += 1
        id
    }
  }

  private[master] def addExecutor(
      worker: WorkerInfo,
      cores: Int,
      useID: Option[Int] = None): ExecutorDesc = {
    val exec = new ExecutorDesc(newExecutorId(useID), this, worker, cores, desc.memoryPerExecutorMB)
    executors(exec.id) = exec
    coresGranted += cores
    exec
  }

  private[master] def removeExecutor(exec: ExecutorDesc) {
    if (executors.contains(exec.id)) {
      removedExecutors += executors(exec.id)
      executors -= exec.id
      coresGranted -= exec.cores
    }
  }

  private val requestedCores = desc.maxCores.getOrElse(defaultCores)

  private[master] def coresLeft: Int = requestedCores - coresGranted

  private var _retryCount = 0

  private[master] def retryCount = _retryCount

  private[master] def incrementRetryCount() = {
    _retryCount += 1
    _retryCount
  }

  private[master] def resetRetryCount() = _retryCount = 0

  private[master] def markFinished(endState: ApplicationState.Value) {
    state = endState
    endTime = System.currentTimeMillis()
  }

  private[master] def isFinished: Boolean = {
    state != ApplicationState.WAITING && state != ApplicationState.RUNNING
  }

  /**
   * Return the limit on the number of executors this application can have.
   * For testing only.
   */
  private[deploy] def getExecutorLimit: Int = executorLimit

  def duration: Long = {
    if (endTime != -1) {
      endTime - startTime
    } else {
      System.currentTimeMillis() - startTime
    }
  }
}
