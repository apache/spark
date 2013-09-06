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

import org.apache.spark.deploy.ApplicationDescription
import java.util.Date
import akka.actor.ActorRef
import scala.collection.mutable

private[spark] class ApplicationInfo(
    val startTime: Long,
    val id: String,
    val desc: ApplicationDescription,
    val submitDate: Date,
    val driver: ActorRef,
    val appUiUrl: String)
{
  var state = ApplicationState.WAITING
  var executors = new mutable.HashMap[Int, ExecutorInfo]
  var coresGranted = 0
  var endTime = -1L
  val appSource = new ApplicationSource(this)

  private var nextExecutorId = 0

  def newExecutorId(): Int = {
    val id = nextExecutorId
    nextExecutorId += 1
    id
  }

  def addExecutor(worker: WorkerInfo, cores: Int): ExecutorInfo = {
    val exec = new ExecutorInfo(newExecutorId(), this, worker, cores, desc.memoryPerSlave)
    executors(exec.id) = exec
    coresGranted += cores
    exec
  }

  def removeExecutor(exec: ExecutorInfo) {
    if (executors.contains(exec.id)) {
      executors -= exec.id
      coresGranted -= exec.cores
    }
  }

  def coresLeft: Int = desc.maxCores - coresGranted

  private var _retryCount = 0

  def retryCount = _retryCount

  def incrementRetryCount = {
    _retryCount += 1
    _retryCount
  }

  def markFinished(endState: ApplicationState.Value) {
    state = endState
    endTime = System.currentTimeMillis()
  }

  def duration: Long = {
    if (endTime != -1) {
      endTime - startTime
    } else {
      System.currentTimeMillis() - startTime
    }
  }

}
