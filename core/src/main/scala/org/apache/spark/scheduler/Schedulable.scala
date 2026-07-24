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

package org.apache.spark.scheduler

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * :: DeveloperApi ::
 * An interface for schedulable entities (there are two types: Pools and TaskSetManagers).
 *
 * Only the read-only scheduling properties below are part of the public API surface; they are the
 * inputs a custom [[SchedulingAlgorithm]] can inspect in its `comparator`. The structural and
 * mutating members are Spark-internal (`private[spark]`) and must not be used by applications.
 */
@DeveloperApi
trait Schedulable {
  def schedulingMode: SchedulingMode
  def weight: Int
  def minShare: Int
  def runningTasks: Int
  def priority: Int
  def stageId: Int
  def name: String

  private[spark] var parent: Pool
  // child queues
  private[spark] def schedulableQueue: ConcurrentLinkedQueue[Schedulable]
  private[spark] def isSchedulable: Boolean
  private[spark] def addSchedulable(schedulable: Schedulable): Unit
  private[spark] def removeSchedulable(schedulable: Schedulable): Unit
  private[spark] def getSchedulableByName(name: String): Schedulable
  private[spark] def executorLost(
      executorId: String, host: String, reason: ExecutorLossReason): Unit
  private[spark] def executorDecommission(executorId: String): Unit
  private[spark] def checkSpeculatableTasks(minTimeToSpeculation: Long): Boolean
  private[spark] def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager]
}
