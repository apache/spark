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

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId

class ExternalClusterManagerSuite extends SparkFunSuite with LocalSparkContext
{
  test("launch of backend and scheduler") {
    val conf = new SparkConf().setMaster("myclusterManager").
        setAppName("testcm").set("spark.driver.allowMultipleContexts", "true")
    sc = new SparkContext(conf)
    // check if the scheduler components are created
    assert(sc.schedulerBackend.isInstanceOf[DummySchedulerBackend])
    assert(sc.taskScheduler.isInstanceOf[DummyTaskScheduler])
  }
}

private class DummyExternalClusterManager extends ExternalClusterManager {

  def canCreate(masterURL: String): Boolean = masterURL == "myclusterManager"

  def createTaskScheduler(sc: SparkContext,
      masterURL: String): TaskScheduler = new DummyTaskScheduler

  def createSchedulerBackend(sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = new DummySchedulerBackend()

  def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {}

}

private class DummySchedulerBackend extends SchedulerBackend {
  def start() {}
  def stop() {}
  def reviveOffers() {}
  def defaultParallelism(): Int = 1
}

private class DummyTaskScheduler extends TaskScheduler {
  override def rootPool: Pool = null
  override def schedulingMode: SchedulingMode = SchedulingMode.NONE
  override def start(): Unit = {}
  override def stop(): Unit = {}
  override def submitTasks(taskSet: TaskSet): Unit = {}
  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = {}
  override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {}
  override def defaultParallelism(): Int = 2
  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {}
  override def applicationAttemptId(): Option[String] = None
  def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulableInfo])],
      blockManagerId: BlockManagerId): Boolean = true
}
