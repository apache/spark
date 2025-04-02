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

import java.util.Properties

import org.apache.spark.{JobArtifactSet, Partition, SparkEnv, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.resource.ResourceProfile

class FakeTask(
    stageId: Int,
    partitionId: Int,
    prefLocs: Seq[TaskLocation] = Nil,
    isBarrier: Boolean = false,
    // This has to be a `val`, so that the accumulators of `TaskMetrics` will be referenced and not
    // GCed before the stage is completed.
    private val fakeTaskMetrics: TaskMetrics = TaskMetrics.registered)
  extends Task[Int](
    stageId,
    0,
    partitionId,
    1,
    JobArtifactSet.defaultJobArtifactSet,
    new Properties,
    SparkEnv.get.closureSerializer.newInstance().serialize(fakeTaskMetrics).array(),
    isBarrier = isBarrier) {

  override def runTask(context: TaskContext): Int = 0
  override def preferredLocations: Seq[TaskLocation] = prefLocs
}

object FakeTask {
  /**
   * Utility method to create a TaskSet, potentially setting a particular sequence of preferred
   * locations for each task (given as varargs) if this sequence is not empty.
   */
  def createTaskSet(numTasks: Int, prefLocs: Seq[TaskLocation]*): TaskSet = {
    createTaskSet(numTasks, stageId = 0, stageAttemptId = 0, priority = 0,
      ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, prefLocs: _*)
  }

  def createTaskSet(numTasks: Int, rpId: Int, prefLocs: Seq[TaskLocation]*): TaskSet = {
    createTaskSet(numTasks, stageId = 0, stageAttemptId = 0, priority = 0, rpId, prefLocs: _*)
  }

  def createTaskSet(
      numTasks: Int,
      stageId: Int,
      stageAttemptId: Int,
      prefLocs: Seq[TaskLocation]*): TaskSet = {
    createTaskSet(numTasks, stageId, stageAttemptId, priority = 0,
      ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, prefLocs: _*)
  }

  def createTaskSet(
      numTasks: Int,
      stageId: Int,
      stageAttemptId: Int,
      priority: Int,
      rpId: Int,
      prefLocs: Seq[TaskLocation]*): TaskSet = {
    if (prefLocs.size != 0 && prefLocs.size != numTasks) {
      throw new IllegalArgumentException("Wrong number of task locations")
    }
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new FakeTask(stageId, i, if (prefLocs.size != 0) prefLocs(i) else Nil)
    }
    new TaskSet(tasks, stageId, stageAttemptId, priority = priority, null, rpId, None)
  }

  def createShuffleMapTaskSet(
      numTasks: Int,
      stageId: Int,
      stageAttemptId: Int,
      prefLocs: Seq[TaskLocation]*): TaskSet = {
    createShuffleMapTaskSet(numTasks, stageId, stageAttemptId, priority = 0, prefLocs: _*)
  }

  def createShuffleMapTaskSet(
      numTasks: Int,
      stageId: Int,
      stageAttemptId: Int,
      priority: Int,
      prefLocs: Seq[TaskLocation]*): TaskSet = {
    if (prefLocs.size != 0 && prefLocs.size != numTasks) {
      throw new IllegalArgumentException("Wrong number of task locations")
    }
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new ShuffleMapTask(stageId, stageAttemptId, null, new Partition {
        override def index: Int = i
      }, 1, prefLocs(i), JobArtifactSet.defaultJobArtifactSet, new Properties,
        SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array())
    }
    new TaskSet(tasks, stageId, stageAttemptId, priority = priority, null,
      ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, Some(0))
  }

  def createBarrierTaskSet(numTasks: Int, prefLocs: Seq[TaskLocation]*): TaskSet = {
    createBarrierTaskSet(numTasks, stageId = 0, stageAttemptId = 0, priority = 0,
      rpId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, prefLocs: _*)
  }

  def createBarrierTaskSet(
      numTasks: Int,
      rpId: Int,
      prefLocs: Seq[TaskLocation]*): TaskSet = {
    createBarrierTaskSet(numTasks, stageId = 0, stageAttemptId = 0, priority = 0,
      rpId = rpId, prefLocs: _*)
  }

  def createBarrierTaskSet(
      numTasks: Int,
      stageId: Int,
      stageAttemptId: Int,
      priority: Int,
      rpId: Int,
      prefLocs: Seq[TaskLocation]*): TaskSet = {
    if (prefLocs.size != 0 && prefLocs.size != numTasks) {
      throw new IllegalArgumentException("Wrong number of task locations")
    }
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new FakeTask(stageId, i, if (prefLocs.size != 0) prefLocs(i) else Nil, isBarrier = true)
    }
    new TaskSet(tasks, stageId, stageAttemptId, priority = priority, null, rpId, None)
  }
}
