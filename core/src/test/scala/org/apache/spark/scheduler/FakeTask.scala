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

import org.apache.spark.TaskContext

class FakeTask(
    stageId: Int,
    partitionId: Int,
    prefLocs: Seq[TaskLocation] = Nil) extends Task[Int](stageId, 0, partitionId) {
  override def runTask(context: TaskContext): Int = 0
  override def preferredLocations: Seq[TaskLocation] = prefLocs
}

object FakeTask {
  /**
   * Utility method to create a TaskSet, potentially setting a particular sequence of preferred
   * locations for each task (given as varargs) if this sequence is not empty.
   */
  def createTaskSet(numTasks: Int, prefLocs: Seq[TaskLocation]*): TaskSet = {
    createTaskSet(numTasks, 0, prefLocs: _*)
  }

  def createTaskSet(numTasks: Int, stageAttemptId: Int, prefLocs: Seq[TaskLocation]*): TaskSet = {
    createTaskSet(numTasks, 0, stageAttemptId, prefLocs: _*)
  }

  def createTaskSet(numTasks: Int, stageId: Int, stageAttemptId: Int, prefLocs: Seq[TaskLocation]*):
  TaskSet = {
    if (prefLocs.size != 0 && prefLocs.size != numTasks) {
      throw new IllegalArgumentException("Wrong number of task locations")
    }
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new FakeTask(stageId, i, if (prefLocs.size != 0) prefLocs(i) else Nil)
    }
    new TaskSet(tasks, stageId, stageAttemptId, 0, null)
  }
}
