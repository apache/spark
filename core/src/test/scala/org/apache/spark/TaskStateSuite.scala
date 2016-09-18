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

import org.scalatest.Matchers

import org.apache.spark.TaskState._

class TaskStateSuite extends SparkFunSuite with Matchers {

  test("Task State should be failed if it is lost or failed") {
    isFailed(LOST) should be (true)
    isFailed(FAILED) should be (true)
  }

  test("Task State should not be failed if it is launching, running, finished or killed") {
    Set(LAUNCHING, RUNNING, FINISHED, KILLED)
      .foreach(taskState => isFailed(taskState) should be (false))
  }

  test("Task State should be finished if it is finished, failed, killed or lost") {
    Set(FINISHED, FAILED, KILLED, LOST).foreach(taskState => isFinished(taskState) should be (true))
  }

  test("Task State should not be finished if it is launching or running") {
    isFinished(LAUNCHING) should be (false)
    isFinished(RUNNING) should be (false)
  }

}
