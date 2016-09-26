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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.util.ManualClock

class TaskSetBlacklistSuite extends SparkFunSuite {

  test("Blacklisting individual tasks") {
    val conf = new SparkConf().setAppName("test").setMaster("local")
      .set(config.BLACKLIST_ENABLED.key, "true")
    val clock = new ManualClock

    val taskSetBlacklist = new TaskSetBlacklist(conf, stageId = 0, clock = clock)
    clock.setTime(0)
    taskSetBlacklist.updateBlacklistForFailedTask("hostA", exec = "exec1", index = 0)
    for {
      executor <- (1 to 4).map(_.toString)
      index <- 0 until 10
    } {
      val exp = (executor == "exec1" && index == 0)
      assert(taskSetBlacklist.isExecutorBlacklistedForTask(executor, index) === exp)
    }
    assert(!taskSetBlacklist.isExecutorBlacklistedForTaskSet("exec1"))
    assert(!taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))

    // Task 1 & 2 failed on both executor 1 & 2, so we should blacklist all executors on that host,
    // for all tasks for the stage.  Note the api expects multiple checks for each type of
    // blacklist -- this actually fits naturally with its use in the scheduler.
    taskSetBlacklist.updateBlacklistForFailedTask("hostA", exec = "exec1", index = 1)
    assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("exec1"))
    assert(!taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    taskSetBlacklist.updateBlacklistForFailedTask("hostA", exec = "exec2", index = 0)
    assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("exec1"))
    assert(!taskSetBlacklist.isExecutorBlacklistedForTaskSet("exec2"))
    assert(!taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    taskSetBlacklist.updateBlacklistForFailedTask("hostA", exec = "exec2", index = 1)
    assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("exec1"))
    assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("exec2"))
    assert(taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    for {
      executor <- (1 to 4).map(e => s"exec$e")
      index <- 0 until 10
    } {
      withClue(s"exec = $executor; index = $index") {
        val badExec = (executor == "exec1" || executor == "exec2")
        val badIndex = (index == 0 || index == 1)
        assert(
          taskSetBlacklist.isExecutorBlacklistedForTask(executor, index) === (badExec && badIndex))
        assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet(executor) === badExec)
      }
    }
    assert(taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    val execToFailures = taskSetBlacklist.execToFailures
    assert(execToFailures.keySet === Set("exec1", "exec2"))

    val expectedExpiryTime = BlacklistTracker.getBlacklistTimeout(conf)
    Seq("exec1", "exec2").foreach { exec =>
      assert(
        execToFailures(exec).taskToFailureCountAndExpiryTime === Map(
          0 -> (1, expectedExpiryTime),
          1 -> (1, expectedExpiryTime)
        )
      )
    }
  }

}
