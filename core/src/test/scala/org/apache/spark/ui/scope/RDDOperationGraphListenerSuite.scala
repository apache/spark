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

package org.apache.spark.ui.scope

import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListenerJobStart, SparkListenerStageSubmitted, StageInfo}

class RDDOperationGraphListenerSuite extends FunSuite {
  private var jobIdCounter = 0
  private var stageIdCounter = 0

  /** Run a job with the specified number of stages. */
  private def runOneJob(numStages: Int, listener: RDDOperationGraphListener): Unit = {
    assert(numStages > 0, "I will not run a job with 0 stages for you.")
    val stageInfos = (0 until numStages).map { _ =>
      val stageInfo = new StageInfo(stageIdCounter, 0, "s", 0, Seq.empty, Seq.empty, "d")
      stageIdCounter += 1
      stageInfo
    }
    listener.onJobStart(new SparkListenerJobStart(jobIdCounter, 0, stageInfos))
    jobIdCounter += 1
  }

  test("listener cleans up metadata") {

    val conf = new SparkConf()
      .set("spark.ui.retainedStages", "10")
      .set("spark.ui.retainedJobs", "10")

    val listener = new RDDOperationGraphListener(conf)
    assert(listener.jobIdToStageIds.isEmpty)
    assert(listener.stageIdToGraph.isEmpty)
    assert(listener.jobIds.isEmpty)
    assert(listener.stageIds.isEmpty)

    // Run a few jobs, but not enough for clean up yet
    runOneJob(1, listener)
    runOneJob(2, listener)
    runOneJob(3, listener)
    assert(listener.jobIdToStageIds.size === 3)
    assert(listener.stageIdToGraph.size === 6)
    assert(listener.jobIds.size === 3)
    assert(listener.stageIds.size === 6)

    // Run a few more, but this time the stages should be cleaned up, but not the jobs
    runOneJob(5, listener)
    runOneJob(100, listener)
    assert(listener.jobIdToStageIds.size === 5)
    assert(listener.stageIdToGraph.size === 9)
    assert(listener.jobIds.size === 5)
    assert(listener.stageIds.size === 9)

    // Run a few more, but this time both jobs and stages should be cleaned up
    (1 to 100).foreach { _ =>
      runOneJob(1, listener)
    }
    assert(listener.jobIdToStageIds.size === 9)
    assert(listener.stageIdToGraph.size === 9)
    assert(listener.jobIds.size === 9)
    assert(listener.stageIds.size === 9)

    // Ensure we clean up old jobs and stages, not arbitrary ones
    assert(!listener.jobIdToStageIds.contains(0))
    assert(!listener.stageIdToGraph.contains(0))
    assert(!listener.stageIds.contains(0))
    assert(!listener.jobIds.contains(0))
  }

}
