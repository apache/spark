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

import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps

import org.scalatest.{Matchers, FunSuite}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.JobExecutionStatus._
import org.apache.spark.SparkContext._

class StatusAPISuite extends FunSuite with Matchers with SharedSparkContext {

  test("basic status API usage") {
    val jobFuture = sc.parallelize(1 to 10000, 2).map(identity).groupBy(identity).collectAsync()
    val jobId: Int = eventually(timeout(10 seconds)) {
      val jobIds = jobFuture.jobIds
      jobIds.size should be(1)
      jobIds.head
    }
    val jobInfo = eventually(timeout(10 seconds)) {
      sc.getJobInfo(jobId).get
    }
    jobInfo.status() should not be FAILED
    val stageIds = jobInfo.stageIds()
    stageIds.size should be(2)

    val firstStageInfo = eventually(timeout(10 seconds)) {
      sc.getStageInfo(stageIds(0)).get
    }
    firstStageInfo.stageId() should be(stageIds(0))
    firstStageInfo.currentAttemptId() should be(0)
    firstStageInfo.numTasks() should be(2)
    eventually(timeout(10 seconds)) {
      val updatedFirstStageInfo = sc.getStageInfo(stageIds(0)).get
      updatedFirstStageInfo.numCompletedTasks() should be(2)
      updatedFirstStageInfo.numActiveTasks() should be(0)
      updatedFirstStageInfo.numFailedTasks() should be(0)
    }
  }

  test("getJobIdsForGroup()") {
    sc.setJobGroup("my-job-group", "description")
    sc.getJobIdsForGroup("my-job-group") should be (Seq.empty)
    val firstJobFuture = sc.parallelize(1 to 1000).countAsync()
    val firstJobId = eventually(timeout(10 seconds)) {
      firstJobFuture.jobIds.head
    }
    eventually(timeout(10 seconds)) {
      sc.getJobIdsForGroup("my-job-group") should be (Seq(firstJobId))
    }
    val secondJobFuture = sc.parallelize(1 to 1000).countAsync()
    val secondJobId = eventually(timeout(10 seconds)) {
      secondJobFuture.jobIds.head
    }
    eventually(timeout(10 seconds)) {
      sc.getJobIdsForGroup("my-job-group").toSet should be (Set(firstJobId, secondJobId))
    }
  }
}