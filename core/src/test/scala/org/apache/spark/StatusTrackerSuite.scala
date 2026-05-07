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

import org.scalatest.concurrent.Eventually._
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.JobExecutionStatus._

class StatusTrackerSuite extends SparkFunSuite with Matchers with LocalSparkContext {

  testRetry("basic status API usage") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val jobFuture = sc.parallelize(1 to 10000, 2).map(identity).groupBy(identity).collectAsync()
    val jobId: Int = eventually(timeout(10.seconds)) {
      val jobIds = jobFuture.jobIds
      jobIds.size should be(1)
      jobIds.head
    }
    val jobInfo = eventually(timeout(10.seconds)) {
      sc.statusTracker.getJobInfo(jobId).get
    }
    jobInfo.status() should not be FAILED
    val stageIds = jobInfo.stageIds()
    stageIds.size should be(2)

    val firstStageInfo = eventually(timeout(10.seconds)) {
      sc.statusTracker.getStageInfo(stageIds.min).get
    }
    firstStageInfo.stageId() should be(stageIds.min)
    firstStageInfo.currentAttemptId() should be(0)
    firstStageInfo.numTasks() should be(2)
    eventually(timeout(10.seconds)) {
      val updatedFirstStageInfo = sc.statusTracker.getStageInfo(stageIds.min).get
      updatedFirstStageInfo.numCompletedTasks() should be(2)
      updatedFirstStageInfo.numActiveTasks() should be(0)
      updatedFirstStageInfo.numFailedTasks() should be(0)
    }
  }

  test("getJobIdsForGroup()") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    // Passing `null` should return jobs that were not run in a job group:
    val defaultJobGroupFuture = sc.parallelize(1 to 1000).countAsync()
    val defaultJobGroupJobId = eventually(timeout(10.seconds)) {
      defaultJobGroupFuture.jobIds.head
    }
    eventually(timeout(10.seconds)) {
      sc.statusTracker.getJobIdsForGroup(null).toSet should be (Set(defaultJobGroupJobId))
    }
    // Test jobs submitted in job groups:
    sc.setJobGroup("my-job-group", "description")
    sc.statusTracker.getJobIdsForGroup("my-job-group") should be (Seq.empty)
    val firstJobFuture = sc.parallelize(1 to 1000).countAsync()
    val firstJobId = eventually(timeout(10.seconds)) {
      firstJobFuture.jobIds.head
    }
    eventually(timeout(10.seconds)) {
      sc.statusTracker.getJobIdsForGroup("my-job-group") should be (Seq(firstJobId))
    }
    val secondJobFuture = sc.parallelize(1 to 1000).countAsync()
    val secondJobId = eventually(timeout(10.seconds)) {
      secondJobFuture.jobIds.head
    }
    eventually(timeout(10.seconds)) {
      sc.statusTracker.getJobIdsForGroup("my-job-group").toSet should be (
        Set(firstJobId, secondJobId))
    }
  }

  test("getJobIdsForGroup() with takeAsync()") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    sc.setJobGroup("my-job-group2", "description")
    sc.statusTracker.getJobIdsForGroup("my-job-group2") shouldBe empty
    val firstJobFuture = sc.parallelize(1 to 1000, 1).takeAsync(1)
    val firstJobId = eventually(timeout(10.seconds)) {
      firstJobFuture.jobIds.head
    }
    eventually(timeout(10.seconds)) {
      sc.statusTracker.getJobIdsForGroup("my-job-group2") should be (Seq(firstJobId))
    }
  }

  test("getJobIdsForGroup() with takeAsync() across multiple partitions") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    sc.setJobGroup("my-job-group2", "description")
    sc.statusTracker.getJobIdsForGroup("my-job-group2") shouldBe empty
    val firstJobFuture = sc.parallelize(1 to 1000, 2).takeAsync(999)
    eventually(timeout(10.seconds)) {
      firstJobFuture.jobIds.head
    }
    eventually(timeout(10.seconds)) {
      sc.statusTracker.getJobIdsForGroup("my-job-group2") should have size 2
    }
  }

  test("getJobIdsForTag()") {
    sc = new SparkContext("local", "test", new SparkConf(false))

    sc.addJobTag("tag1")
    sc.statusTracker.getJobIdsForTag("tag1") should be (Seq.empty)

    // countAsync()
    val firstJobFuture = sc.parallelize(1 to 1000).countAsync()
    val firstJobId = eventually(timeout(10.seconds)) {
      firstJobFuture.jobIds.head
    }
    eventually(timeout(10.seconds)) {
      sc.statusTracker.getJobIdsForTag("tag1") should be (Seq(firstJobId))
    }

    sc.addJobTag("tag2")
    // takeAsync()
    val secondJobFuture = sc.parallelize(1 to 1000).takeAsync(1)
    val secondJobId = eventually(timeout(10.seconds)) {
      secondJobFuture.jobIds.head
    }
    eventually(timeout(10.seconds)) {
      sc.statusTracker.getJobIdsForTag("tag1").toSet should be (
        Set(firstJobId, secondJobId))
      sc.statusTracker.getJobIdsForTag("tag2") should be (Seq(secondJobId))
    }

    sc.removeJobTag("tag1")

    // takeAsync() across multiple partitions
    val thirdJobFuture = sc.parallelize(1 to 1000, 2).takeAsync(999)
    val thirdJobIds = eventually(timeout(10.seconds)) {
      // Wait for the two jobs triggered by takeAsync
      thirdJobFuture.jobIds.size should be(2)
      thirdJobFuture.jobIds
    }
    eventually(timeout(10.seconds)) {
      sc.statusTracker.getJobIdsForTag("tag1").toSet should be (
        Set(firstJobId, secondJobId))
      sc.statusTracker.getJobIdsForTag("tag2").toSet should be (
        Set(secondJobId) ++ thirdJobIds)
    }
  }
}
