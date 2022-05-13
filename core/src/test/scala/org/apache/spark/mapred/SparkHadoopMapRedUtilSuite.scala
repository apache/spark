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

package org.apache.spark.mapred

import org.apache.hadoop.mapreduce.{JobID, OutputCommitter, TaskAttemptContext, TaskAttemptID, TaskID}
import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv, SparkFunSuite, TaskContextImpl}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.scheduler.{LiveListenerBus, OutputCommitCoordinator}

class SparkHadoopMapRedUtilSuite extends SparkFunSuite with LocalSparkContext with BeforeAndAfter {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf()
    sc = new SparkContext("local[2, 4]", "test", conf) {
      override private[spark] def createSparkEnv(
          conf: SparkConf,
          isLocal: Boolean,
          listenerBus: LiveListenerBus): SparkEnv = {
        val outputCommitCoordinator = new OutputCommitCoordinator(conf, isDriver = true) {
          override def canCommit(stage: Int, stageAttempt: Int,
              partition: Int, attemptNumber: Int): Boolean = {
            false
          }
        }

        SparkEnv.createDriverEnv(conf, isLocal, listenerBus,
          SparkContext.numDriverCores(master), Some(outputCommitCoordinator))
      }
    }
  }

  test("Commit task be denied, because the driver did not authorize commit ") {
    val taskContext = mock(classOf[TaskContextImpl])
    withTaskContext(taskContext) {
      val taskAttemptContext = mock(classOf[TaskAttemptContext])

      val attemptId = mock(classOf[TaskAttemptID])
      when(taskAttemptContext.getTaskAttemptID).thenReturn(attemptId)

      val jobId = mock(classOf[JobID])
      when(attemptId.getJobID).thenReturn(jobId)

      val taskId = mock(classOf[TaskID])
      when(attemptId.getTaskID).thenReturn(taskId)

      val committer = mock(classOf[OutputCommitter])
      when(committer.needsTaskCommit(taskAttemptContext)).thenReturn(true)

      val e = intercept[CommitDeniedException](
        SparkHadoopMapRedUtil.commitTask(committer, taskAttemptContext,
          taskAttemptContext.getTaskAttemptID.getJobID.getId,
          taskAttemptContext.getTaskAttemptID.getTaskID.getId)
      )

      assert(e.getErrorClass === "COMMIT_DENIED")
      assert(e.getMessage ===
        "[COMMIT_DENIED] Commit denied for partition 0 (task 0, attempt 0, stage 0.0)")
    }
  }
}
