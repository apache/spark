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

import org.apache.hadoop.mapred.{FileOutputCommitter, TaskAttemptContext}
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite, TaskContext}

/**
 * Integration tests for the OutputCommitCoordinator.
 *
 * See also: [[OutputCommitCoordinatorSuite]] for unit tests that use mocks.
 */
class OutputCommitCoordinatorIntegrationSuite
  extends SparkFunSuite
  with LocalSparkContext
  with TimeLimits {

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf()
      .set("spark.hadoop.outputCommitCoordination.enabled", "true")
      .set("spark.hadoop.mapred.output.committer.class",
        classOf[ThrowExceptionOnFirstAttemptOutputCommitter].getCanonicalName)
    sc = new SparkContext("local[2, 4]", "test", conf)
  }

  test("exception thrown in OutputCommitter.commitTask()") {
    // Regression test for SPARK-10381
    failAfter(Span(60, Seconds)) {
      withTempDir { tempDir =>
        sc.parallelize(1 to 4, 2).map(_.toString).saveAsTextFile(tempDir.getAbsolutePath + "/out")
      }
    }
  }
}

private class ThrowExceptionOnFirstAttemptOutputCommitter extends FileOutputCommitter {
  override def commitTask(context: TaskAttemptContext): Unit = {
    val ctx = TaskContext.get()
    if (ctx.attemptNumber() < 1) {
      throw new java.io.FileNotFoundException("Intentional exception")
    }
    super.commitTask(context)
  }
}
