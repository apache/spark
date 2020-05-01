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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{LocalSparkContext, SparkContext, SparkFunSuite}

class JobCleanedHookListenerSuite extends SparkFunSuite with LocalSparkContext {

  test("JobCleanedHook was called once for each job") {
    sc = new SparkContext("local", "CheckJobCleanedAfterJobRemoved")
    val counter = new AtomicInteger(0)
    var currentJobId = sc.dagScheduler.nextJobId.get()
    sc.jobCleanedHookListener.addCleanedHook(currentJobId, jobId => counter.set(jobId))
    sc.parallelize(1 to 100).count()
    assert(sc.dagScheduler.jobIdToActiveJob.get(currentJobId).isEmpty)
    assert(sc.dagScheduler.nextJobId.get() == currentJobId + 1)
    assert(counter.get() == 0)

    currentJobId = sc.dagScheduler.nextJobId.get()
    sc.jobCleanedHookListener.addCleanedHook(currentJobId, jobId => counter.set(jobId))
    sc.parallelize(1 to 100).count()
    assert(sc.dagScheduler.jobIdToActiveJob.get(currentJobId).isEmpty)
    assert(sc.dagScheduler.nextJobId.get() == currentJobId + 1)
    assert(counter.get() == 1)
  }

  test("JobCleanedHood was called even if job failed") {
    sc = new SparkContext("local", "CheckJobCleanedAfterJobFailed")
    val counter = new AtomicInteger(0)
    var currentJobId = sc.dagScheduler.nextJobId.get()
    sc.jobCleanedHookListener.addCleanedHook(currentJobId, jobId => counter.set(jobId))
    try {
      sc.parallelize(1 to 100).map { index =>
        if (index > 80) {
          throw new Exception(s"current $index has run Failed")
        }
      }.count()
    } catch {
      case e: Exception =>
        // Nothing to be done
    }
    assert(sc.dagScheduler.jobIdToActiveJob.get(currentJobId).isEmpty)
    assert(sc.dagScheduler.nextJobId.get() == currentJobId + 1)
    assert(counter.get() == 0)
  }
}
