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

package org.apache.spark.api.python

import org.apache.spark.{SparkException, SparkFunSuite}

class BasePythonRunnerSuite extends SparkFunSuite {

  test("SPARK-58192: pyspark memory is split across the executor's concurrent task slots") {
    def workerMemoryMb(maxConcurrentTasks: Int): Option[Long] =
      BasePythonRunner.getWorkerMemoryMb(Some(4096L), maxConcurrentTasks)

    // One slot per concurrent task; each worker gets an equal share of the executor allocation.
    assert(workerMemoryMb(4) === Some(1024L))
    // A fractional spark.task.cpus (e.g. 4 cores / 0.5) admits more concurrent tasks than cores,
    // so each worker gets a smaller share and the aggregate stays within the allocation.
    assert(workerMemoryMb(8) === Some(512L))
    assert(workerMemoryMb(5) === Some(819L))
    // The concurrency is the limiting resource, not just cpu slots: a GPU that caps the executor
    // at a single concurrent task means the sole worker keeps the whole allocation, even though
    // the cpu-slot count (e.g. 64 cores / 0.1 = 640) is far higher.
    assert(workerMemoryMb(1) === Some(4096L))
    // Never split into less than one slot.
    assert(BasePythonRunner.getWorkerMemoryMb(Some(4096L), 0) === Some(4096L))
    // No pyspark memory configured means no per-worker limit.
    assert(BasePythonRunner.getWorkerMemoryMb(None, 8) === None)
  }

  test("SPARK-58192: fail fast when the per-slot pyspark memory share rounds to zero") {
    // 512 MiB across 640 genuinely concurrent tasks rounds down to 0, which the worker would
    // treat as "no limit". Fail fast rather than silently dropping the configured cap.
    val e = intercept[SparkException] {
      BasePythonRunner.getWorkerMemoryMb(Some(512L), 640)
    }
    assert(e.getMessage.contains("spark.executor.pyspark.memory"))
    assert(e.getMessage.contains("640"))
    // A share of exactly 1 MiB is still enforceable and must not fail.
    assert(BasePythonRunner.getWorkerMemoryMb(Some(640L), 640) === Some(1L))
    // An explicit spark.executor.pyspark.memory=0 means the limit is disabled (the worker
    // treats 0 as "no limit"), so it must pass through rather than fail fast, even when
    // concurrency is high enough that a positive budget would round to zero.
    assert(BasePythonRunner.getWorkerMemoryMb(Some(0L), 640) === Some(0L))
    assert(BasePythonRunner.getWorkerMemoryMb(Some(0L), 1) === Some(0L))
    // The smallest positive budget still fails fast when it cannot give every slot at least
    // 1 MiB; only an explicit budget of 0 disables the limit.
    intercept[SparkException] {
      BasePythonRunner.getWorkerMemoryMb(Some(1L), 2)
    }
  }
}
