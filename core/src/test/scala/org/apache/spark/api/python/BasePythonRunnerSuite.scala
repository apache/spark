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

import org.apache.spark.SparkFunSuite

class BasePythonRunnerSuite extends SparkFunSuite {

  test("SPARK-58192: pyspark memory is split across the executor's task slots") {
    def workerMemoryMb(execCores: Int, taskCpus: String): Option[Long] =
      BasePythonRunner.getWorkerMemoryMb(Some(4096L), execCores, BigDecimal(taskCpus))

    // The default one cpu per task keeps the historical one-slot-per-core split.
    assert(workerMemoryMb(4, "1") === Some(1024L))
    // A fractional amount below 1 admits more concurrent tasks than cores, so each worker
    // gets a smaller share and the aggregate stays within the executor-wide allocation.
    assert(workerMemoryMb(4, "0.5") === Some(512L))
    assert(workerMemoryMb(4, "0.7") === Some(819L)) // floor(4 / 0.7) = 5 slots
    // An amount above 1 admits fewer concurrent tasks than cores.
    assert(workerMemoryMb(4, "1.5") === Some(2048L)) // floor(4 / 1.5) = 2 slots
    assert(workerMemoryMb(4, "2") === Some(2048L))
    // Never split into less than one slot, even when the amount exceeds the announced cores.
    assert(workerMemoryMb(1, "8") === Some(4096L))
    // No pyspark memory configured means no per-worker limit.
    assert(BasePythonRunner.getWorkerMemoryMb(None, 4, BigDecimal("0.5")) === None)
  }
}
