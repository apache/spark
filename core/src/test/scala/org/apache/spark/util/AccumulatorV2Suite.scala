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

package org.apache.spark.util

import java.util.Properties

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics

class AccumulatorV2Suite extends SparkFunSuite {

  test("LongAccumulator add/avg/sum/count/isZero") {
    val acc = new LongAccumulator
    assert(acc.isZero)
    assert(acc.count == 0)
    assert(acc.sum == 0)
    assert(acc.avg.isNaN)

    acc.add(0)
    assert(!acc.isZero)
    assert(acc.count == 1)
    assert(acc.sum == 0)
    assert(acc.avg == 0.0)

    acc.add(1)
    assert(acc.count == 2)
    assert(acc.sum == 1)
    assert(acc.avg == 0.5)

    // Also test add using non-specialized add function
    acc.add(java.lang.Long.valueOf(2))
    assert(acc.count == 3)
    assert(acc.sum == 3)
    assert(acc.avg == 1.0)

    // Test merging
    val acc2 = new LongAccumulator
    acc2.add(2)
    acc.merge(acc2)
    assert(acc.count == 4)
    assert(acc.sum == 5)
    assert(acc.avg == 1.25)
  }

  test("DoubleAccumulator add/avg/sum/count/isZero") {
    val acc = new DoubleAccumulator
    assert(acc.isZero)
    assert(acc.count == 0)
    assert(acc.sum == 0.0)
    assert(acc.avg.isNaN)

    acc.add(0.0)
    assert(!acc.isZero)
    assert(acc.count == 1)
    assert(acc.sum == 0.0)
    assert(acc.avg == 0.0)

    acc.add(1.0)
    assert(acc.count == 2)
    assert(acc.sum == 1.0)
    assert(acc.avg == 0.5)

    // Also test add using non-specialized add function
    acc.add(java.lang.Double.valueOf(2.0))
    assert(acc.count == 3)
    assert(acc.sum == 3.0)
    assert(acc.avg == 1.0)

    // Test merging
    val acc2 = new DoubleAccumulator
    acc2.add(2.0)
    acc.merge(acc2)
    assert(acc.count == 4)
    assert(acc.sum == 5.0)
    assert(acc.avg == 1.25)
  }

  test("ListAccumulator") {
    val acc = new CollectionAccumulator[Double]
    assert(acc.value.isEmpty)
    assert(acc.isZero)

    acc.add(0.0)
    assert(acc.value.contains(0.0))
    assert(!acc.isZero)

    acc.add(java.lang.Double.valueOf(1.0))

    val acc2 = acc.copyAndReset()
    assert(acc2.value.isEmpty)
    assert(acc2.isZero)

    assert(acc.value.contains(1.0))
    assert(!acc.isZero)
    assert(acc.value.size() === 2)

    acc2.add(2.0)
    assert(acc2.value.contains(2.0))
    assert(!acc2.isZero)
    assert(acc2.value.size() === 1)

    // Test merging
    acc.merge(acc2)
    assert(acc.value.contains(2.0))
    assert(!acc.isZero)
    assert(acc.value.size() === 3)

    val acc3 = acc.copy()
    assert(acc3.value.contains(2.0))
    assert(!acc3.isZero)
    assert(acc3.value.size() === 3)

    acc3.reset()
    assert(acc3.isZero)
    assert(acc3.value.isEmpty)
  }

  test("SPARK-59845: a subclass is registered only once it is fully deserialized") {
    // Deserialization registers the accumulator with the `TaskContext`, which publishes it to other
    // threads -- in production the executor heartbeater, which calls `isZero` on every registered
    // accumulator. Registering before the subclass's fields have been read exposes state that is
    // still unset, so a subclass holding its state in a plain `val` would see a null there.
    // Probe from `registerAccumulator` to observe that instant without racing a real reader.
    val acc = new NaiveStateAccumulator
    acc.metadata = AccumulatorMetadata(AccumulatorContext.newId(), None, countFailedValues = false)
    AccumulatorContext.register(acc)

    var probed = false
    val taskContext = new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      taskAttemptId = 0L,
      attemptNumber = 0,
      numPartitions = 1,
      taskMemoryManager = null,
      localProperties = new Properties,
      metricsSystem = null,
      taskMetrics = TaskMetrics.empty,
      cpuAmount = BigDecimal(1)) {
      private[spark] override def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
        assert(a.isZero, "accumulator state should be readable as soon as it is registered")
        probed = true
      }
    }

    TaskContext.setTaskContext(taskContext)
    try {
      Utils.deserialize[NaiveStateAccumulator](Utils.serialize(acc))
    } finally {
      TaskContext.unset()
      AccumulatorContext.remove(acc.id)
    }
    assert(probed, "the accumulator was never registered, so nothing was verified")
  }
}

class MyData(val i: Int) extends Serializable

/**
 * Holds its state the way a subclass naturally would, in a `val` dereferenced by `isZero`, with no
 * null handling. Registration must therefore happen only after this field has been deserialized.
 */
private class NaiveStateAccumulator extends AccumulatorV2[Int, java.util.List[Int]] {
  private val state = new java.util.ArrayList[Int]()

  override def isZero: Boolean = state.isEmpty
  override def copyAndReset(): NaiveStateAccumulator = new NaiveStateAccumulator
  override def copy(): NaiveStateAccumulator = {
    val newAcc = new NaiveStateAccumulator
    newAcc.state.addAll(state)
    newAcc
  }
  override def reset(): Unit = state.clear()
  override def add(v: Int): Unit = state.add(v)
  override def merge(other: AccumulatorV2[Int, java.util.List[Int]]): Unit =
    state.addAll(other.value)
  override def value: java.util.List[Int] = state
}
