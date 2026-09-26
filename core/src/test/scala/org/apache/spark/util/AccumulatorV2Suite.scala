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

import java.io.ObjectInputStream
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark._

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

  test("SPARK-59451: accumulator is registered with the task only once fully deserialized") {
    val acc = new HeartbeatProbeAccumulator
    acc.metadata = AccumulatorMetadata(AccumulatorContext.newId(), None, countFailedValues = false)
    AccumulatorContext.register(acc)
    val bytes = Utils.serialize(acc)

    // A task context makes the deserialized copy auto-register, as it does inside a task.
    TaskContext.setTaskContext(TaskContext.empty())
    try {
      // The probe's readObject throws if this object is already registered, see its doc.
      val deserialized = Utils.deserialize[HeartbeatProbeAccumulator](bytes)

      assert(!deserialized.isAtDriverSide)
      assert(TaskContext.get().taskMetrics().accumulators().contains(deserialized))
      assert(deserialized.isZero)
    } finally {
      TaskContext.unset()
      AccumulatorContext.clear()
    }
  }

}

class MyData(val i: Int) extends Serializable

/**
 * An accumulator whose state lives in a field of this class rather than of `AccumulatorV2`. Java
 * deserializes a class hierarchy from the top down, so there is a window in which the
 * `AccumulatorV2` slice of the object has been read but this slice has not and `values` is null.
 * This class's `readObject` runs in exactly that window and mimics the executor heartbeater:
 * it iterates the task's registered accumulators and calls `isZero` on each.
 */
class HeartbeatProbeAccumulator extends AccumulatorV2[Int, java.util.Set[Int]] {
  private val values = ConcurrentHashMap.newKeySet[Int]()

  override def isZero: Boolean = values.isEmpty

  override def copy(): HeartbeatProbeAccumulator = {
    val newAcc = new HeartbeatProbeAccumulator
    newAcc.values.addAll(values)
    newAcc
  }

  override def reset(): Unit = values.clear()

  override def add(v: Int): Unit = values.add(v)

  override def merge(other: AccumulatorV2[Int, java.util.Set[Int]]): Unit = other match {
    case o: HeartbeatProbeAccumulator => values.addAll(o.values)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: java.util.Set[Int] = values

  private def readObject(in: ObjectInputStream): Unit = {
    // Simulate a heartbeat arriving before this class's fields have been read.
    TaskContext.get().taskMetrics().accumulators().foreach(_.isZero)
    in.defaultReadObject()
  }
}
