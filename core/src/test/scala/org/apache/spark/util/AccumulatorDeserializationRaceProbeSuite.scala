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
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark._
import org.apache.spark.internal.config.{EXECUTOR_HEARTBEAT_INTERVAL,
  EXECUTOR_HEARTBEAT_MAX_FAILURES}

/**
 * SPARK-59451 exposure probe. NOT FOR MERGE; it is removed again in a later commit of the same PR.
 *
 * Measures how often the executor heartbeater observes an accumulator whose subclass fields have
 * not been deserialized yet. In production that observation is a NullPointerException in `isZero`
 * that kills heartbeating; here the probe accumulator's `isZero` counts the observation instead of
 * throwing, so the run survives and the count comes out at the end.
 *
 * The heartbeat interval is set to 1ms to make the window easy to hit. With the default 10s
 * interval the same window is roughly 10,000 times harder to hit per task, which is consistent with
 * the handful of executor losses per run seen at a few million tasks in production.
 *
 * At the commit that adds this suite the count is non-zero (about 1.5-2% of tasks at a 1ms
 * interval in local[4]). At the following commit, which moves registration to after
 * deserialization, it is zero. Run it at either commit with:
 *
 *   build/sbt 'core/testOnly *AccumulatorDeserializationRaceProbeSuite'
 *
 * `-Dspark.test.raceProbe.tasks=N` overrides the number of tasks (default 100000).
 */
class AccumulatorDeserializationRaceProbeSuite extends SparkFunSuite with LocalSparkContext {

  test("SPARK-59451: count heartbeats that observe a half-deserialized accumulator") {
    val numTasks = sys.props.get("spark.test.raceProbe.tasks").map(_.toInt).getOrElse(100000)
    val tasksPerJob = 5000

    val conf = new SparkConf().setMaster("local[4]").setAppName("race probe")
    conf.set(EXECUTOR_HEARTBEAT_INTERVAL.key, "1ms")
    // The interval is also the heartbeat RPC timeout. Do not let a run of timeouts make the
    // local executor exit the JVM; only the observation count matters here.
    conf.set(EXECUTOR_HEARTBEAT_MAX_FAILURES.key, Int.MaxValue.toString)
    sc = new SparkContext(conf)

    RaceProbe.reset()
    val acc = new RaceProbeAccumulator
    sc.register(acc, "raceProbe")

    // Capture the accumulator in the closure so every task deserializes it, as a cached relation's
    // PartitionKeyedAccumulator is deserialized by every task downstream of the cache.
    val rdd = sc.parallelize(1 to tasksPerJob, tasksPerJob).map { i => acc.add(i); i }
    val start = System.nanoTime()
    (1 to (numTasks / tasksPerJob)).foreach(_ => rdd.count())
    val elapsedSec = (System.nanoTime() - start) / 1e9

    val deserializations = RaceProbe.deserializations.get()
    val hits = RaceProbe.hits.get()
    val report =
      f"SPARK-59451 race probe: $hits hits in $deserializations accumulator deserializations " +
        f"(${100.0 * hits / math.max(deserializations, 1)}%.3f%% of tasks, " +
        f"heartbeat interval 1ms, $elapsedSec%.1f s)"
    logWarning(report)
    // scalastyle:off println
    System.err.println(report)
    // scalastyle:on println

    // Only sanity-check that the probe was actually deserialized per task. The hit count is the
    // measurement, not an assertion: it is zero with the fix and non-zero without it.
    assert(deserializations >= numTasks)
  }
}

private[util] object RaceProbe {
  val deserializations = new AtomicLong()
  val hits = new AtomicLong()

  def reset(): Unit = {
    deserializations.set(0)
    hits.set(0)
  }
}

/**
 * Same shape as `PartitionKeyedAccumulator`: `isZero` reads a collection field declared in the
 * subclass. Instead of throwing when that field is still null, it counts the observation.
 */
class RaceProbeAccumulator extends AccumulatorV2[Int, java.util.Set[Int]] {
  private val values = ConcurrentHashMap.newKeySet[Int]()

  override def isZero: Boolean = {
    val v = values
    if (v == null) {
      // A heartbeat landed between the AccumulatorV2 slice and this slice being deserialized.
      RaceProbe.hits.incrementAndGet()
      true
    } else {
      v.isEmpty
    }
  }

  override def copy(): RaceProbeAccumulator = {
    val newAcc = new RaceProbeAccumulator
    newAcc.values.addAll(values)
    newAcc
  }

  override def reset(): Unit = values.clear()

  override def add(v: Int): Unit = values.add(v)

  override def merge(other: AccumulatorV2[Int, java.util.Set[Int]]): Unit = other match {
    case o: RaceProbeAccumulator => values.addAll(o.values)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: java.util.Set[Int] = values

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    // Count only task-side deserializations, which are the ones the heartbeater can observe. The
    // driver also deserializes each task's returned update, but without a TaskContext.
    if (TaskContext.get() != null) {
      RaceProbe.deserializations.incrementAndGet()
    }
  }
}
