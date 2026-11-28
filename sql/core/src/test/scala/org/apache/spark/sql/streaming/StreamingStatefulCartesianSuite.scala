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

package org.apache.spark.sql.streaming

import scala.collection.mutable

import org.apache.spark.{SparkException, SparkFunSuite, SparkThrowable}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RoundRobinPartitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.CartesianProductExec
import org.apache.spark.sql.execution.streaming.operators.stateful.StreamingGlobalLimitExec
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions.{array, explode, lit, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/** Checks that a query failed because sibling tasks waited on the same RocksDB instance. */
trait StatefulCartesianErrorChecks { self: SparkFunSuite =>
  private val unreleasedThreadCondition = "CANNOT_LOAD_STATE_STORE.UNRELEASED_THREAD_ERROR"

  protected def checkUnreleasedThreadError(e: Throwable): Unit = {
    // The deepest match is the original error; a wrapping StreamingQueryException can carry
    // the same condition with its own parameters.
    val error = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).collect {
      case t: SparkThrowable if t.getCondition == unreleasedThreadCondition => t
    }.toSeq.lastOption
      .getOrElse(fail(s"expected $unreleasedThreadCondition, got ${Utils.exceptionString(e)}"))
    checkError(
      exception = error,
      condition = unreleasedThreadCondition,
      sqlState = Some("58030"),
      parameters = Map(
        // Every query here has one deduplicate, so only the partition that waited varies.
        "loggingId" -> "StateStoreId\\(opId=0,partId=\\d+,name=default\\)",
        "operationType" -> "load",
        "newAcquiredThreadInfo" -> "(?s).*",
        "acquiredThreadInfo" -> "(?s).*",
        "timeWaitedMs" -> "\\d+",
        "stackTraceOutput" -> "(?s).*"),
      matchPVals = true)
  }
}

/**
 * A stateful operator under a `CartesianProductExec` in the same stage.
 * With two state partitions and a two-partition static side the Cartesian stage runs four tasks,
 * and the two tasks of a pair load the same state partition at once. A task holds its RocksDB
 * instance until it finishes (the slow filter keeps it busy for at least a second), so the sibling
 * times out with CANNOT_LOAD_STATE_STORE.UNRELEASED_THREAD_ERROR. The fix must instead run
 * exactly one task per state partition in the stage that loads state.
 */
class StreamingStatefulCartesianSuite extends StreamTest with StatefulCartesianErrorChecks {
  import testImplicits._

  private val shuffleEnabledKey =
    SQLConf.STATEFUL_OPERATOR_SHUFFLE_BEFORE_CARTESIAN_PRODUCT_ENABLED.key

  // Every test adds these values to the stream and cross joins them with these static ids.
  private val inputValues = Seq(1, 2, 3, 4)
  private val staticIds = 0L until 4L
  private val expectedRows = for (v <- inputValues; id <- staticIds) yield (v, id)

  /** Runs the repro and returns numTasks of every completed stage that loaded a state store. */
  private def runStatefulCartesian(
      enabled: Boolean,
      statefulOnLeft: Boolean,
      keepDedupKey: Boolean = true)(actions: StreamAction*): Seq[Int] = {
    val statefulStageTasks = mutable.ArrayBuffer.empty[Int]
    val listener = new SparkListener {
      override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
        if (event.stageInfo.rddInfos.exists(_.name == "StateStoreRDD")) {
          statefulStageTasks.synchronized { statefulStageTasks += event.stageInfo.numTasks }
        }
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      withSQLConf(
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.SHUFFLE_PARTITIONS.key -> "2",
          // Force CartesianProductExec rather than a broadcast join of the static side.
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          // A sibling waiting on an already-loaded instance fails fast instead of after 120s.
          "spark.sql.streaming.stateStore.rocksdb.lockAcquireTimeoutMs" -> "300",
          shuffleEnabledKey -> enabled.toString) {
        val input = MemoryStream[Int]
        val deduped = input.toDS().dropDuplicates()
        // Replacing the dedup key drops the dedup's hash partitioning from the stateful output.
        val stateful = if (keepDedupKey) {
          deduped.withColumn("k", lit(1))
        } else {
          deduped.select(($"value" * 10).as("value"), lit(1).as("k"))
        }
        val static = spark.range(staticIds.head, staticIds.last + 1, 1, numPartitions = 2)
          .withColumn("k", lit(1))
        // Every joined row sleeps, so a task holds its state store for at least one second. The
        // udf is non-deterministic so the optimizer keeps the filter above the join instead of
        // pushing it onto the static side, where it would run before the state store is opened.
        val slow = udf((_: Long) => { Thread.sleep(1000); true }).asNondeterministic()
        val joined = if (statefulOnLeft) stateful.join(static, "k") else static.join(stateful, "k")
        testStream(joined.filter(slow($"id")).select($"value", $"id"))(
          AddData(input, inputValues: _*) +: actions: _*)
      }
      spark.sparkContext.listenerBus.waitUntilEmpty()
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
    statefulStageTasks.synchronized(statefulStageTasks.toSeq)
  }

  test("baseline: sibling Cartesian tasks load one state partition at once and time out") {
    val tasks = runStatefulCartesian(enabled = false, statefulOnLeft = true)(
      ExpectFailure[SparkException](checkUnreleasedThreadError, typeIsSuperClass = true))
    // 2 state partitions x 2 static partitions = 4 tasks in the stage that loads state.
    assert(tasks == Seq(4), s"stateful stage task counts: $tasks")
  }

  /** The shuffle that a Cartesian product in the plan reads one of its sides through, if any. */
  private def shuffleUnderCartesian(p: SparkPlan): Option[ShuffleExchangeExec] =
    p.collectFirst {
      case c: CartesianProductExec => c.children.collectFirst { case s: ShuffleExchangeExec => s }
    }.flatten

  /** Asserts the inserted shuffle keeps the stateful side's 2 partitions. */
  private def assertInsertedShuffle(keepsHashPartitioning: Boolean): AssertOnQuery =
    AssertOnQuery { q =>
      val plan = q.lastExecution.executedPlan
      val partitioning = shuffleUnderCartesian(plan)
        .getOrElse(fail(s"no shuffle under the Cartesian product:\n$plan")).outputPartitioning
      assert(partitioning.numPartitions == 2, s"inserted shuffle partitioning: $partitioning")
      if (keepsHashPartitioning) {
        assert(partitioning.isInstanceOf[HashPartitioning], s"expected hash: $partitioning")
      } else {
        assert(partitioning.isInstanceOf[RoundRobinPartitioning],
          s"expected round-robin: $partitioning")
      }
      true
    }

  for (statefulOnLeft <- Seq(true, false)) {
    val side = if (statefulOnLeft) "left" else "right"
    test(s"shuffle: stateful $side side is computed by one task per state partition") {
      val tasks = runStatefulCartesian(enabled = true, statefulOnLeft)(
        CheckAnswer(expectedRows: _*),
        assertInsertedShuffle(keepsHashPartitioning = true))
      assert(tasks.nonEmpty && tasks.forall(_ == 2), s"stateful stage task counts: $tasks")
    }
  }

  test("shuffle: round-robin when the stateful output lost its hash partitioning") {
    val tasks = runStatefulCartesian(enabled = true, statefulOnLeft = true, keepDedupKey = false)(
      CheckAnswer((for (v <- inputValues; id <- staticIds) yield (v * 10, id)): _*),
      assertInsertedShuffle(keepsHashPartitioning = false))
    assert(tasks.nonEmpty && tasks.forall(_ == 2), s"stateful stage task counts: $tasks")
  }

  test("shuffle: no extra shuffle when one already separates the stateful side") {
    val statefulStageTasks = mutable.ArrayBuffer.empty[Int]
    val listener = new SparkListener {
      override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
        if (event.stageInfo.rddInfos.exists(_.name == "StateStoreRDD")) {
          statefulStageTasks.synchronized { statefulStageTasks += event.stageInfo.numTasks }
        }
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      withSQLConf(
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.SHUFFLE_PARTITIONS.key -> "2",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          shuffleEnabledKey -> "true") {
        val input = MemoryStream[(Int, Int)]
        // Deduplicating on `a` and then joining on `b` makes EnsureRequirements shuffle the
        // deduplicated rows by `b`, which already ends the stateful stage below the Cartesian.
        val deduped = input.toDF().toDF("a", "b").dropDuplicates("a")
        val byB = spark.range(0, 4).select($"id".cast("int").as("b"))
        val static = spark.range(0, 2, 1, numPartitions = 2).select($"id".as("c"), lit(1).as("k"))
        val joined = deduped.join(byB, "b").withColumn("k", lit(1)).join(static, "k")
        testStream(joined.select($"a", $"b", $"c"))(
          AddData(input, (1, 0), (2, 1), (3, 2)),
          CheckAnswer((1, 0, 0L), (1, 0, 1L), (2, 1, 0L), (2, 1, 1L), (3, 2, 0L), (3, 2, 1L)),
          AssertOnQuery(
            _.lastExecution.executedPlan.exists(_.isInstanceOf[CartesianProductExec]),
            "expected a Cartesian product"),
          AssertOnQuery(q => shuffleUnderCartesian(q.lastExecution.executedPlan).isEmpty,
            "added a shuffle under the Cartesian product although one already ends the stage"))
      }
      spark.sparkContext.listenerBus.waitUntilEmpty()
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
    val tasks = statefulStageTasks.synchronized(statefulStageTasks.toSeq)
    assert(tasks.nonEmpty && tasks.forall(_ == 2), s"stateful stage task counts: $tasks")
  }

  test("shuffle: no hash reuse when the stateful side no longer outputs the hash key") {
    withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        shuffleEnabledKey -> "true") {
      val input = MemoryStream[Int]
      // GenerateExec forwards the dedup's HashPartitioning(value) although it drops `value`.
      val exploded =
        input.toDS().dropDuplicates().select(explode(array($"value", $"value")).as("e"))
      val static = spark.range(staticIds.head, staticIds.last + 1, 1, numPartitions = 2)
      testStream(exploded.crossJoin(static))(
        AddData(input, inputValues: _*),
        CheckAnswer((for (v <- inputValues; _ <- 1 to 2; id <- staticIds) yield (v, id)): _*),
        assertInsertedShuffle(keepsHashPartitioning = false))
    }
  }

  test("shuffle: keeps a single-partition stateful side single-partitioned") {
    withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        shuffleEnabledKey -> "true") {
      val input = MemoryStream[Int]
      // A streaming global limit has one partition although its state info has two.
      val limited = input.toDS().limit(inputValues.size)
      val static = spark.range(staticIds.head, staticIds.last + 1, 1, numPartitions = 2)
      testStream(limited.crossJoin(static))(
        AddData(input, inputValues: _*),
        CheckAnswer(expectedRows: _*),
        AssertOnQuery { q =>
          val plan = q.lastExecution.executedPlan
          val shuffle = shuffleUnderCartesian(plan)
            .getOrElse(fail(s"no shuffle under the Cartesian product:\n$plan"))
          assert(shuffle.child.exists(_.isInstanceOf[StreamingGlobalLimitExec]), s"plan:\n$plan")
          assert(shuffle.outputPartitioning.numPartitions == 1, s"plan:\n$plan")
          true
        })
    }
  }
}
