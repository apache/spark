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

package org.apache.spark.sql.pipelines.graph

import scala.util.Random

import org.apache.spark.sql.pipelines.autocdc.ScdType
import org.apache.spark.sql.pipelines.utils.ExecutionTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Differential test for the AutoCDC merge's order-invariance property, for both SCD Type 1 and
 * SCD Type 2: feeding the same randomly-generated CDC event stream as a single sorted micro-batch
 * and as several shuffled micro-batches must converge to the same target table contents.
 */
class AutoCdcOutOfOrderConvergenceSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin
    with AutoCdcRandomCdcTestMixin {

  private def assertTargetsConverge(inOrderTable: String, outOfOrderTable: String): Unit = {
    checkAnswer(
      spark.table(s"$catalog.$namespace.$outOfOrderTable"),
      spark.table(s"$catalog.$namespace.$inOrderTable")
    )
  }

  private def runConvergenceTest(scdType: ScdType, testName: String): Unit = {
    val numDistinctKeys = resolveNumDistinctKeys()
    val maxUniqueEventsPerKey = resolveMaxUniqueEventsPerKey()
    val numBatches = resolveNumBatches()

    forEachConvergenceSeed(testName) { (seed, seedIndex) =>
      val rand = new Random(seed)
      val sortedEventStream = generateRandomCdcEventStream(rand)
      val shuffledEventStream = rand.shuffle(sortedEventStream)

      withClue(
        s"\nout-of-order convergence scdType=${scdType.label} testName=$testName " +
        s"seedIndex=$seedIndex seed=$seed " +
        s"(rerun this test with -D$convergenceReproSeedSystemProperty=$seed to reproduce)\n" +
        s"keys=$numDistinctKeys maxEventsPerKey=$maxUniqueEventsPerKey " +
        s"numBatches=$numBatches events=${sortedEventStream.size}\n"
      ) {
        val inOrderTable = s"inorder_target_$seedIndex"
        val outOfOrderTable = s"outoforder_target_$seedIndex"

        // In-order baseline: one microbatch with the sequence-sorted stream.
        runRandomCdcPipeline(inOrderTable, scdType, sortedEventStream, numBatches = 1)
        // Out-of-order: same events shuffled across the configured number of microbatches.
        runRandomCdcPipeline(outOfOrderTable, scdType, shuffledEventStream, numBatches)

        // Only the user-visible target must converge. The auxiliary tables legitimately differ by
        // arrival order (e.g. deletedByBatchId stamps and cross-batch GC depend on how events are
        // batched), so they are not compared.
        assertTargetsConverge(inOrderTable, outOfOrderTable)
      }
    }
  }

  private val scd1OutOfOrderTestName =
    "SCD1 merge converges across micro-batch shuffling for randomly generated CDC events"
  private val scd2OutOfOrderTestName =
    "SCD2 merge converges across micro-batch shuffling for randomly generated CDC events"

  test(scd1OutOfOrderTestName) {
    runConvergenceTest(ScdType.Type1, scd1OutOfOrderTestName)
  }

  test(scd2OutOfOrderTestName) {
    runConvergenceTest(ScdType.Type2, scd2OutOfOrderTestName)
  }
}
