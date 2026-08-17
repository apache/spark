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
 *
 * Scale uses the shared defaults / system properties on [[AutoCdcRandomCdcTestMixin]]. Set a
 * deterministic base seed with `-Dspark.sql.test.autocdc.convergenceBaseSeed=<seed>`. The first
 * iteration uses that seed directly and any remaining iteration seeds are derived from it.
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

  private def runConvergenceTest(scdType: ScdType): Unit = {
    val numDistinctKeys = resolveNumDistinctKeys()
    val maxUniqueEventsPerKey = resolveMaxUniqueEventsPerKey()
    val numOutOfOrderBatches = resolveNumOutOfOrderBatches()

    forEachConvergenceSeed { (seed, seedIndex) =>
      val rand = new Random(seed)
      val sortedEventStream = generateConfiguredCdcEventStream(rand)
      val shuffledEventStream = rand.shuffle(sortedEventStream)

      // Seed alone regenerates the stream; avoid dumping every event into an eagerly-built clue.
      withClue(
        s"\nout-of-order convergence scdType=${scdType.label} " +
        s"seedIndex=$seedIndex seed=$seed " +
        s"(rerun with -D$baseSeedSystemProperty=$seed " +
        s"-D$numSeedsSystemProperty=1 to reproduce)\n" +
        s"keys=$numDistinctKeys maxEventsPerKey=$maxUniqueEventsPerKey " +
        s"outOfOrderBatches=$numOutOfOrderBatches events=${sortedEventStream.size}\n"
      ) {
        // Table names are scd-type- and seed-suffixed so multi-seed runs within one test case do
        // not collide before afterEach resets the catalog.
        val suffix = scdType.label.toLowerCase(java.util.Locale.ROOT)
        val inOrderTable = s"inorder_target_${suffix}_$seedIndex"
        val outOfOrderTable = s"outoforder_target_${suffix}_$seedIndex"

        // In-order baseline: one microbatch with the sequence-sorted stream.
        runRandomCdcPipeline(inOrderTable, scdType, sortedEventStream, numBatches = 1)
        // Out-of-order: same events shuffled across the configured number of microbatches.
        runRandomCdcPipeline(
          outOfOrderTable, scdType, shuffledEventStream, numOutOfOrderBatches)

        // Only the user-visible target must converge. The auxiliary tables legitimately differ by
        // arrival order (e.g. deletedByBatchId stamps and cross-batch GC depend on how events are
        // batched), so they are not compared.
        assertTargetsConverge(inOrderTable, outOfOrderTable)
      }
    }
  }

  test("SCD1 merge converges across micro-batch shuffling for randomly generated CDC events") {
    runConvergenceTest(ScdType.Type1)
  }

  test("SCD2 merge converges across micro-batch shuffling for randomly generated CDC events") {
    runConvergenceTest(ScdType.Type2)
  }
}
