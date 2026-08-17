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

import org.apache.spark.sql.functions
import org.apache.spark.sql.pipelines.autocdc.{Scd2BatchProcessor, ScdType}
import org.apache.spark.sql.pipelines.utils.ExecutionTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Differential test for cross-SCD current-state agreement: given the same randomly-generated
 * CDF, every live key's SCD Type 1 target row must equal (in user data columns) the current
 * open SCD Type 2 row for that key - this is by definitions of an SCD1 and SCD2 transformation.
 *
 * By asserting the final outcome of SCD1 equals the final live rows of SCD2, each implementation
 * is an effective verifier of the other, and catches regressions or behavior changes made to one
 * implementation but not the other.
 *
 * CDC metadata and SCD2 interval bounds are not compared.
 *
 * Each seed independently shuffles and micro-batches the same event set for SCD1 and SCD2, so
 * one SCD implementation keeps the other in check under out-of-order ingestion.
 *
 * Scale uses the shared defaults / system properties on [[AutoCdcRandomCdcTestMixin]]. Set a
 * deterministic base seed with `-Dspark.sql.test.autocdc.convergenceBaseSeed=<seed>`. The first
 * iteration uses that seed directly and any remaining iteration seeds are derived from it.
 */
class AutoCdcCrossScdConvergenceSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin
    with AutoCdcRandomCdcTestMixin {

  /**
   * Assert SCD1 live rows equal SCD2 current open rows (`__END_AT IS NULL`) on user data
   * columns only.
   */
  private def assertCrossScdAgreement(scd1Table: String, scd2Table: String): Unit = {
    val scd1Data = spark.table(s"$catalog.$namespace.$scd1Table").select(
      dataColumnNames.map(functions.col): _*
    )
    val scd2CurrentData = spark.table(s"$catalog.$namespace.$scd2Table")
      .where(functions.col(Scd2BatchProcessor.endAtColName).isNull)
      .select(dataColumnNames.map(functions.col): _*)
    checkAnswer(scd1Data, scd2CurrentData)
  }

  test("SCD1 current rows match SCD2 open rows across independently shuffled CDC streams") {
    val numDistinctKeys = resolveNumDistinctKeys()
    val maxUniqueEventsPerKey = resolveMaxUniqueEventsPerKey()
    val numOutOfOrderBatches = resolveNumOutOfOrderBatches()

    forEachConvergenceSeed { (seed, seedIndex) =>
      val rand = new Random(seed)
      val sortedEventStream = generateConfiguredCdcEventStream(rand)

      // Independent shuffle / batching RNGs so each SCD side exercises a different arrival order
      // while still being fully determined by `seed`.
      val scd1Rand = new Random(rand.nextLong())
      val scd2Rand = new Random(rand.nextLong())
      val scd1Shuffled = scd1Rand.shuffle(sortedEventStream)
      val scd2Shuffled = scd2Rand.shuffle(sortedEventStream)
      val scd1Batches = 1 + scd1Rand.nextInt(numOutOfOrderBatches)
      val scd2Batches = 1 + scd2Rand.nextInt(numOutOfOrderBatches)

      // Seed alone is enough to regenerate the stream; avoid dumping thousands of events into
      // every clue string (ScalaTest evaluates clues eagerly).
      withClue(
        s"\ncross-SCD convergence seedIndex=$seedIndex seed=$seed " +
        s"(rerun with -D$baseSeedSystemProperty=$seed " +
        s"-D$numSeedsSystemProperty=1 to reproduce)\n" +
        s"keys=$numDistinctKeys maxEventsPerKey=$maxUniqueEventsPerKey " +
        s"scd1Batches=$scd1Batches scd2Batches=$scd2Batches " +
        s"events=${sortedEventStream.size}\n"
      ) {
        val scd1Table = s"cross_scd1_$seedIndex"
        val scd2Table = s"cross_scd2_$seedIndex"
        runRandomCdcPipeline(scd1Table, ScdType.Type1, scd1Shuffled, scd1Batches)
        runRandomCdcPipeline(scd2Table, ScdType.Type2, scd2Shuffled, scd2Batches)
        assertCrossScdAgreement(scd1Table, scd2Table)
      }
    }
  }
}
