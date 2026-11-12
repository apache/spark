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
  private def assertCrossScdAgreement(
      scd1Table: String,
      scd2Table: String,
      expectedLiveKeyCount: Int): Unit = {
    val scd1Data = spark.table(s"$catalog.$namespace.$scd1Table").select(
      dataColumnNames.map(functions.col): _*
    )
    val scd2CurrentData = spark.table(s"$catalog.$namespace.$scd2Table")
      .where(functions.col(Scd2BatchProcessor.endAtColName).isNull)
      .select(dataColumnNames.map(functions.col): _*)

    // Verify the number of live keys (i.e rows that haven't been fully deleted) are the same in
    // both SCD1 and SCD2, after all events are applied.
    val scd1LiveKeyCount = scd1Data.count()
    val scd2LiveKeyCount = scd2CurrentData.count()
    assert(
      scd1LiveKeyCount == expectedLiveKeyCount,
      s"Expected $expectedLiveKeyCount live SCD1 keys, found $scd1LiveKeyCount")
    assert(
      scd2LiveKeyCount == expectedLiveKeyCount,
      s"Expected $expectedLiveKeyCount live SCD2 keys, found $scd2LiveKeyCount")

    checkAnswer(scd1Data, scd2CurrentData)
  }

  private val crossScdConvergenceTestName =
    "SCD1 current rows match SCD2 open rows for the same shuffled CDC stream"

  test(crossScdConvergenceTestName) {
    val numDistinctKeys = resolveNumDistinctKeys()
    val maxUniqueEventsPerKey = resolveMaxUniqueEventsPerKey()
    val numBatches = resolveNumBatches()

    forEachConvergenceSeed(crossScdConvergenceTestName) { (seed, seedIndex) =>
      val rand = new Random(seed)
      val sortedEventStream = generateRandomCdcEventStream(rand)
      val shuffledEventStream = rand.shuffle(sortedEventStream)
      val expectedLiveKeyCount = sortedEventStream
        .groupBy(_.key)
        .values
        .count(events => !events.maxBy(_.sequence).isDelete)

      // Avoid dumping thousands of events into every clue string (ScalaTest evaluates clues
      // eagerly).
      withClue(
        s"\ncross-SCD convergence testName=$crossScdConvergenceTestName " +
        s"seedIndex=$seedIndex seed=$seed " +
        s"(rerun this test with -D$convergenceReproSeedSystemProperty=$seed to reproduce)\n" +
        s"keys=$numDistinctKeys maxEventsPerKey=$maxUniqueEventsPerKey " +
        s"numBatches=$numBatches expectedLiveKeys=$expectedLiveKeyCount " +
        s"events=${sortedEventStream.size}\n"
      ) {
        val scd1Table = s"cross_scd1_$seedIndex"
        val scd2Table = s"cross_scd2_$seedIndex"
        runRandomCdcPipeline(scd1Table, ScdType.Type1, shuffledEventStream, numBatches)
        runRandomCdcPipeline(scd2Table, ScdType.Type2, shuffledEventStream, numBatches)
        assertCrossScdAgreement(scd1Table, scd2Table, expectedLiveKeyCount)
      }
    }
  }
}
