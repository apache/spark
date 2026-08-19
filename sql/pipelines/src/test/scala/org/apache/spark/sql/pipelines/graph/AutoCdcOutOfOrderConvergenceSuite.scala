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

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions
import org.apache.spark.sql.pipelines.autocdc.{ColumnSelection, ScdType, UnqualifiedColumnName}
import org.apache.spark.sql.pipelines.graph.AutoCdcOutOfOrderConvergenceSuite.SourceRow
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

object AutoCdcOutOfOrderConvergenceSuite {
  /**
   * A single CDC event in the source stream.
   *
   * @param key       Identity column (the AutoCDC `keys`).
   * @param name      Data column (nullable string).
   * @param amount    Data column (nullable int).
   * @param active    Data column (nullable boolean).
   * @param sequence  Sequencing value (the AutoCDC `sequencing` expression).
   * @param isDelete  Drives the AutoCDC `deleteCondition`; `true` marks the event as a delete,
   *                  `false` as an upsert. Excluded from the target via `columnSelection`.
   */
  case class SourceRow(
      key: Int,
      name: Option[String],
      amount: Option[Int],
      active: Option[Boolean],
      sequence: Long,
      isDelete: Boolean)
}

/**
 * Differential test for the AutoCDC merge's order-invariance property, for both SCD Type 1 and
 * SCD Type 2: feeding the same randomly-generated CDC event stream as a single sorted micro-batch
 * and as several shuffled micro-batches must converge to the same target table contents.
 */
class AutoCdcOutOfOrderConvergenceSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  import testImplicits._

  // Distinct keys in the generated event stream.
  private val numDistinctKeys: Int = 5
  // Upper bound on unique events (one per sequence) generated per key, before intentionally
  // duplicating some events.
  private val maxUniqueEventsPerKey: Int = 80
  // Probability an event is a delete; (1 - this) is the upsert probability.
  private val deleteEventProbability: Double = 0.20
  // Probability an event is immediately re-emitted with the same sequence and payload.
  private val duplicateEventProbability: Double = 0.15
  // Probability an optional payload column is non-null; (1 - this) is the null probability.
  private val nonNullProbability: Double = 0.75
  // Number of microbatches the out-of-order pipeline splits the shuffled events across.
  private val numOutOfOrderBatches: Int = 8

  // System property used to pin the test seed for reproduction. If unset, the suite generates a
  // fresh seed on each run and reports it in the failure message so a failing seed can be replayed
  // by setting this property. Mirrors the convention used by `RandomDataGenerator` and other Spark
  // suites that expose tunables via `spark.sql.test.<feature>` system properties.
  private val seedSystemProperty: String =
    "spark.sql.test.autocdc.outOfOrderConvergenceSeed"

  private def resolveTestSeed(): Long = {
    Option(System.getProperty(seedSystemProperty)).map(_.toLong).getOrElse(Random.nextLong())
  }

  private val keyColumn: String = "key"
  private val nameColumn: String = "name"
  private val amountColumn: String = "amount"
  private val activeColumn: String = "active"
  private val sequenceColumn: String = "sequence"
  private val isDeleteColumn: String = "is_delete"

  private val sourceColumnNames: Seq[String] =
    Seq(keyColumn, nameColumn, amountColumn, activeColumn, sequenceColumn, isDeleteColumn)

  private def randomUpsertOrDelete(
      rand: Random, key: Int, sequence: Long, isDelete: Boolean): SourceRow = {
    val colorPalette = Seq("red", "blue", "green", "yellow")
    SourceRow(
      key = key,
      name = Option.when(rand.nextDouble() < nonNullProbability)(
        colorPalette(rand.nextInt(colorPalette.length))),
      amount = Option.when(rand.nextDouble() < nonNullProbability)(rand.nextInt(100)),
      active = Option.when(rand.nextDouble() < nonNullProbability)(rand.nextBoolean()),
      sequence = sequence,
      isDelete = isDelete
    )
  }

  private def generateRandomCdcEventStream(rand: Random): Seq[SourceRow] = {
    var nextSequence: Long = 0L
    val events = ArrayBuffer.empty[SourceRow]
    (0 until numDistinctKeys).foreach { key =>
      val numUniqueEventsForKey = rand.between(1, maxUniqueEventsPerKey + 1)
      (0 until numUniqueEventsForKey).foreach { _ =>
        val isDelete = rand.nextDouble() < deleteEventProbability
        val event = randomUpsertOrDelete(rand, key, nextSequence, isDelete)
        nextSequence += 1
        events += event
        if (rand.nextDouble() < duplicateEventProbability) {
          events += event
        }
      }
    }
    events.sortBy(_.sequence).toSeq
  }

  /** Build a pipeline context with a single AutoCDC flow of `scdType` reading from `stream`. */
  private def buildPipelineContext(
      targetTable: String,
      stream: MemoryStream[SourceRow],
      scdType: ScdType): TestGraphRegistrationContext = {
    new TestGraphRegistrationContext(spark) {
      registerTable(targetTable, catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = s"${targetTable}_flow",
        target = targetTable,
        query = dfFlowFunc(stream.toDF().toDF(sourceColumnNames: _*)),
        keys = Seq(keyColumn),
        sequencing = functions.col(sequenceColumn),
        deleteCondition = Some(functions.col(isDeleteColumn) === true),
        columnSelection = Some(ColumnSelection.ExcludeColumns(
          Seq(UnqualifiedColumnName(isDeleteColumn))
        )),
        scdType = scdType
      ))
    }
  }

  /**
   * DDL fragment for the SCD-type-specific reserved columns a target table carries after the
   * user-selected data columns: the CDC metadata column for SCD1, and the interval bounds plus
   * metadata column for SCD2. The sequencing type is BIGINT here.
   */
  private def reservedColumnsDdl(scdType: ScdType): String = scdType match {
    case ScdType.Type1 => scd1MetadataDdl
    case ScdType.Type2 => scd2MetadataDdl
  }

  private def createTargetTable(targetTable: String, scdType: ScdType): Unit = {
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.$targetTable (" +
      s"`$keyColumn` INT NOT NULL, " +
      s"`$nameColumn` STRING, " +
      s"`$amountColumn` INT, " +
      s"`$activeColumn` BOOLEAN, " +
      s"`$sequenceColumn` BIGINT NOT NULL, " +
      s"${reservedColumnsDdl(scdType)})"
    )
  }

  private def assertTargetsConverge(inOrderTable: String, outOfOrderTable: String): Unit = {
    checkAnswer(
      spark.table(s"$catalog.$namespace.$outOfOrderTable"),
      spark.table(s"$catalog.$namespace.$inOrderTable")
    )
  }

  private def runConvergenceTest(seed: Long, scdType: ScdType): Unit = {
    val rand = new Random(seed)
    val sortedEventStream = generateRandomCdcEventStream(rand)
    val shuffledEventStream = rand.shuffle(sortedEventStream)

    withClue(
      s"\nscdType=${scdType.label} seed=$seed " +
      s"(rerun with -D$seedSystemProperty=$seed to reproduce)\n" +
      s"events (${sortedEventStream.size} total, sorted by sequence):\n" +
      sortedEventStream.map(r => s"  $r").mkString("\n") + "\n"
    ) {
      // Table names are scd-type-suffixed purely for readability: the SCD1 and SCD2 tests run as
      // separate test cases and the mixin's afterEach resets the catalog between them, so they
      // could not collide even with identical names; the suffix just makes a failing run's tables
      // self-identifying.
      val suffix = scdType.label.toLowerCase(java.util.Locale.ROOT)
      val inOrderTable = s"inorder_target_$suffix"
      val outOfOrderTable = s"outoforder_target_$suffix"
      createTargetTable(inOrderTable, scdType)
      createTargetTable(outOfOrderTable, scdType)

      val inOrderStream = MemoryStream[SourceRow]
      val inOrderCtx = buildPipelineContext(inOrderTable, inOrderStream, scdType)
      inOrderStream.addData(sortedEventStream: _*)
      runPipeline(inOrderCtx)

      val outOfOrderStream = MemoryStream[SourceRow]
      val outOfOrderCtx = buildPipelineContext(outOfOrderTable, outOfOrderStream, scdType)
      val totalEvents = shuffledEventStream.size
      (0 until numOutOfOrderBatches).foreach { batchIndex =>
        val batchStart = batchIndex * totalEvents / numOutOfOrderBatches
        val batchEnd = (batchIndex + 1) * totalEvents / numOutOfOrderBatches
        outOfOrderStream.addData(shuffledEventStream.slice(batchStart, batchEnd): _*)
        runPipeline(outOfOrderCtx)
      }

      // Only the user-visible target must converge. The auxiliary tables legitimately differ by
      // arrival order (e.g. deletedByBatchId stamps and cross-batch GC depend on how events are
      // batched), so they are not compared.
      assertTargetsConverge(inOrderTable, outOfOrderTable)
    }
  }

  test("SCD1 merge converges across micro-batch shuffling for randomly generated CDC events") {
    runConvergenceTest(resolveTestSeed(), ScdType.Type1)
  }

  test("SCD2 merge converges across micro-batch shuffling for randomly generated CDC events") {
    runConvergenceTest(resolveTestSeed(), ScdType.Type2)
  }
}
