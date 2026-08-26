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
import org.apache.spark.sql.pipelines.graph.AutoCdcRandomCdcTestMixin.SourceRow
import org.apache.spark.sql.pipelines.utils.ExecutionTest
import org.apache.spark.sql.test.SharedSparkSession

object AutoCdcRandomCdcTestMixin {
  /**
   * A single CDC event in a randomly-generated AutoCDC source stream.
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
 * Shared random-CDC fixture helpers for AutoCDC differential convergence suites
 * ([[AutoCdcOutOfOrderConvergenceSuite]], [[AutoCdcCrossScdConvergenceSuite]]).
 *
 * Owns the common event schema, stream generator, and microbatch feeding for random-data AutoCDC
 * suites.
 *
 * Exposed random data generation knobs (optional; defaults are CI-sized):
 *   - `spark.sql.test.autocdc.convergenceBaseSeed`
 *   - `spark.sql.test.autocdc.convergenceNumSeeds`
 *   - `spark.sql.test.autocdc.convergenceNumKeys`
 *   - `spark.sql.test.autocdc.convergenceMaxEventsPerKey`
 *   - `spark.sql.test.autocdc.convergenceNumBatches`
 *
 * Suites may override these defaults when they genuinely need a different baseline. For local
 * stress testing, for example:
 * {{{
 * build/sbt \
 *   -Dspark.sql.test.autocdc.convergenceNumSeeds=10 \
 *   -Dspark.sql.test.autocdc.convergenceNumKeys=100 \
 *   'pipelines/testOnly *AutoCdcCrossScdConvergenceSuite'
 * }}}
 *
 * The shared base seed property supplies the first iteration's seed and deterministically derives
 * any remaining per-iteration seeds from it.
 */
trait AutoCdcRandomCdcTestMixin {
  self: ExecutionTest with SharedSparkSession with AutoCdcGraphExecutionTestMixin =>

  // Probability an event is a delete; (1 - this) is the upsert probability.
  protected val deleteEventProbability: Double = 0.20
  // Probability an event is immediately re-emitted with the same sequence and payload.
  protected val duplicateEventProbability: Double = 0.15
  // Probability an upsert repeats the previous upsert's payload at a new sequence. In SCD2 this
  // will produce a no-op upsert row, provided that sequence is excluded from track-history column
  // selection.
  protected val noOpContinuationProbability: Double = 0.15
  // Probability an optional payload column is non-null; (1 - this) is the null probability.
  protected val nonNullProbability: Double = 0.75

  // CI-sized defaults shared by every convergence suite. Override in a suite only when that
  // suite genuinely needs a different baseline; prefer the shared system properties for
  // local stress scaling so both suites stay aligned under normal CI.
  protected val defaultBaseSeed: Long = 0x5EEDL
  protected val defaultNumDistinctKeys: Int = 5
  protected val defaultMaxUniqueEventsPerKey: Int = 80
  protected val defaultNumBatches: Int = 8
  protected val defaultNumSeedsPerRun: Int = 1

  // Exposed so suite failure clues can tell callers how to force a deterministic replay.
  protected val baseSeedSystemProperty: String =
    "spark.sql.test.autocdc.convergenceBaseSeed"
  protected val numSeedsSystemProperty: String =
    "spark.sql.test.autocdc.convergenceNumSeeds"
  private val numKeysSystemProperty: String =
    "spark.sql.test.autocdc.convergenceNumKeys"
  private val maxEventsPerKeySystemProperty: String =
    "spark.sql.test.autocdc.convergenceMaxEventsPerKey"
  private val numBatchesSystemProperty: String =
    "spark.sql.test.autocdc.convergenceNumBatches"

  private def positiveIntProp(name: String, default: Int): Int = {
    val value = Option(System.getProperty(name)).map(_.toInt).getOrElse(default)
    require(value > 0, s"$name must be positive, but got $value")
    value
  }

  protected def configuredBaseSeed: Long =
    Option(System.getProperty(baseSeedSystemProperty))
      .map(_.toLong)
      .getOrElse(defaultBaseSeed)

  private def resolveNumSeeds(): Int =
    positiveIntProp(numSeedsSystemProperty, defaultNumSeedsPerRun)

  protected def resolveNumDistinctKeys(): Int =
    positiveIntProp(numKeysSystemProperty, defaultNumDistinctKeys)

  protected def resolveMaxUniqueEventsPerKey(): Int =
    positiveIntProp(maxEventsPerKeySystemProperty, defaultMaxUniqueEventsPerKey)

  protected def resolveNumBatches(): Int =
    positiveIntProp(numBatchesSystemProperty, defaultNumBatches)

  /**
   * Invoke `callback(seed, seedIndex)` once per configured seed. The first iteration uses
   * [[configuredBaseSeed]] mixed with [[testName]]; any remaining iteration seeds are
   * deterministically derived from that per-test base.
   */
  protected def forEachConvergenceSeed(testName: String)(callback: (Long, Int) => Unit): Unit = {
    val effectiveBaseSeed = configuredBaseSeed ^ testName.hashCode.toLong
    val numSeeds = resolveNumSeeds()
    val masterRand = new Random(effectiveBaseSeed)
    val seeds = effectiveBaseSeed +: Seq.fill(numSeeds - 1)(masterRand.nextLong())
    seeds.zipWithIndex.foreach { case (seed, seedIndex) =>
      callback(seed, seedIndex)
    }
  }

  // Forward declare key, sequence, and data columns, so that inheriting suites can reference them.
  protected val keyColumn: String = "key"
  protected val nameColumn: String = "name"
  protected val amountColumn: String = "amount"
  protected val activeColumn: String = "active"
  protected val sequenceColumn: String = "sequence"
  protected val isDeleteColumn: String = "is_delete"

  protected val sourceColumnNames: Seq[String] =
    Seq(keyColumn, nameColumn, amountColumn, activeColumn, sequenceColumn, isDeleteColumn)

  /** User data columns on the target; excludes CDC metadata and SCD2 interval bounds. */
  protected val dataColumnNames: Seq[String] =
    Seq(keyColumn, nameColumn, amountColumn, activeColumn, sequenceColumn)

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

  /**
   * Generate a sequence-sorted CDC event stream.
   */
  protected def generateRandomCdcEventStream(rand: Random): Seq[SourceRow] = {
    val numDistinctKeys = resolveNumDistinctKeys()
    val maxUniqueEventsPerKey = resolveMaxUniqueEventsPerKey()

    var nextSequence: Long = 0L
    val allEvents = ArrayBuffer.empty[SourceRow]
    (0 until numDistinctKeys).foreach { key =>
      val numUniqueEventsForKey = rand.between(1, maxUniqueEventsPerKey + 1)
      val eventsForKey = ArrayBuffer.empty[SourceRow]

      (0 until numUniqueEventsForKey).foreach { _ =>
        val isDelete = rand.nextDouble() < deleteEventProbability
        val event = if (isDelete) {
          randomUpsertOrDelete(rand, key, nextSequence, isDelete = true)
        } else {
          val previousEventIfUpsertOpt = eventsForKey.lastOption.filterNot(_.isDelete)
          val upsertToNoOpContinueOpt = previousEventIfUpsertOpt.filter(
            _ => rand.nextDouble() < noOpContinuationProbability)

          upsertToNoOpContinueOpt match {
            case Some(upsertToNoOpContinue) =>
              // If we're no-op continuing a previous upsert, reuse the same [tracked history]
              // columns, incrementing only the sequence. This relies on sequence being the single
              // non-track-history column in the AutoCDC configuration.
              upsertToNoOpContinue.copy(sequence = nextSequence)
            case _ =>
              // If we're not no-op continuing a previous upsert, create a new upsert event.
              randomUpsertOrDelete(rand, key, nextSequence, isDelete = false)
          }
        }

        // By AutoCDC contract, only exact duplicate re-emissions (handled separately below) may
        // reuse sequences for a particular key. Otherwise, the behavior for two unique events for
        // the same key with the same sequence leads to undefined behavior. Each distinct event
        // creation for this key should increment `nextSequence`.
        nextSequence += 1
        eventsForKey += event

        if (rand.nextDouble() < duplicateEventProbability) {
          // Full duplicate events are intentionally not counted against `numUniqueEventsForKey`.
          // These differ from no-op upsert continuation events, as they share the same sequence as
          // their preceding event too, in addition to all other columns.
          eventsForKey += event
        }
      }

      allEvents.addAll(eventsForKey)
    }
    allEvents.sortBy(_.sequence).toSeq
  }

  /**
   * Feed `events` through an AutoCDC pipeline of `scdType` across `numBatches` microbatches
   * (one pipeline run per microbatch). The target and auxiliary tables are created by pipeline
   * materialization from the flow's inferred schema.
   */
  protected def runRandomCdcPipeline(
      targetTable: String,
      scdType: ScdType,
      events: Seq[SourceRow],
      numBatches: Int): Unit = {
    val session = spark
    import session.implicits._

    val stream = MemoryStream[SourceRow]
    val ctx = singleAutoCdcFlowPipeline(
      flowName = s"${targetTable}_flow",
      target = targetTable,
      sourceDf = stream.toDF().toDF(sourceColumnNames: _*),
      keys = Seq(keyColumn),
      sequencing = functions.col(sequenceColumn),
      columnSelection = Some(ColumnSelection.ExcludeColumns(
        Seq(UnqualifiedColumnName(isDeleteColumn))
      )),
      deleteCondition = Some(functions.col(isDeleteColumn) === true),
      scdType = scdType,
      trackHistorySelection = scdType match {
        case ScdType.Type1 => None
        case ScdType.Type2 => Some(ColumnSelection.ExcludeColumns(
          Seq(UnqualifiedColumnName(sequenceColumn))))
      }
    )
    val totalEvents = events.size
    (0 until numBatches).foreach { batchIndex =>
      val batchStart = batchIndex * totalEvents / numBatches
      val batchEnd = (batchIndex + 1) * totalEvents / numBatches
      stream.addData(events.slice(batchStart, batchEnd): _*)
      runPipeline(ctx)
    }
  }
}
