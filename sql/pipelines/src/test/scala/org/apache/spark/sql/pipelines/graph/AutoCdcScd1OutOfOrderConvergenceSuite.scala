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
import org.apache.spark.sql.pipelines.autocdc.{
  ColumnSelection,
  Scd1BatchProcessor,
  UnqualifiedColumnName
}
import org.apache.spark.sql.pipelines.graph.AutoCdcScd1OutOfOrderConvergenceSuite.SourceRow
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

object AutoCdcScd1OutOfOrderConvergenceSuite {
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
 * Differential test for the SCD1 AutoCDC merge's order-invariance property: feeding the same
 * randomly-generated CDC event stream as a single sorted micro-batch and as several shuffled
 * micro-batches must converge to the same target table contents.
 */
class AutoCdcScd1OutOfOrderConvergenceSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  private val numDistinctKeys: Int = 3
  private val maxEventsPerKey: Int = 25
  private val deleteEventProbability: Double = 0.20
  private val triplicateEventProbability: Double = 0.15
  private val numOutOfOrderBatches: Int = 4

  private val keyColumn: String = "key"
  private val nameColumn: String = "name"
  private val amountColumn: String = "amount"
  private val activeColumn: String = "active"
  private val sequenceColumn: String = "sequence"
  private val isDeleteColumn: String = "is_delete"

  private val sourceColumnNames: Seq[String] =
    Seq(keyColumn, nameColumn, amountColumn, activeColumn, sequenceColumn, isDeleteColumn)

  private def randomOptional[T](rand: Random, value: => T, nullProbability: Double = 0.25)
      : Option[T] =
    if (rand.nextDouble() < nullProbability) None else Some(value)

  private def randomUpsertOrDelete(
      rand: Random, key: Int, sequence: Long, isDelete: Boolean): SourceRow = {
    val colorPalette = Seq("red", "blue", "green", "yellow")
    SourceRow(
      key = key,
      name = randomOptional(rand, colorPalette(rand.nextInt(colorPalette.length))),
      amount = randomOptional(rand, rand.nextInt(200) - 100),
      active = randomOptional(rand, rand.nextBoolean()),
      sequence = sequence,
      isDelete = isDelete
    )
  }

  private def generateRandomCdcEventStream(rand: Random): Seq[SourceRow] = {
    var nextSequence: Long = 0L
    val events = ArrayBuffer.empty[SourceRow]
    (0 until numDistinctKeys).foreach { _ =>
      val key = rand.nextInt(50)
      val numEventsForKey = 1 + rand.nextInt(maxEventsPerKey)
      var keyHasLiveRow = false
      (0 until numEventsForKey).foreach { _ =>
        val isDelete = keyHasLiveRow && rand.nextDouble() < deleteEventProbability
        keyHasLiveRow = !isDelete
        val event = randomUpsertOrDelete(rand, key, nextSequence, isDelete)
        nextSequence += 1
        if (rand.nextDouble() < triplicateEventProbability) {
          events += event
          events += event
          events += event
        } else {
          events += event
        }
      }
    }
    events.sortBy(_.sequence).toSeq
  }

  /** Build a pipeline context with a single SCD1 AutoCDC flow reading from `stream`. */
  private def buildPipelineContext(
      targetTable: String,
      stream: MemoryStream[SourceRow]): TestGraphRegistrationContext = {
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
        ))
      ))
    }
  }

  private def createTargetTable(targetTable: String): Unit = {
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.$targetTable (" +
      s"$keyColumn INT NOT NULL, " +
      s"$nameColumn STRING, " +
      s"$amountColumn INT, " +
      s"$activeColumn BOOLEAN, " +
      s"$sequenceColumn BIGINT NOT NULL, " +
      s"$cdcMetadataDdl)"
    )
  }

  // The two pipelines see the same logical events but not the same per-row sequence values
  // along the way, so `_cdc_metadata` (which holds the per-row delete/upsert sequence) can
  // legitimately differ even when the visible row content matches. Drop it before comparing.
  private def assertTargetsConverge(inOrderTable: String, outOfOrderTable: String): Unit = {
    val cdcMetadataColumn = Scd1BatchProcessor.cdcMetadataColName
    checkAnswer(
      spark.table(s"$catalog.$namespace.$outOfOrderTable").drop(cdcMetadataColumn),
      spark.table(s"$catalog.$namespace.$inOrderTable").drop(cdcMetadataColumn)
    )
  }

  private def runConvergenceTest(seed: Long): Unit = {
    val session = spark
    import session.implicits._

    val rand = new Random(seed)
    val sortedEventStream = generateRandomCdcEventStream(rand)
    val shuffledEventStream = rand.shuffle(sortedEventStream)

    withClue(
      s"\nseed=$seed\n" +
      s"events (${sortedEventStream.size} total, sorted by sequence):\n" +
      sortedEventStream.map(r => s"  $r").mkString("\n") + "\n"
    ) {
      val inOrderTable = "inorder_target"
      val outOfOrderTable = "outoforder_target"
      createTargetTable(inOrderTable)
      createTargetTable(outOfOrderTable)

      val inOrderStream = MemoryStream[SourceRow]
      val inOrderCtx = buildPipelineContext(inOrderTable, inOrderStream)
      inOrderStream.addData(sortedEventStream: _*)
      runPipeline(inOrderCtx)

      val outOfOrderStream = MemoryStream[SourceRow]
      val outOfOrderCtx = buildPipelineContext(outOfOrderTable, outOfOrderStream)
      val totalEvents = shuffledEventStream.size
      (0 until numOutOfOrderBatches).foreach { batchIndex =>
        val batchStart = batchIndex * totalEvents / numOutOfOrderBatches
        val batchEnd = (batchIndex + 1) * totalEvents / numOutOfOrderBatches
        outOfOrderStream.addData(shuffledEventStream.slice(batchStart, batchEnd): _*)
        runPipeline(outOfOrderCtx)
      }

      assertTargetsConverge(inOrderTable, outOfOrderTable)
    }
  }

  test("SCD1 merge converges across micro-batch shuffling for randomly generated " +
    "CDC events (seed 1)") {
    runConvergenceTest(seed = 1L)
  }

  test("SCD1 merge converges across micro-batch shuffling for randomly generated " +
    "CDC events (seed 2)") {
    runConvergenceTest(seed = 2L)
  }

  test("SCD1 merge converges across micro-batch shuffling for randomly generated " +
    "CDC events (seed 3)") {
    runConvergenceTest(seed = 3L)
  }
}
