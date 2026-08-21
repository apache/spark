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

package org.apache.spark.sql.execution.aggregate

import java.util.concurrent.TimeUnit._

import scala.collection.mutable

import org.apache.spark.TaskContext
import org.apache.spark.internal.LogKeys.CONFIG
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MILLIS
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.MutableColumnarRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{CalendarIntervalType, DecimalType, StringType}
import org.apache.spark.unsafe.KVIterator
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * Hash-based aggregate operator that can also fallback to sorting when data exceeds memory size.
 */
case class HashAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    isStreaming: Boolean,
    numShufflePartitions: Option[Int],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends AggregateCodegenSupport {

  require(Aggregate.supportsHashAggregate(aggregateBufferAttributes, groupingExpressions))

  override def allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in aggregation build"),
    "avgHashProbe" ->
      SQLMetrics.createAverageMetric(sparkContext, "avg hash probes per key"),
    "numTasksFallBacked" -> SQLMetrics.createMetric(sparkContext, "number of sort fallback tasks")
  ) ++ {
    // Only the aggregates that can actually bypass report this, so the rest do not show a
    // constant 0 in the SQL UI (see `UnionExec.metrics` for the same approach).
    if (adaptivePartialAggEnabled) {
      Map("numBypassingRows" -> SQLMetrics.createMetric(sparkContext, "number of bypassing rows"))
    } else {
      Map.empty[String, SQLMetric]
    }
  }

  // This is for testing. We force TungstenAggregationIterator to fall back to the unsafe row hash
  // map and/or the sort-based aggregation once it has processed a given number of input rows.
  private val testFallbackStartsAt: Option[(Int, Int)] = {
    Option(session).map { s =>
      s.conf.get("spark.sql.TungstenAggregate.testFallbackStartsAt", null)
    }.orNull match {
      case null | "" => None
      case fallbackStartsAt =>
        val splits = fallbackStartsAt.split(",").map(_.trim)
        Some((splits.head.toInt, splits.last.toInt))
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val peakMemory = longMetric("peakMemory")
    val spillSize = longMetric("spillSize")
    val avgHashProbe = longMetric("avgHashProbe")
    val aggTime = longMetric("aggTime")
    val numTasksFallBacked = longMetric("numTasksFallBacked")
    // Registered only when the feature applies, and only read from the pass-through path.
    val numBypassingRows = if (adaptivePartialAggEnabled) longMetric("numBypassingRows") else null

    child.execute().mapPartitionsWithIndex { (partIndex, iter) =>

      val beforeAgg = System.nanoTime()
      val hasInput = iter.hasNext
      val res = if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator.empty
      } else {
        val aggregationIterator =
          new TungstenAggregationIterator(
            partIndex,
            groupingExpressions,
            aggregateExpressions,
            aggregateAttributes,
            initialInputBufferOffset,
            resultExpressions,
            (expressions, inputSchema) =>
              MutableProjection.create(expressions, inputSchema),
            inputAttributes,
            iter,
            testFallbackStartsAt,
            numOutputRows,
            peakMemory,
            spillSize,
            avgHashProbe,
            numTasksFallBacked,
            numBypassingRows,
            aggTime,
            adaptivePartialAggEnabled,
            adaptiveMinRows,
            adaptiveMinCompaction)
        if (!hasInput && groupingExpressions.isEmpty) {
          numOutputRows += 1
          Iterator.single[UnsafeRow](aggregationIterator.outputForEmptyGroupingKeyWithoutInput())
        } else {
          aggregationIterator
        }
      }
      aggTime += NANOSECONDS.toMillis(System.nanoTime() - beforeAgg)
      res
    }
  }

  private val groupingAttributes = groupingExpressions.map(_.toAttribute)
  private val groupingKeySchema = DataTypeUtils.fromAttributes(groupingAttributes)
  private val declFunctions = aggregateExpressions.map(_.aggregateFunction)
    .filter(_.isInstanceOf[DeclarativeAggregate])
    .map(_.asInstanceOf[DeclarativeAggregate])
  private val bufferSchema = DataTypeUtils.fromAttributes(aggregateBufferAttributes)

  /**
   * Whether adaptive partial aggregation applies to this operator. When it does, the aggregation
   * may bypass partial aggregation at runtime and pass the remaining input rows through as
   * single-row partial buffers (see [[SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED]]). It only
   * applies to a pre-shuffle partial aggregation with grouping keys:
   *   - `Partial` and `PartialMerge` modes only: the downstream `Final` aggregation merges the
   *     passed-through single-row buffers. `Final`/`Complete` produce the result themselves and
   *     have no such downstream. A `PartialMerge` member is the non-distinct aggregate of the
   *     DISTINCT intermediate phase (`AggUtils.planAggregateWithOneDistinct`): its input row is
   *     already a partial buffer, so the pass-through applies the merge to an empty buffer, which
   *     leaves the incoming buffer unchanged, and the downstream `Final` re-merges it. A pure
   *     `PartialMerge` phase (the de-duplication on keys ++ distinct columns) must not bypass, or
   *     duplicate (key, distinct column) rows would over-count DISTINCT. The built-in planner
   *     never emits such a phase without a required distribution, so the
   *     `requiredChildDistributionExpressions` check below already keeps it out. The
   *     `exists(_.mode == Partial)` check is a defensive guard on top of it, and only for a
   *     de-duplication phase that carries non-distinct aggregates: one with none at all has an
   *     empty `aggregateExpressions`, is admitted by the `isEmpty` disjunct, and still relies on
   *     the distribution check alone.
   *   - grouping keys present: a global aggregation produces a single output row, so partial
   *     aggregation achieves the maximum reduction and must never be bypassed.
   *   - no required distribution: with no aggregate functions (a group-by-only aggregate) the
   *     mode check is vacuously true for both phases, so `requiredChildDistributionExpressions`
   *     tells them apart, the `Final` phase requiring a distribution and the pre-shuffle phase
   *     not. It is not a general pre-shuffle test, because `AggUtils.planAggregateWithOneDistinct`
   *     leaves it `None` on a post-shuffle aggregate. DISTINCT aggregate functions are allowed:
   *     the phase that de-duplicates on (keys ++ distinct columns) requires a distribution, so
   *     this check keeps it out, while the distinct partial phase that groups on the keys alone
   *     (carrying the non-distinct aggregates as `PartialMerge`) is eligible even though it sits
   *     after a shuffle: another `Exchange` and a `Final` follow it, so its passed-through
   *     buffers are still merged.
   *   - batch only: a streaming partial aggregate keeps state across batches, and it is built
   *     with all-`Partial` modes and no required distribution, so it would otherwise qualify.
   *     A batch `session_window` grouping likewise qualifies, but its partial aggregate feeds a
   *     `MergingSessionsExec` that merges overlapping sessions, so it is kept out for now. The
   *     static sibling `spark.sql.execution.bypassPartialAggregation` likewise stays away from
   *     streaming and `session_window` groupings.
   */
  private val adaptivePartialAggEnabled: Boolean = {
    conf.adaptivePartialAggregationEnabled &&
      groupingExpressions.nonEmpty &&
      !isStreaming &&
      !groupingExpressions.exists(_.metadata.contains(SessionWindow.marker)) &&
      aggregateExpressions.forall(a => a.mode == Partial || a.mode == PartialMerge) &&
      (aggregateExpressions.exists(_.mode == Partial) || aggregateExpressions.isEmpty) &&
      requiredChildDistributionExpressions.isEmpty
  }

  // The number of rows between two compaction-ratio evaluations, and the ratio below which the
  // partial aggregation is considered ineffective. Only read when the feature applies.
  private val adaptiveMinRows: Long = conf.adaptivePartialAggregationMinRows
  private val adaptiveMinCompaction: Double = conf.adaptivePartialAggregationMinCompaction

  // The name for Fast HashMap
  private var fastHashMapTerm: String = _
  private var isFastHashMapEnabled: Boolean = false

  // whether a vectorized hashmap is used instead
  // we have decided to always use the row-based hashmap,
  // but the vectorized hashmap can still be switched on for testing and benchmarking purposes.
  private var isVectorizedHashMapEnabled: Boolean = false

  // The name for UnsafeRow HashMap
  private var hashMapTerm: String = _
  private var sorterTerm: String = _

  // Codegen state for adaptive partial aggregation. When the aggregation maps stop collapsing
  // enough rows, the operator stops populating them and instead streams each remaining row through
  // as a single-row partial buffer for the Final aggregate to merge. The compaction ratio is
  // measured at the operator level: all processed rows against the keys held by both the fast and
  // the regular map, so two-level-map routing does not change the decision.
  private var adaptivePassThroughTerm: String = _
  private var processedRowsTerm: String = _
  private var adaptiveChildrenConsumedTerm: String = _
  // Whether the map output has already been emitted (and the maps freed). Once pass-through is
  // active the maps are frozen; draining them -- which also frees them -- releases their memory
  // before the rest of the input is streamed. The drain starts as soon as the first passed-through
  // row queues a copy behind the maps, advancing one map row per queued row (see
  // `handlePassThroughRow`); this flag lets the later output skip the maps.
  private var adaptiveMapOutputDoneTerm: String = _
  // Whether the map iterators have been set up (`finishHashMap`). The map-output function may be
  // re-entered when its loops return via `shouldStop()` to drain the buffer, and `finishAggregate`
  // destructs the map, so the setup must run only once.
  private var adaptiveMapSetupDoneTerm: String = _
  // The processed-row count at which the compaction ratio is evaluated next. It advances by
  // `minRows` after every check, and the count is reset after a spill so the new in-memory map
  // epoch is judged on its own rows.
  private var adaptiveNextCheckRowTerm: String = _
  // The fast map's key count as of the last spill. The fast map never spills and is never cleared,
  // so its keys outlive the epoch the processed-row count is reset for; subtracting this baseline
  // keeps both sides of the ratio on the same epoch. Once the fast map fills it stops accepting
  // keys, so the difference settles at zero and the ratio is the regular map's alone.
  private var adaptiveFastKeysAtSpillTerm: String = _
  // The name of the generated output function, promoted to a field so `doConsumeWithKeys` can emit
  // pass-through rows directly from within the build loop.
  private var outputFunc: String = _
  // The name of the generated function that drains the frozen maps and, once they are fully
  // drained, flushes the queue of passed-through rows held behind them. Promoted to a field so
  // `doConsumeWithKeys` can advance the maps one row per passed-through row (see
  // `handlePassThroughRow`).
  private var adaptiveOutputMapAndFlushFuncName: String = _
  // The queue of passed-through rows, held behind the frozen maps so the maps are emitted first.
  // Each queued row advances the map output by one row, so the queue stays bounded by how many
  // passed-through rows a child emits before it honours `shouldStop()` (see
  // `handlePassThroughRow`).
  private var adaptivePendingRowsTerm: String = _

  /**
   * This is called by generated Java class, should be public.
   */
  def createHashMap(): UnsafeFixedWidthAggregationMap = {
    // create initialized aggregate buffer
    val initExpr = declFunctions.flatMap(f => f.initialValues)
    val initialBuffer = UnsafeProjection.create(initExpr)(EmptyRow)

    // create hashMap
    new UnsafeFixedWidthAggregationMap(
      initialBuffer,
      bufferSchema,
      groupingKeySchema,
      TaskContext.get(),
      1024 * 16, // initial capacity
      TaskContext.get().taskMemoryManager().pageSizeBytes
    )
  }

  def getTaskContext(): TaskContext = {
    TaskContext.get()
  }

  /**
   * Registers a task-completion hook to close the generated fast hash map, so that the close hook
   * is a plain method call on this plan, with the listener being a lambda in compiled Scala code,
   * rather than an anonymous `TaskCompletionListener` emitted per fast hash map (one fewer
   * generated inner class per map). This is called by the generated Java class, should be public.
   */
  def addFastHashMapCloseHook(fastHashMap: AutoCloseable): Unit = {
    TaskContext.get().addTaskCompletionListener[Unit](_ => fastHashMap.close())
  }

  def getEmptyAggregationBuffer(): InternalRow = {
    val initExpr = declFunctions.flatMap(f => f.initialValues)
    val initialBuffer = UnsafeProjection.create(initExpr)(EmptyRow)
    initialBuffer
  }

  /**
   * This is called by generated Java class, should be public.
   */
  def createUnsafeJoiner(): UnsafeRowJoiner = {
    GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)
  }

  /**
   * Called by generated Java class to finish the aggregate and return a KVIterator.
   */
  def finishAggregate(
      hashMap: UnsafeFixedWidthAggregationMap,
      sorter: UnsafeKVExternalSorter,
      peakMemory: SQLMetric,
      spillSize: SQLMetric,
      avgHashProbe: SQLMetric,
      numTasksFallBacked: SQLMetric): KVIterator[UnsafeRow, UnsafeRow] = {

    // update peak execution memory
    val mapMemory = hashMap.getPeakMemoryUsedBytes
    val sorterMemory = Option(sorter).map(_.getPeakMemoryUsedBytes).getOrElse(0L)
    val maxMemory = Math.max(mapMemory, sorterMemory)
    val metrics = TaskContext.get().taskMetrics()
    peakMemory.add(maxMemory)
    metrics.incPeakExecutionMemory(maxMemory)

    // Update average hashmap probe
    avgHashProbe.set(hashMap.getAvgHashProbesPerKey)

    if (sorter == null) {
      // not spilled
      return hashMap.iterator()
    }

    // merge the final hashMap into sorter
    numTasksFallBacked += 1
    sorter.merge(hashMap.destructAndCreateExternalSorter())
    hashMap.free()
    val sortedIter = sorter.sortedIterator()

    // Create a KVIterator based on the sorted iterator.
    new KVIterator[UnsafeRow, UnsafeRow] {

      // Create a MutableProjection to merge the rows of same key together
      val mergeExpr = declFunctions.flatMap(_.mergeExpressions)
      val mergeProjection = MutableProjection.create(
        mergeExpr,
        aggregateBufferAttributes ++ declFunctions.flatMap(_.inputAggBufferAttributes))
      val joinedRow = new JoinedRow()

      var currentKey: UnsafeRow = null
      var currentRow: UnsafeRow = null
      var nextKey: UnsafeRow = if (sortedIter.next()) {
        sortedIter.getKey
      } else {
        null
      }

      override def next(): Boolean = {
        if (nextKey != null) {
          currentKey = nextKey.copy()
          currentRow = sortedIter.getValue.copy()
          nextKey = null
          // use the first row as aggregate buffer
          mergeProjection.target(currentRow)

          // merge the following rows with same key together
          var findNextGroup = false
          while (!findNextGroup && sortedIter.next()) {
            val key = sortedIter.getKey
            if (currentKey.equals(key)) {
              mergeProjection(joinedRow(currentRow, sortedIter.getValue))
            } else {
              // We find a new group.
              findNextGroup = true
              nextKey = key
            }
          }

          true
        } else {
          spillSize.add(sorter.getSpillSize)
          false
        }
      }

      override def getKey: UnsafeRow = currentKey
      override def getValue: UnsafeRow = currentRow
      override def close(): Unit = {
        sortedIter.close()
      }
    }
  }

  /**
   * Generate the code for output.
   * @return function name for the result code.
   */
  private def generateResultFunction(ctx: CodegenContext): String = {
    val funcName = ctx.freshName("doAggregateWithKeysOutput")
    val keyTerm = ctx.freshName("keyTerm")
    val bufferTerm = ctx.freshName("bufferTerm")
    val numOutput = metricTerm(ctx, "numOutputRows")

    val body =
    if (modes.contains(Final) || modes.contains(Complete)) {
      // generate output using resultExpressions
      ctx.currentVars = null
      ctx.INPUT_ROW = keyTerm
      val keyVars = groupingExpressions.zipWithIndex.map { case (e, i) =>
        BoundReference(i, e.dataType, e.nullable).genCode(ctx)
      }
      val evaluateKeyVars = evaluateVariables(keyVars)
      ctx.INPUT_ROW = bufferTerm
      val bufferVars = aggregateBufferAttributes.zipWithIndex.map { case (e, i) =>
        BoundReference(i, e.dataType, e.nullable).genCode(ctx)
      }
      val evaluateBufferVars = evaluateVariables(bufferVars)
      // evaluate the aggregation result
      ctx.currentVars = bufferVars
      val aggResults = bindReferences(
        declFunctions.map(_.evaluateExpression),
        aggregateBufferAttributes).map(_.genCode(ctx))
      val evaluateAggResults = evaluateVariables(aggResults)
      // generate the final result
      ctx.currentVars = keyVars ++ aggResults
      val inputAttrs = groupingAttributes ++ aggregateAttributes
      val resultVars = bindReferences[Expression](
        resultExpressions,
        inputAttrs).map(_.genCode(ctx))
      val evaluateNondeterministicResults =
        evaluateNondeterministicVariables(output, resultVars, resultExpressions)
      s"""
         |$evaluateKeyVars
         |$evaluateBufferVars
         |$evaluateAggResults
         |$evaluateNondeterministicResults
         |${consume(ctx, resultVars)}
       """.stripMargin
    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // resultExpressions are Attributes of groupingExpressions and aggregateBufferAttributes.
      assert(resultExpressions.forall(_.isInstanceOf[Attribute]))
      assert(resultExpressions.length ==
        groupingExpressions.length + aggregateBufferAttributes.length)

      ctx.currentVars = null

      ctx.INPUT_ROW = keyTerm
      val keyVars = groupingExpressions.zipWithIndex.map { case (e, i) =>
        BoundReference(i, e.dataType, e.nullable).genCode(ctx)
      }
      val evaluateKeyVars = evaluateVariables(keyVars)

      ctx.INPUT_ROW = bufferTerm
      val resultBufferVars = aggregateBufferAttributes.zipWithIndex.map { case (e, i) =>
        BoundReference(i, e.dataType, e.nullable).genCode(ctx)
      }
      val evaluateResultBufferVars = evaluateVariables(resultBufferVars)

      ctx.currentVars = keyVars ++ resultBufferVars
      val inputAttrs = resultExpressions.map(_.toAttribute)
      val resultVars = bindReferences[Expression](
        resultExpressions,
        inputAttrs).map(_.genCode(ctx))
      s"""
         |$evaluateKeyVars
         |$evaluateResultBufferVars
         |${consume(ctx, resultVars)}
       """.stripMargin
    } else {
      // generate result based on grouping key
      ctx.INPUT_ROW = keyTerm
      ctx.currentVars = null
      val resultVars = bindReferences[Expression](
        resultExpressions,
        groupingAttributes).map(_.genCode(ctx))
      val evaluateNondeterministicResults =
        evaluateNondeterministicVariables(output, resultVars, resultExpressions)
      s"""
         |$evaluateNondeterministicResults
         |${consume(ctx, resultVars)}
       """.stripMargin
    }
    ctx.addNewFunction(funcName,
      s"""
         |private void $funcName(UnsafeRow $keyTerm, UnsafeRow $bufferTerm)
         |    throws java.io.IOException {
         |  $numOutput.add(1);
         |  $body
         |}
       """.stripMargin)
  }

  /**
   * A required check for any fast hash map implementation (basically the common requirements
   * for row-based and vectorized).
   * Currently fast hash map is supported for primitive data types during partial aggregation.
   * This list of supported use-cases should be expanded over time.
   */
  private def checkIfFastHashMapSupported(): Boolean = {
    val isSupported =
      (groupingKeySchema ++ bufferSchema).forall(f => CodeGenerator.isPrimitiveType(f.dataType) ||
        f.dataType.isInstanceOf[DecimalType] || f.dataType.isInstanceOf[StringType] ||
        f.dataType.isInstanceOf[CalendarIntervalType])

    // For vectorized hash map, We do not support byte array based decimal type for aggregate values
    // as ColumnVector.putDecimal for high-precision decimals doesn't currently support in-place
    // updates. Due to this, appending the byte array in the vectorized hash map can turn out to be
    // quite inefficient and can potentially OOM the executor.
    // For row-based hash map, while decimal update is supported in UnsafeRow, we will just act
    // conservative here, due to lack of testing and benchmarking.
    val isNotByteArrayDecimalType = bufferSchema.map(_.dataType).filter(_.isInstanceOf[DecimalType])
      .forall(!DecimalType.isByteArrayDecimalType(_))

    val isEnabledForAggModes =
      if (modes.forall(mode => mode == Partial || mode == PartialMerge)) {
        true
      } else {
        !conf.getConf(SQLConf.ENABLE_TWOLEVEL_AGG_MAP_PARTIAL_ONLY)
      }

    isSupported && isNotByteArrayDecimalType && isEnabledForAggModes
  }

  private def enableTwoLevelHashMap(): Unit = {
    if (!checkIfFastHashMapSupported()) {
      if (!Utils.isTesting) {
        logInfo(log"${MDC(CONFIG, SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key)} is set to true, but" +
          log" current version of codegened fast hashmap does not support this aggregate.")
      }
    } else {
      isFastHashMapEnabled = true

      // This is for testing/benchmarking only.
      // We enforce to first level to be a vectorized hashmap, instead of the default row-based one.
      isVectorizedHashMapEnabled = conf.enableVectorizedHashMap
    }
  }

  protected override def needHashTable: Boolean = true

  protected override def doProduceWithKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "initAgg")
    if (adaptivePartialAggEnabled) {
      adaptivePassThroughTerm =
        ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "adaptivePassThrough")
      processedRowsTerm = ctx.addMutableState(CodeGenerator.JAVA_LONG, "processedRows")
      adaptiveNextCheckRowTerm =
        ctx.addMutableState(CodeGenerator.JAVA_LONG, "adaptiveNextCheckRow",
          v => s"$v = ${adaptiveMinRows}L;")
      adaptiveChildrenConsumedTerm =
        ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "adaptiveChildrenConsumed")
      adaptiveMapOutputDoneTerm =
        ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "adaptiveMapOutputDone")
      adaptiveMapSetupDoneTerm =
        ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "adaptiveMapSetupDone")
      adaptiveFastKeysAtSpillTerm =
        ctx.addMutableState(CodeGenerator.JAVA_INT, "adaptiveFastKeysAtSpill")
      adaptivePendingRowsTerm = ctx.addMutableState(
        "java.util.LinkedList<UnsafeRow[]>", "adaptivePendingRows",
        v => s"$v = new java.util.LinkedList<UnsafeRow[]>();", forceInline = true)
    }
    if (conf.enableTwoLevelAggMap) {
      enableTwoLevelHashMap()
    } else if (conf.enableVectorizedHashMap) {
      logWarning("Two level hashmap is disabled but vectorized hashmap is enabled.")
    }
    val bitMaxCapacity = testFallbackStartsAt match {
      case Some((fastMapCounter, _)) =>
        // In testing, with fall back counter of fast hash map (`fastMapCounter`), set the max bit
        // of map to be no more than log2(`fastMapCounter`). This helps control the number of keys
        // in map to mimic fall back.
        if (fastMapCounter <= 1) {
          0
        } else {
          (math.log10(fastMapCounter) / math.log10(2)).floor.toInt
        }
      case _ => conf.fastHashAggregateRowMaxCapacityBit
    }

    val thisPlan = ctx.addReferenceObj("plan", this)

    // Create a name for the iterator from the fast hash map, and the code to create fast hash map.
    val (iterTermForFastHashMap, createFastHashMap) = if (isFastHashMapEnabled) {
      // Generates the fast hash map class and creates the fast hash map term.
      val fastHashMapClassName = ctx.freshName("FastHashMap")
      if (isVectorizedHashMapEnabled) {
        val generatedMap = new VectorizedHashMapGenerator(ctx, aggregateExpressions,
          fastHashMapClassName, groupingKeySchema, bufferSchema, bitMaxCapacity).generate()
        ctx.addInnerClass(generatedMap)

        // Inline mutable state since not many aggregation operations in a task
        fastHashMapTerm = ctx.addMutableState(
          fastHashMapClassName, "vectorizedFastHashMap", forceInline = true)
        val iter = ctx.addMutableState(
          "java.util.Iterator<InternalRow>",
          "vectorizedFastHashMapIter",
          forceInline = true)
        val create = s"$fastHashMapTerm = new $fastHashMapClassName();"
        (iter, create)
      } else {
        val generatedMap = new RowBasedHashMapGenerator(ctx, aggregateExpressions,
          fastHashMapClassName, groupingKeySchema, bufferSchema, bitMaxCapacity).generate()
        ctx.addInnerClass(generatedMap)

        // Inline mutable state since not many aggregation operations in a task
        fastHashMapTerm = ctx.addMutableState(
          fastHashMapClassName, "fastHashMap", forceInline = true)
        val iter = ctx.addMutableState(
          "org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow>",
          "fastHashMapIter", forceInline = true)
        val create = s"$fastHashMapTerm = new $fastHashMapClassName(" +
          s"$thisPlan.getTaskContext().taskMemoryManager(), " +
          s"$thisPlan.getEmptyAggregationBuffer());"
        (iter, create)
      }
    } else ("", "")

    // Generates the code to register a cleanup task with TaskContext to ensure that memory
    // is guaranteed to be freed at the end of the task. This is necessary to avoid memory
    // leaks in when the downstream operator does not fully consume the aggregation map's
    // output (e.g. aggregate followed by limit).
    val addHookToCloseFastHashMap = if (isFastHashMapEnabled) {
      s"""
         |$thisPlan.addFastHashMapCloseHook($fastHashMapTerm);
       """.stripMargin
    } else ""

    // Create a name for the iterator from the regular hash map.
    // Inline mutable state since not many aggregation operations in a task
    val iterTerm = ctx.addMutableState(classOf[KVIterator[UnsafeRow, UnsafeRow]].getName,
      "mapIter", forceInline = true)
    // create hashMap
    val hashMapClassName = classOf[UnsafeFixedWidthAggregationMap].getName
    hashMapTerm = ctx.addMutableState(hashMapClassName, "hashMap", forceInline = true)
    sorterTerm = ctx.addMutableState(classOf[UnsafeKVExternalSorter].getName, "sorter",
      forceInline = true)

    val doAgg = ctx.freshName("doAggregateWithKeys")
    val peakMemory = metricTerm(ctx, "peakMemory")
    val spillSize = metricTerm(ctx, "spillSize")
    val avgHashProbe = metricTerm(ctx, "avgHashProbe")
    val numTasksFallBacked = metricTerm(ctx, "numTasksFallBacked")

    val finishRegularHashMap = s"$iterTerm = $thisPlan.finishAggregate(" +
      s"$hashMapTerm, $sorterTerm, $peakMemory, $spillSize, $avgHashProbe, $numTasksFallBacked);"
    val finishHashMap = if (isFastHashMapEnabled) {
      s"""
         |$iterTermForFastHashMap = $fastHashMapTerm.rowIterator();
         |$finishRegularHashMap
       """.stripMargin
    } else {
      finishRegularHashMap
    }

    // `partitionIndex` is passed as a parameter so any bare `partitionIndex`
    // reference in the child's produce resolves to the local parameter, not
    // the protected `BufferedRowIterator.partitionIndex` field. When
    // `addNewFunction` spills this helper into a nested class (as can happen
    // once the outer class passes the code-size threshold), the bare field
    // reference fails with `IllegalAccessError`.

    // Generate code for output. With adaptive partial aggregation enabled this must happen before
    // the `doAgg` helper below, because `doConsumeWithKeys` (invoked from the child's produce
    // inside `doAgg`) emits pass-through rows by calling this output function directly. Otherwise
    // the output function is generated after `doAgg`, so the early `consume(...)` inside it cannot
    // change the codegen layout for plans the feature never touches.
    val keyTerm = ctx.freshName("aggKey")
    val bufferTerm = ctx.freshName("aggBuffer")
    if (adaptivePartialAggEnabled) {
      outputFunc = generateResultFunction(ctx)
    }

    // After the child input is consumed, finish the build: with adaptive partial aggregation mark
    // that the child is fully consumed (to support re-entry; the map iterators are set up inside
    // the map-output function), otherwise set up the map iterators for the output below.
    // A child may leave its produce loop early rather than exhausting its input: `UnionExec` runs
    // each child inside its own helper, so a streamed row that fills the output buffer returns
    // only as far as here. Reaching the end of `produce` therefore does not by itself mean the
    // input is consumed -- pending output says the child parked instead, and the build resumes on
    // re-entry. Without this the rest of that partition is silently dropped.
    val postChildProduce = if (adaptivePartialAggEnabled) {
      s"if (!shouldStop()) { $adaptiveChildrenConsumedTerm = true; }"
    } else {
      finishHashMap
    }
    val limitNotReachedCondition = limitNotReachedCond

    def outputFromFastHashMap: String = {
      if (isFastHashMapEnabled) {
        if (isVectorizedHashMapEnabled) {
          outputFromVectorizedMap
        } else {
          outputFromRowBasedMap
        }
      } else ""
    }

    def outputFromRowBasedMap: String = {
      s"""
         |while ($limitNotReachedCondition $iterTermForFastHashMap.next()) {
         |  UnsafeRow $keyTerm = (UnsafeRow) $iterTermForFastHashMap.getKey();
         |  UnsafeRow $bufferTerm = (UnsafeRow) $iterTermForFastHashMap.getValue();
         |  $outputFunc($keyTerm, $bufferTerm);
         |
         |  if (shouldStop()) return;
         |}
         |$fastHashMapTerm.close();
       """.stripMargin
    }

    // Iterate over the aggregate rows and convert them from InternalRow to UnsafeRow
    def outputFromVectorizedMap: String = {
      val row = ctx.freshName("fastHashMapRow")
      ctx.currentVars = null
      ctx.INPUT_ROW = row
      val generateKeyRow = GenerateUnsafeProjection.createCode(ctx,
        toAttributes(groupingKeySchema).zipWithIndex
          .map { case (attr, i) => BoundReference(i, attr.dataType, attr.nullable) }
      )
      val generateBufferRow = GenerateUnsafeProjection.createCode(ctx,
        toAttributes(bufferSchema).zipWithIndex.map { case (attr, i) =>
          BoundReference(groupingKeySchema.length + i, attr.dataType, attr.nullable)
        })
      s"""
         |while ($limitNotReachedCondition $iterTermForFastHashMap.hasNext()) {
         |  InternalRow $row = (InternalRow) $iterTermForFastHashMap.next();
         |  ${generateKeyRow.code}
         |  ${generateBufferRow.code}
         |  $outputFunc(${generateKeyRow.value}, ${generateBufferRow.value});
         |
         |  if (shouldStop()) return;
         |}
         |
         |$fastHashMapTerm.close();
       """.stripMargin
    }

    def outputFromRegularHashMap: String = {
      s"""
         |while ($limitNotReachedCondition $iterTerm.next()) {
         |  UnsafeRow $keyTerm = (UnsafeRow) $iterTerm.getKey();
         |  UnsafeRow $bufferTerm = (UnsafeRow) $iterTerm.getValue();
         |  $outputFunc($keyTerm, $bufferTerm);
         |  if (shouldStop()) return;
         |}
         |$iterTerm.close();
         |if ($sorterTerm == null) {
         |  $hashMapTerm.free();
         |}
       """.stripMargin
    }

    // With adaptive partial aggregation the maps are frozen once pass-through is active, so their
    // output (which also frees them) can happen as soon as pass-through fires, releasing the memory
    // before the remaining input is streamed. The output loops are wrapped in a function so the
    // same code runs either early (once pass-through freezes the maps) or at the end of the build.
    // The done flag is set inside, after the loops, so a mid-output drain (the loops return via
    // `shouldStop()`) leaves it unset and the caller resumes the map iterator on re-entry; once it
    // is set the maps have been fully output and freed and will not be touched again. The iterator
    // setup (`finishHashMap`, which destructs the map) is guarded to run only once. The queue of
    // passed-through rows held behind the maps is flushed here, right after the done flag is set:
    // the maps must precede the held rows so the downstream Final merges a group's map buffer
    // before its pass-through buffers (a group can straddle the freeze, since the maps only freeze
    // once pass-through fires), and the copies were made when the row was queued, so emitting them
    // here cannot corrupt the build's reusable rows.
    adaptiveOutputMapAndFlushFuncName = if (adaptivePartialAggEnabled) {
      val name = ctx.freshName("outputMapAndFlush")
      val pair = ctx.freshName("pendingRow")
      ctx.addNewFunction(name,
        s"""
           |private void $name() throws java.io.IOException {
           |  if (!$adaptiveMapSetupDoneTerm) {
           |    $finishHashMap
           |    $adaptiveMapSetupDoneTerm = true;
           |  }
           |  $outputFromFastHashMap
           |  $outputFromRegularHashMap
           |  $adaptiveMapOutputDoneTerm = true;
           |  while (!$adaptivePendingRowsTerm.isEmpty()) {
           |    UnsafeRow[] $pair = (UnsafeRow[]) $adaptivePendingRowsTerm.poll();
           |    $outputFunc($pair[0], $pair[1]);
           |  }
           |}
         """.stripMargin)
    } else {
      ""
    }

    val doAggFuncName = ctx.addNewFunction(doAgg,
      s"""
         |private void $doAgg(int partitionIndex) throws java.io.IOException {
         |  ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         |  $postChildProduce
         |}
       """.stripMargin)
    // For a non-adaptive plan, generate the output function only after `doAgg` so its
    // `consume(...)` cannot reorder the codegen layout (see above). It must still land before
    // `adaptiveFinalOutput` reads it below.
    if (!adaptivePartialAggEnabled) {
      outputFunc = generateResultFunction(ctx)
    }

    val aggTime = metricTerm(ctx, "aggTime")
    val beforeAgg = ctx.freshName("beforeAgg")
    // Split by an exchange, `doAgg` may start appending pass-through rows to the output buffer
    // mid-build. In that case `shouldStop()` becomes true and we must return so the buffered rows
    // are drained; on re-entry the frozen maps are output first (`adaptiveOutputMapAndFlush`) and
    // then the build resumes (guarded by `childrenConsumed`) until the child input is exhausted.
    // Fused with the Final, nothing is buffered, so `shouldStop()` never fires and the maps are
    // drained inside the build (see `handlePassThroughRow`).
    val adaptiveStopCheck = if (adaptivePartialAggEnabled) {
      "if (shouldStop()) return;"
    } else {
      ""
    }
    // Once pass-through is active the maps are frozen, so output them (releasing their memory)
    // ahead of the passed-through rows. The output starts the moment the first passed-through row
    // queues behind the maps (`handlePassThroughRow` advances the maps by one row per queued row),
    // and continues on re-entry here; `adaptiveFinalOutput` covers the case where pass-through
    // never fired during the build, when the full maps are the result. The output loops return via
    // `shouldStop()` when the buffer fills, so the done flag is set inside the output function and
    // re-entry resumes the map iterator. Only once the maps are fully drained are the queued
    // pass-through rows flushed, so every map buffer precedes the rows that collided with it.
    val adaptiveOutputMapAndFlush = if (adaptivePartialAggEnabled) {
      s"""
         |if (!$adaptiveMapOutputDoneTerm) {
         |  $adaptiveOutputMapAndFlushFuncName();
         |  if (shouldStop()) return;
         |}
       """.stripMargin
    } else {
      ""
    }
    val adaptiveResumeBuild = if (adaptivePartialAggEnabled) {
      val beforeResumedAgg = ctx.freshName("beforeResumedAgg")
      s"""
         |if (!$adaptiveChildrenConsumedTerm) {
         |  $adaptiveOutputMapAndFlush
         |  long $beforeResumedAgg = System.nanoTime();
         |  $doAggFuncName(partitionIndex);
         |  $aggTime.add((System.nanoTime() - $beforeResumedAgg) / $NANOS_PER_MILLIS);
         |  if (shouldStop()) return;
         |}
       """.stripMargin
    } else {
      ""
    }
    val adaptiveFinalOutput = if (adaptivePartialAggEnabled) {
      adaptiveOutputMapAndFlush
    } else {
      s"""
         |$outputFromFastHashMap
         |$outputFromRegularHashMap
       """.stripMargin
    }
    s"""
       |if (!$initAgg) {
       |  $initAgg = true;
       |  $createFastHashMap
       |  $addHookToCloseFastHashMap
       |  $hashMapTerm = $thisPlan.createHashMap();
       |  long $beforeAgg = System.nanoTime();
       |  $doAggFuncName(partitionIndex);
       |  $aggTime.add((System.nanoTime() - $beforeAgg) / $NANOS_PER_MILLIS);
       |  $adaptiveStopCheck
       |}
       |$adaptiveResumeBuild
       |$adaptiveFinalOutput
     """.stripMargin
  }

  // Blocking operators normally suppress the child's `shouldStop()` check because they buffer all
  // output. With adaptive partial aggregation, pass-through rows are appended to the output buffer
  // while consuming child input, so the stop check is re-enabled to let the child yield between
  // rows.
  //
  // This bounds the buffer only as far as the child honours it. Each passed-through row queues a
  // copy and advances the frozen-map output by one row, and that one appended map row makes
  // `shouldStop()` true, so a child that checks between rows never queues more than one row. A
  // one-to-many child that does not check `shouldStop()` inside its fan-out (`GenerateExec` emits
  // `for (index ...) { consume }` and `while (iterator.hasNext()) { consume }` with no check)
  // appends every row produced from one input row before it can yield: the fan-out batch lands in
  // `BufferedRowIterator.currentRows` (which is not spillable), and the map advances one row per
  // queued row, so the buffer grows to roughly the batch width rather than `minRows`.
  override def needStopCheck: Boolean = adaptivePartialAggEnabled

  // Blocking operators normally do not copy their result because every output row is drained (via
  // `shouldStop()`) before the next one is produced. Adaptive pass-through breaks that assumption:
  // `outputFunc` writes every output row into the same reusable result `UnsafeRow`, and under a
  // fan-out child several such rows are appended without an intervening drain - the frozen-map
  // rows, emitted one per queued row inside the child's fan-out loop, and the held pass-through
  // batch, flushed from `outputMapAndFlush` after the maps drain. Without a copy they would all
  // alias the single result row. Such children report `needCopyResult` themselves, so propagate
  // their requirement rather than copying for every adaptive aggregate.
  override def needCopyResult: Boolean = adaptivePartialAggEnabled &&
    child.asInstanceOf[CodegenSupport].needCopyResult

  protected override def doConsumeWithKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // create grouping key
    val unsafeRowKeyCode = GenerateUnsafeProjection.createCode(
      ctx, bindReferences[Expression](groupingExpressions, child.output))
    val fastRowKeys = ctx.generateExpressions(
      bindReferences[Expression](groupingExpressions, child.output))
    val unsafeRowKeys = unsafeRowKeyCode.value
    val unsafeRowKeyHash = ctx.freshName("unsafeRowKeyHash")
    val unsafeRowBuffer = ctx.freshName("unsafeRowAggBuffer")
    val fastRowBuffer = ctx.freshName("fastAggBuffer")

    // For adaptive partial aggregation pass-through, each bypassed row is emitted as a single-row
    // partial buffer: start from the initial aggregation buffer, apply the update expressions once,
    // and output `key ++ buffer` for the Final aggregate to merge. This projects the initial
    // buffer.
    val emptyAggBufferCode = if (adaptivePartialAggEnabled) {
      GenerateUnsafeProjection.createCode(ctx, declFunctions.flatMap(f => f.initialValues))
    } else {
      null
    }
    // Per-row local flag marking that the current row is being streamed through (held by no map).
    val adaptiveRowBypassedTerm = ctx.freshName("adaptiveRowBypassed")

    // To individually generate code for each aggregate function, an element in `updateExprs` holds
    // all the expressions for the buffer of an aggregation function.
    val updateExprs = aggregateExpressions.map { e =>
      // only have DeclarativeAggregate
      e.mode match {
        case Partial | Complete =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].updateExpressions
        case PartialMerge | Final =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].mergeExpressions
      }
    }

    val (checkFallbackForBytesToBytesMap, resetCounter, incCounter) = testFallbackStartsAt match {
      case Some((_, regularMapCounter)) =>
        val countTerm = ctx.addMutableState(CodeGenerator.JAVA_INT, "fallbackCounter")
        (s"$countTerm < $regularMapCounter", s"$countTerm = 0;", s"$countTerm += 1;")
      case _ => ("true", "", "")
    }

    // The compaction ratio is measured at the operator level: all processed rows against the keys
    // held by both maps, so two-level-map routing does not change the decision. The same predicate
    // decides both check points -- periodically every `minRows` rows, and right before the map
    // would spill (in which case the spill is skipped entirely). `minRows = 0` disables the
    // periodic check: the row count is only ever compared after being incremented past 0, so it
    // never matches and only the spill check remains.
    val adaptiveIneffective = if (adaptivePartialAggEnabled) {
      val totalKeys = if (isFastHashMapEnabled) {
        s"($fastHashMapTerm.getNumKeys() - $adaptiveFastKeysAtSpillTerm + " +
          s"$hashMapTerm.getNumKeys())"
      } else {
        s"$hashMapTerm.getNumKeys()"
      }
      s"$processedRowsTerm < (double) $totalKeys * ${adaptiveMinCompaction}D"
    } else {
      ""
    }

    val findOrInsertRegularHashMap: String = {
      // Assumes the grouping key projection (`unsafeRowKeyCode.code`) has already run for this row,
      // so `unsafeRowKeyCode.value` holds the current key. The projection is emitted exactly once
      // per regular-map row (see below); emitting it in more than one runtime branch is unsafe
      // because the projection's subexpression/writer state assigned in one branch would be read
      // stale from another (e.g. the adaptive pass-through path would reuse the last probed key).
      val probeRegularMap =
        s"""
           |int $unsafeRowKeyHash = ${unsafeRowKeyCode.value}.hashCode();
           |if ($checkFallbackForBytesToBytesMap) {
           |  // try to get the buffer from hash map
           |  $unsafeRowBuffer =
           |    $hashMapTerm.getAggregationBufferFromUnsafeRow($unsafeRowKeys, $unsafeRowKeyHash);
           |}
         """.stripMargin

      val spillMap =
        s"""
           |if ($sorterTerm == null) {
           |  $sorterTerm = $hashMapTerm.destructAndCreateExternalSorter();
           |} else {
           |  $sorterTerm.merge($hashMapTerm.destructAndCreateExternalSorter());
           |}
           |$resetCounter
           |// the hash map had been spilled, so it should have enough memory now,
           |// try to allocate buffer again.
           |$unsafeRowBuffer = $hashMapTerm.getAggregationBufferFromUnsafeRow(
           |  $unsafeRowKeys, $unsafeRowKeyHash);
           |if ($unsafeRowBuffer == null) {
           |  // failed to allocate the first page
           |  throw QueryExecutionErrors.aggregateOutOfMemoryError();
           |}
         """.stripMargin

      if (adaptivePartialAggEnabled) {
        // Spilling starts a new in-memory epoch, so the counters restart and the ratio of that
        // epoch alone decides the remaining rows. The fast map neither spills nor clears, so its
        // keys outlive the epoch; snapshotting them here keeps both sides of the ratio on the
        // same rows. Once the fast map fills it stops accepting keys and the difference settles
        // at zero, leaving the regular map's keys alone.
        val snapshotFastKeys = if (isFastHashMapEnabled) {
          s"$adaptiveFastKeysAtSpillTerm = $fastHashMapTerm.getNumKeys();"
        } else {
          ""
        }
        val spillAndRestartEpoch =
          s"""
             |$spillMap
             |$processedRowsTerm = 0L;
             |$adaptiveNextCheckRowTerm = ${adaptiveMinRows}L;
             |$snapshotFastKeys
           """.stripMargin
        s"""
           |// generate grouping key
           |${unsafeRowKeyCode.code}
           |if (!$adaptivePassThroughTerm) {
           |  $probeRegularMap
           |  if ($unsafeRowBuffer == null) {
           |    if ($processedRowsTerm > 0 && $adaptiveIneffective) {
           |      $adaptivePassThroughTerm = true;
           |    } else {
           |      $spillAndRestartEpoch
           |    }
           |  }
           |}
         """.stripMargin
      } else {
        s"""
           |// generate grouping key
           |${unsafeRowKeyCode.code}
           |$probeRegularMap
           |// Can't allocate buffer from the hash map. Spill the map and fallback to sort-based
           |// aggregation after processing all input rows.
           |if ($unsafeRowBuffer == null) {
           |  $spillMap
           |}
         """.stripMargin
      }
    }

    val findOrInsertHashMap: String = {
      val findCode = if (isFastHashMapEnabled) {
        // If fast hash map is on, we first generate code to probe and update the fast hash map.
        // If the probe is successful the corresponding fast row buffer will hold the mutable row.
        // Once adaptive pass-through is active, skip the fast map entirely so the row is streamed
        // through instead of being inserted anywhere.
        val fastMapProbe =
          s"""
             |${fastRowKeys.map(_.code).mkString("\n")}
             |if (${fastRowKeys.map("!" + _.isNull).mkString(" && ")}) {
             |  $fastRowBuffer = $fastHashMapTerm.findOrInsert(
             |    ${fastRowKeys.map(_.value).mkString(", ")});
             |}
           """.stripMargin
        val guardedFastMapProbe = if (adaptivePartialAggEnabled) {
          s"""
             |if (!$adaptivePassThroughTerm) {
             |  $fastMapProbe
             |}
           """.stripMargin
        } else {
          fastMapProbe
        }
        s"""
           |$guardedFastMapProbe
           |// Cannot find the key in fast hash map, try regular hash map.
           |if ($fastRowBuffer == null) {
           |  $findOrInsertRegularHashMap
           |}
         """.stripMargin
      } else {
        findOrInsertRegularHashMap
      }

      // Every row is either accepted by an aggregation map or streamed through -- the fast map
      // serves a row without it ever reaching the regular map, so both buffers are consulted to
      // tell the two apart.
      //
      // An accepted row counts toward the compaction ratio, so the numerator matches the
      // operator-level denominator. Counting inside the regular-map branch alone would drop the
      // rows the fast map absorbed from the ratio and bypass an aggregation that is in fact
      // reducing. A row no map holds is streamed once pass-through is active: both probes are
      // skipped (guarded above), so neither buffer is set, and `rowBypassed` marks exactly those
      // rows. The row that fails to insert at the spill boundary lands here too, while the row
      // that merely flipped pass-through at the check point is already aggregated in the map that
      // took it and must not be re-emitted.
      val countOrPassThroughRow = if (adaptivePartialAggEnabled) {
        val heldByAMap = if (isFastHashMapEnabled) {
          s"($fastRowBuffer != null || $unsafeRowBuffer != null)"
        } else {
          s"($unsafeRowBuffer != null)"
        }
        // The grouping key was already projected in `findOrInsertRegularHashMap`
        // (`unsafeRowKeyCode.code`), so `unsafeRowKeyCode.value` holds this row's key. Only build
        // the single-row partial buffer here.
        s"""
           |if ($heldByAMap) {
           |  if (!$adaptivePassThroughTerm) {
           |    $processedRowsTerm += 1;
           |    if ($processedRowsTerm == $adaptiveNextCheckRowTerm) {
           |      if ($adaptiveIneffective) {
           |        $adaptivePassThroughTerm = true;
           |      } else {
           |        $adaptiveNextCheckRowTerm += ${adaptiveMinRows}L;
           |      }
           |    }
           |  }
           |} else if ($adaptivePassThroughTerm) {
           |  $adaptiveRowBypassedTerm = true;
           |  ${emptyAggBufferCode.code}
           |  $unsafeRowBuffer = ${emptyAggBufferCode.value};
           |}
         """.stripMargin
      } else {
        ""
      }

      s"""
         |$findCode
         |$countOrPassThroughRow
       """.stripMargin
    }

    val inputAttrs = aggregateBufferAttributes ++ inputAttributes
    // Here we set `currentVars(0)` to `currentVars(numBufferSlots)` to null, so that when
    // generating code for buffer columns, we use `INPUT_ROW`(will be the buffer row), while
    // generating input columns, we use `currentVars`.
    ctx.currentVars = (new Array[ExprCode](aggregateBufferAttributes.length) ++ input)
      .toImmutableArraySeq

    val aggNames = aggregateExpressions.map(_.aggregateFunction.prettyName)
    // Computes start offsets for each aggregation function code
    // in the underlying buffer row.
    val bufferStartOffsets = {
      val offsets = mutable.ArrayBuffer[Int]()
      var curOffset = 0
      updateExprs.foreach { exprsForOneFunc =>
        offsets += curOffset
        curOffset += exprsForOneFunc.length
      }
      offsets.toArray
    }

    val updateRowInRegularHashMap: String = {
      ctx.INPUT_ROW = unsafeRowBuffer
      val boundUpdateExprs = updateExprs.map { updateExprsForOneFunc =>
        bindReferences(updateExprsForOneFunc, inputAttrs)
      }
      val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExprs.flatten)
      val effectiveCodes = ctx.evaluateSubExprEliminationState(subExprs.states.values)
      val unsafeRowBufferEvals = boundUpdateExprs.map { boundUpdateExprsForOneFunc =>
        ctx.withSubExprEliminationExprs(subExprs.states) {
          boundUpdateExprsForOneFunc.map(_.genCode(ctx))
        }
      }

      val aggCodeBlocks = updateExprs.indices.map { i =>
        val rowBufferEvalsForOneFunc = unsafeRowBufferEvals(i)
        val boundUpdateExprsForOneFunc = boundUpdateExprs(i)
        val bufferOffset = bufferStartOffsets(i)

        // All the update code for aggregation buffers should be placed in the end
        // of each aggregation function code.
        val updateRowBuffers = rowBufferEvalsForOneFunc.zipWithIndex.map { case (ev, j) =>
          val updateExpr = boundUpdateExprsForOneFunc(j)
          val dt = updateExpr.dataType
          val nullable = updateExpr.nullable
          CodeGenerator.updateColumn(unsafeRowBuffer, dt, bufferOffset + j, ev, nullable)
        }
        code"""
           |${ctx.registerComment(s"evaluate aggregate function for ${aggNames(i)}")}
           |${evaluateVariables(rowBufferEvalsForOneFunc)}
           |${ctx.registerComment("update unsafe row buffer")}
           |${updateRowBuffers.mkString("\n").trim}
         """.stripMargin
      }

      val codeToEvalAggFuncs = generateEvalCodeForAggFuncs(
        ctx, input, inputAttrs, boundUpdateExprs, aggNames, aggCodeBlocks, subExprs)
      s"""
         |// common sub-expressions
         |$effectiveCodes
         |// evaluate aggregate functions and update aggregation buffers
         |$codeToEvalAggFuncs
       """.stripMargin
    }

    val updateRowInHashMap: String = {
      if (isFastHashMapEnabled) {
        if (isVectorizedHashMapEnabled) {
          ctx.INPUT_ROW = fastRowBuffer
          val boundUpdateExprs = updateExprs.map { updateExprsForOneFunc =>
            bindReferences(updateExprsForOneFunc, inputAttrs)
          }
          val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExprs.flatten)
          val effectiveCodes = ctx.evaluateSubExprEliminationState(subExprs.states.values)
          val fastRowEvals = boundUpdateExprs.map { boundUpdateExprsForOneFunc =>
            ctx.withSubExprEliminationExprs(subExprs.states) {
              boundUpdateExprsForOneFunc.map(_.genCode(ctx))
            }
          }

          val aggCodeBlocks = fastRowEvals.zipWithIndex.map { case (fastRowEvalsForOneFunc, i) =>
            val boundUpdateExprsForOneFunc = boundUpdateExprs(i)
            val bufferOffset = bufferStartOffsets(i)
            // All the update code for aggregation buffers should be placed in the end
            // of each aggregation function code.
            val updateRowBuffer = fastRowEvalsForOneFunc.zipWithIndex.map { case (ev, j) =>
              val updateExpr = boundUpdateExprsForOneFunc(j)
              val dt = updateExpr.dataType
              val nullable = updateExpr.nullable
              CodeGenerator.updateColumn(fastRowBuffer, dt, bufferOffset + j, ev, nullable,
                isVectorized = true)
            }
            code"""
               |${ctx.registerComment(s"evaluate aggregate function for ${aggNames(i)}")}
               |${evaluateVariables(fastRowEvalsForOneFunc)}
               |${ctx.registerComment("update fast row")}
               |${updateRowBuffer.mkString("\n").trim}
             """.stripMargin
          }

          val codeToEvalAggFuncs = generateEvalCodeForAggFuncs(
            ctx, input, inputAttrs, boundUpdateExprs, aggNames, aggCodeBlocks, subExprs)

          // If vectorized fast hash map is on, we first generate code to update row
          // in vectorized fast hash map, if the previous loop up hit vectorized fast hash map.
          // Otherwise, update row in regular hash map.
          s"""
             |if ($fastRowBuffer != null) {
             |  // common sub-expressions
             |  $effectiveCodes
             |  // evaluate aggregate functions and update aggregation buffers
             |  $codeToEvalAggFuncs
             |} else {
             |  $updateRowInRegularHashMap
             |}
          """.stripMargin
        } else {
          // If row-based hash map is on and the previous loop up hit fast hash map,
          // we reuse regular hash buffer to update row of fast hash map.
          // Otherwise, update row in regular hash map.
          s"""
             |// Updates the proper row buffer
             |if ($fastRowBuffer != null) {
             |  $unsafeRowBuffer = $fastRowBuffer;
             |}
             |$updateRowInRegularHashMap
          """.stripMargin
        }
      } else {
        updateRowInRegularHashMap
      }
    }

    val declareRowBuffer: String = {
      val declareBuffers = if (isFastHashMapEnabled) {
        val fastRowType = if (isVectorizedHashMapEnabled) {
          classOf[MutableColumnarRow].getName
        } else {
          "UnsafeRow"
        }
        s"""
           |UnsafeRow $unsafeRowBuffer = null;
           |$fastRowType $fastRowBuffer = null;
         """.stripMargin
      } else {
        s"UnsafeRow $unsafeRowBuffer = null;"
      }
      val declareBypassed = if (adaptivePartialAggEnabled) {
        s"boolean $adaptiveRowBypassedTerm = false;"
      } else {
        ""
      }
      s"""
         |$declareBuffers
         |$declareBypassed
       """.stripMargin
    }

    // We try to do hash map based in-memory aggregation first. If there is not enough memory (the
    // hash map will return null for new key), we spill the hash map to disk to free memory, then
    // continue to do in-memory aggregation and spilling until all the rows had been processed.
    // Finally, sort the spilled aggregate buffers by key, and merge them together for same key.
    //
    // With adaptive partial aggregation, once pass-through is active `updateRowInHashMap` fills the
    // single-row buffer built above; we then emit `key ++ buffer` straight to the parent so the row
    // skips both the fast map and the regular map.
    //
    // The maps were frozen when pass-through fired, so a group can straddle the freeze and hold
    // both a map buffer and pass-through buffers. The downstream Final merges in emit order, so
    // every map buffer must precede the pass-through buffers of the same group. We hold each
    // passed-through row in a queue behind the maps: the row is queued as a copy, and queuing it
    // advances the map output by one row. That one appended map row is what makes `shouldStop()`
    // true, so a child that honours the check yields at the end of its batch and never queues more
    // than one row; a one-to-many child that cannot yield mid-fan-out queues its whole batch at
    // once, bounding the queue by the batch width. The map drains one row per queued row (the same
    // cadence the 1:1 shape already uses), and once the maps are fully drained the queue is
    // flushed, so the maps always precede the held rows. Fused with the Final, nothing is
    // buffered, `shouldStop()` never fires, and the single output call drains the whole map before
    // the held row follows.
    val handlePassThroughRow = if (adaptivePartialAggEnabled) {
      val numBypassingRows = metricTerm(ctx, "numBypassingRows")
      s"""
         |if ($adaptiveRowBypassedTerm) {
         |  $numBypassingRows.add(1);
         |  if ($adaptiveMapOutputDoneTerm) {
         |    $outputFunc(${unsafeRowKeyCode.value}, $unsafeRowBuffer);
         |  } else {
         |    $adaptivePendingRowsTerm.add(new UnsafeRow[] {
         |      ${unsafeRowKeyCode.value}.copy(), $unsafeRowBuffer.copy() });
         |    $adaptiveOutputMapAndFlushFuncName();
         |  }
         |}
       """.stripMargin
    } else {
      ""
    }
    s"""
       |$declareRowBuffer
       |$findOrInsertHashMap
       |$incCounter
       |$updateRowInHashMap
       |$handlePassThroughRow
     """.stripMargin
  }

  override def verboseString(maxFields: Int): String = toString(verbose = true, maxFields)

  override def simpleString(maxFields: Int): String = toString(verbose = false, maxFields)

  private def toString(verbose: Boolean, maxFields: Int): String = {
    val allAggregateExpressions = aggregateExpressions

    testFallbackStartsAt match {
      case None =>
        val keyString = truncatedString(groupingExpressions, "[", ", ", "]", maxFields)
        val functionString = truncatedString(allAggregateExpressions, "[", ", ", "]", maxFields)
        val outputString = truncatedString(output, "[", ", ", "]", maxFields)
        if (verbose) {
          s"HashAggregate(keys=$keyString, functions=$functionString, output=$outputString)"
        } else {
          s"HashAggregate(keys=$keyString, functions=$functionString)"
        }
      case Some(fallbackStartsAt) =>
        s"HashAggregateWithControlledFallback $groupingExpressions " +
          s"$allAggregateExpressions $resultExpressions fallbackStartsAt=$fallbackStartsAt"
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): HashAggregateExec =
    copy(child = newChild)
}
