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
import org.apache.spark.memory.SparkOutOfMemoryError
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MILLIS
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.MutableColumnarRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{CalendarIntervalType, DecimalType, StringType, StructType}
import org.apache.spark.unsafe.KVIterator
import org.apache.spark.util.Utils

/**
 * Hash-based aggregate operator that can also fallback to sorting when data exceeds memory size.
 */
case class HashAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends AggregateCodegenSupport {

  require(HashAggregateExec.supportsAggregate(aggregateBufferAttributes))

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in aggregation build"),
    "avgHashProbe" ->
      SQLMetrics.createAverageMetric(sparkContext, "avg hash probes per key"),
    "numTasksFallBacked" -> SQLMetrics.createMetric(sparkContext, "number of sort fallback tasks"))

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
            numTasksFallBacked)
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
  private val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
  private val declFunctions = aggregateExpressions.map(_.aggregateFunction)
    .filter(_.isInstanceOf[DeclarativeAggregate])
    .map(_.asInstanceOf[DeclarativeAggregate])
  private val bufferSchema = StructType.fromAttributes(aggregateBufferAttributes)

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
        f.dataType.isInstanceOf[CalendarIntervalType]) &&
        bufferSchema.nonEmpty

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
        logInfo(s"${SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key} is set to true, but"
          + " current version of codegened fast hashmap does not support this aggregate.")
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
         |$thisPlan.getTaskContext().addTaskCompletionListener(
         |  new org.apache.spark.util.TaskCompletionListener() {
         |    @Override
         |    public void onTaskCompletion(org.apache.spark.TaskContext context) {
         |      $fastHashMapTerm.close();
         |    }
         |});
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

    val doAggFuncName = ctx.addNewFunction(doAgg,
      s"""
         |private void $doAgg() throws java.io.IOException {
         |  ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         |  $finishHashMap
         |}
       """.stripMargin)

    // generate code for output
    val keyTerm = ctx.freshName("aggKey")
    val bufferTerm = ctx.freshName("aggBuffer")
    val outputFunc = generateResultFunction(ctx)

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
        groupingKeySchema.toAttributes.zipWithIndex
          .map { case (attr, i) => BoundReference(i, attr.dataType, attr.nullable) }
      )
      val generateBufferRow = GenerateUnsafeProjection.createCode(ctx,
        bufferSchema.toAttributes.zipWithIndex.map { case (attr, i) =>
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

    val aggTime = metricTerm(ctx, "aggTime")
    val beforeAgg = ctx.freshName("beforeAgg")
    s"""
       |if (!$initAgg) {
       |  $initAgg = true;
       |  $createFastHashMap
       |  $addHookToCloseFastHashMap
       |  $hashMapTerm = $thisPlan.createHashMap();
       |  long $beforeAgg = System.nanoTime();
       |  $doAggFuncName();
       |  $aggTime.add((System.nanoTime() - $beforeAgg) / $NANOS_PER_MILLIS);
       |}
       |// output the result
       |$outputFromFastHashMap
       |$outputFromRegularHashMap
     """.stripMargin
  }

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

    val oomeClassName = classOf[SparkOutOfMemoryError].getName

    val findOrInsertRegularHashMap: String =
      s"""
         |// generate grouping key
         |${unsafeRowKeyCode.code}
         |int $unsafeRowKeyHash = ${unsafeRowKeyCode.value}.hashCode();
         |if ($checkFallbackForBytesToBytesMap) {
         |  // try to get the buffer from hash map
         |  $unsafeRowBuffer =
         |    $hashMapTerm.getAggregationBufferFromUnsafeRow($unsafeRowKeys, $unsafeRowKeyHash);
         |}
         |// Can't allocate buffer from the hash map. Spill the map and fallback to sort-based
         |// aggregation after processing all input rows.
         |if ($unsafeRowBuffer == null) {
         |  if ($sorterTerm == null) {
         |    $sorterTerm = $hashMapTerm.destructAndCreateExternalSorter();
         |  } else {
         |    $sorterTerm.merge($hashMapTerm.destructAndCreateExternalSorter());
         |  }
         |  $resetCounter
         |  // the hash map had be spilled, it should have enough memory now,
         |  // try to allocate buffer again.
         |  $unsafeRowBuffer = $hashMapTerm.getAggregationBufferFromUnsafeRow(
         |    $unsafeRowKeys, $unsafeRowKeyHash);
         |  if ($unsafeRowBuffer == null) {
         |    // failed to allocate the first page
         |    throw new $oomeClassName("No enough memory for aggregation");
         |  }
         |}
       """.stripMargin

    val findOrInsertHashMap: String = {
      if (isFastHashMapEnabled) {
        // If fast hash map is on, we first generate code to probe and update the fast hash map.
        // If the probe is successful the corresponding fast row buffer will hold the mutable row.
        s"""
           |${fastRowKeys.map(_.code).mkString("\n")}
           |if (${fastRowKeys.map("!" + _.isNull).mkString(" && ")}) {
           |  $fastRowBuffer = $fastHashMapTerm.findOrInsert(
           |    ${fastRowKeys.map(_.value).mkString(", ")});
           |}
           |// Cannot find the key in fast hash map, try regular hash map.
           |if ($fastRowBuffer == null) {
           |  $findOrInsertRegularHashMap
           |}
         """.stripMargin
      } else {
        findOrInsertRegularHashMap
      }
    }

    val inputAttrs = aggregateBufferAttributes ++ inputAttributes
    // Here we set `currentVars(0)` to `currentVars(numBufferSlots)` to null, so that when
    // generating code for buffer columns, we use `INPUT_ROW`(will be the buffer row), while
    // generating input columns, we use `currentVars`.
    ctx.currentVars = new Array[ExprCode](aggregateBufferAttributes.length) ++ input

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

    val declareRowBuffer: String = if (isFastHashMapEnabled) {
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

    // We try to do hash map based in-memory aggregation first. If there is not enough memory (the
    // hash map will return null for new key), we spill the hash map to disk to free memory, then
    // continue to do in-memory aggregation and spilling until all the rows had been processed.
    // Finally, sort the spilled aggregate buffers by key, and merge them together for same key.
    s"""
       |$declareRowBuffer
       |$findOrInsertHashMap
       |$incCounter
       |$updateRowInHashMap
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

object HashAggregateExec {
  def supportsAggregate(aggregateBufferAttributes: Seq[Attribute]): Boolean = {
    val aggregationBufferSchema = StructType.fromAttributes(aggregateBufferAttributes)
    UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema(aggregationBufferSchema)
  }
}
