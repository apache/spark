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

import org.apache.spark.TaskContext
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.MutableColumnarRow
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}
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
  extends UnaryExecNode with CodegenSupport {

  private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  require(HashAggregateExec.supportsAggregate(aggregateBufferAttributes))

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "aggregate time"),
    "avgHashProbe" -> SQLMetrics.createAverageMetric(sparkContext, "avg hash probe"))

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
    AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
    AttributeSet(aggregateBufferAttributes)

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) if exprs.nonEmpty => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  // This is for testing. We force TungstenAggregationIterator to fall back to the unsafe row hash
  // map and/or the sort-based aggregation once it has processed a given number of input rows.
  private val testFallbackStartsAt: Option[(Int, Int)] = {
    sqlContext.getConf("spark.sql.TungstenAggregate.testFallbackStartsAt", null) match {
      case null | "" => None
      case fallbackStartsAt =>
        val splits = fallbackStartsAt.split(",").map(_.trim)
        Some((splits.head.toInt, splits.last.toInt))
    }
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val numOutputRows = longMetric("numOutputRows")
    val peakMemory = longMetric("peakMemory")
    val spillSize = longMetric("spillSize")
    val avgHashProbe = longMetric("avgHashProbe")
    val aggTime = longMetric("aggTime")

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
              newMutableProjection(expressions, inputSchema, subexpressionEliminationEnabled),
            child.output,
            iter,
            testFallbackStartsAt,
            numOutputRows,
            peakMemory,
            spillSize,
            avgHashProbe)
        if (!hasInput && groupingExpressions.isEmpty) {
          numOutputRows += 1
          Iterator.single[UnsafeRow](aggregationIterator.outputForEmptyGroupingKeyWithoutInput())
        } else {
          aggregationIterator
        }
      }
      aggTime += (System.nanoTime() - beforeAgg) / 1000000
      res
    }
  }

  // all the mode of aggregate expressions
  private val modes = aggregateExpressions.map(_.mode).distinct

  override def usedInputs: AttributeSet = inputSet

  override def supportCodegen: Boolean = {
    // ImperativeAggregate is not supported right now
    !aggregateExpressions.exists(_.aggregateFunction.isInstanceOf[ImperativeAggregate])
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  // The result rows come from the aggregate buffer, or a single row(no grouping keys), so this
  // operator doesn't need to copy its result even if its child does.
  override def needCopyResult: Boolean = false

  // Aggregate operator always consumes all the input rows before outputting any result, so we
  // don't need a stop check before aggregating.
  override def needStopCheck: Boolean = false

  protected override def doProduce(ctx: CodegenContext): String = {
    if (groupingExpressions.isEmpty) {
      doProduceWithoutKeys(ctx)
    } else {
      doProduceWithKeys(ctx)
    }
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    if (groupingExpressions.isEmpty) {
      doConsumeWithoutKeys(ctx, input)
    } else {
      doConsumeWithKeys(ctx, input)
    }
  }

  // The variables used as aggregation buffer. Only used for aggregation without keys.
  private var bufVars: Seq[ExprCode] = _

  private def doProduceWithoutKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "initAgg")
    // The generated function doesn't have input row in the code context.
    ctx.INPUT_ROW = null

    // generate variables for aggregation buffer
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val initExpr = functions.flatMap(f => f.initialValues)
    bufVars = initExpr.map { e =>
      val isNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "bufIsNull")
      val value = ctx.addMutableState(CodeGenerator.javaType(e.dataType), "bufValue")
      // The initial expression should not access any column
      val ev = e.genCode(ctx)
      val initVars = code"""
         | $isNull = ${ev.isNull};
         | $value = ${ev.value};
       """.stripMargin
      ExprCode(
        ev.code + initVars,
        JavaCode.isNullGlobal(isNull),
        JavaCode.global(value, e.dataType))
    }
    val initBufVar = evaluateVariables(bufVars)

    // generate variables for output
    val (resultVars, genResult) = if (modes.contains(Final) || modes.contains(Complete)) {
      // evaluate aggregate results
      ctx.currentVars = bufVars
      val aggResults = functions.map(_.evaluateExpression).map { e =>
        BindReferences.bindReference(e, aggregateBufferAttributes).genCode(ctx)
      }
      val evaluateAggResults = evaluateVariables(aggResults)
      // evaluate result expressions
      ctx.currentVars = aggResults
      val resultVars = resultExpressions.map { e =>
        BindReferences.bindReference(e, aggregateAttributes).genCode(ctx)
      }
      (resultVars, s"""
        |$evaluateAggResults
        |${evaluateVariables(resultVars)}
       """.stripMargin)
    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // output the aggregate buffer directly
      (bufVars, "")
    } else {
      // no aggregate function, the result should be literals
      val resultVars = resultExpressions.map(_.genCode(ctx))
      (resultVars, evaluateVariables(resultVars))
    }

    val doAgg = ctx.freshName("doAggregateWithoutKey")
    val doAggFuncName = ctx.addNewFunction(doAgg,
      s"""
         | private void $doAgg() throws java.io.IOException {
         |   // initialize aggregation buffer
         |   $initBufVar
         |
         |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         | }
       """.stripMargin)

    val numOutput = metricTerm(ctx, "numOutputRows")
    val aggTime = metricTerm(ctx, "aggTime")
    val beforeAgg = ctx.freshName("beforeAgg")
    s"""
       | while (!$initAgg) {
       |   $initAgg = true;
       |   long $beforeAgg = System.nanoTime();
       |   $doAggFuncName();
       |   $aggTime.add((System.nanoTime() - $beforeAgg) / 1000000);
       |
       |   // output the result
       |   ${genResult.trim}
       |
       |   $numOutput.add(1);
       |   ${consume(ctx, resultVars).trim}
       | }
     """.stripMargin
  }

  private def doConsumeWithoutKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // only have DeclarativeAggregate
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val inputAttrs = functions.flatMap(_.aggBufferAttributes) ++ child.output
    val updateExpr = aggregateExpressions.flatMap { e =>
      e.mode match {
        case Partial | Complete =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].updateExpressions
        case PartialMerge | Final =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].mergeExpressions
      }
    }
    ctx.currentVars = bufVars ++ input
    val boundUpdateExpr = updateExpr.map(BindReferences.bindReference(_, inputAttrs))
    val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExpr)
    val effectiveCodes = subExprs.codes.mkString("\n")
    val aggVals = ctx.withSubExprEliminationExprs(subExprs.states) {
      boundUpdateExpr.map(_.genCode(ctx))
    }
    // aggregate buffer should be updated atomic
    val updates = aggVals.zipWithIndex.map { case (ev, i) =>
      s"""
         | ${bufVars(i).isNull} = ${ev.isNull};
         | ${bufVars(i).value} = ${ev.value};
       """.stripMargin
    }
    s"""
       | // do aggregate
       | // common sub-expressions
       | $effectiveCodes
       | // evaluate aggregate function
       | ${evaluateVariables(aggVals)}
       | // update aggregation buffer
       | ${updates.mkString("\n").trim}
     """.stripMargin
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

  def getTaskMemoryManager(): TaskMemoryManager = {
    TaskContext.get().taskMemoryManager()
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
      avgHashProbe: SQLMetric): KVIterator[UnsafeRow, UnsafeRow] = {

    // update peak execution memory
    val mapMemory = hashMap.getPeakMemoryUsedBytes
    val sorterMemory = Option(sorter).map(_.getPeakMemoryUsedBytes).getOrElse(0L)
    val maxMemory = Math.max(mapMemory, sorterMemory)
    val metrics = TaskContext.get().taskMetrics()
    peakMemory.add(maxMemory)
    metrics.incPeakExecutionMemory(maxMemory)

    // Update average hashmap probe
    avgHashProbe.set(hashMap.getAverageProbesPerLookup())

    if (sorter == null) {
      // not spilled
      return hashMap.iterator()
    }

    // merge the final hashMap into sorter
    sorter.merge(hashMap.destructAndCreateExternalSorter())
    hashMap.free()
    val sortedIter = sorter.sortedIterator()

    // Create a KVIterator based on the sorted iterator.
    new KVIterator[UnsafeRow, UnsafeRow] {

      // Create a MutableProjection to merge the rows of same key together
      val mergeExpr = declFunctions.flatMap(_.mergeExpressions)
      val mergeProjection = newMutableProjection(
        mergeExpr,
        aggregateBufferAttributes ++ declFunctions.flatMap(_.inputAggBufferAttributes),
        subexpressionEliminationEnabled)
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
      val aggResults = declFunctions.map(_.evaluateExpression).map { e =>
        BindReferences.bindReference(e, aggregateBufferAttributes).genCode(ctx)
      }
      val evaluateAggResults = evaluateVariables(aggResults)
      // generate the final result
      ctx.currentVars = keyVars ++ aggResults
      val inputAttrs = groupingAttributes ++ aggregateAttributes
      val resultVars = resultExpressions.map { e =>
        BindReferences.bindReference(e, inputAttrs).genCode(ctx)
      }
      val evaluateNondeterministicResults =
        evaluateNondeterministicVariables(output, resultVars, resultExpressions)
      s"""
       $evaluateKeyVars
       $evaluateBufferVars
       $evaluateAggResults
       $evaluateNondeterministicResults
       ${consume(ctx, resultVars)}
       """
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
      val resultVars = resultExpressions.map { e =>
        BindReferences.bindReference(e, inputAttrs).genCode(ctx)
      }
      s"""
       $evaluateKeyVars
       $evaluateResultBufferVars
       ${consume(ctx, resultVars)}
       """
    } else {
      // generate result based on grouping key
      ctx.INPUT_ROW = keyTerm
      ctx.currentVars = null
      val resultVars = resultExpressions.map{ e =>
        BindReferences.bindReference(e, groupingAttributes).genCode(ctx)
      }
      val evaluateNondeterministicResults =
        evaluateNondeterministicVariables(output, resultVars, resultExpressions)
      s"""
       $evaluateNondeterministicResults
       ${consume(ctx, resultVars)}
       """
    }
    ctx.addNewFunction(funcName,
      s"""
        private void $funcName(UnsafeRow $keyTerm, UnsafeRow $bufferTerm)
            throws java.io.IOException {
          $numOutput.add(1);
          $body
        }
       """)
  }

  /**
   * A required check for any fast hash map implementation (basically the common requirements
   * for row-based and vectorized).
   * Currently fast hash map is supported for primitive data types during partial aggregation.
   * This list of supported use-cases should be expanded over time.
   */
  private def checkIfFastHashMapSupported(ctx: CodegenContext): Boolean = {
    val isSupported =
      (groupingKeySchema ++ bufferSchema).forall(f => CodeGenerator.isPrimitiveType(f.dataType) ||
        f.dataType.isInstanceOf[DecimalType] || f.dataType.isInstanceOf[StringType]) &&
        bufferSchema.nonEmpty && modes.forall(mode => mode == Partial || mode == PartialMerge)

    // For vectorized hash map, We do not support byte array based decimal type for aggregate values
    // as ColumnVector.putDecimal for high-precision decimals doesn't currently support in-place
    // updates. Due to this, appending the byte array in the vectorized hash map can turn out to be
    // quite inefficient and can potentially OOM the executor.
    // For row-based hash map, while decimal update is supported in UnsafeRow, we will just act
    // conservative here, due to lack of testing and benchmarking.
    val isNotByteArrayDecimalType = bufferSchema.map(_.dataType).filter(_.isInstanceOf[DecimalType])
      .forall(!DecimalType.isByteArrayDecimalType(_))

    isSupported  && isNotByteArrayDecimalType
  }

  private def enableTwoLevelHashMap(ctx: CodegenContext): Unit = {
    if (!checkIfFastHashMapSupported(ctx)) {
      if (modes.forall(mode => mode == Partial || mode == PartialMerge) && !Utils.isTesting) {
        logInfo("spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but"
          + " current version of codegened fast hashmap does not support this aggregate.")
      }
    } else {
      isFastHashMapEnabled = true

      // This is for testing/benchmarking only.
      // We enforce to first level to be a vectorized hashmap, instead of the default row-based one.
      isVectorizedHashMapEnabled = sqlContext.getConf(
        "spark.sql.codegen.aggregate.map.vectorized.enable", "false") == "true"
    }
  }

  private def doProduceWithKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "initAgg")
    if (sqlContext.conf.enableTwoLevelAggMap) {
      enableTwoLevelHashMap(ctx)
    } else {
      sqlContext.getConf("spark.sql.codegen.aggregate.map.vectorized.enable", null) match {
        case "true" =>
          logWarning("Two level hashmap is disabled but vectorized hashmap is enabled.")
        case _ =>
      }
    }
    val bitMaxCapacity = sqlContext.conf.fastHashAggregateRowMaxCapacityBit

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
          s"$thisPlan.getTaskMemoryManager(), $thisPlan.getEmptyAggregationBuffer());"
        (iter, create)
      }
    } else ("", "")

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

    val finishRegularHashMap = s"$iterTerm = $thisPlan.finishAggregate(" +
      s"$hashMapTerm, $sorterTerm, $peakMemory, $spillSize, $avgHashProbe);"
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
         |while ($iterTermForFastHashMap.next()) {
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
         |while ($iterTermForFastHashMap.hasNext()) {
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
         |while ($iterTerm.next()) {
         |  UnsafeRow $keyTerm = (UnsafeRow) $iterTerm.getKey();
         |  UnsafeRow $bufferTerm = (UnsafeRow) $iterTerm.getValue();
         |  $outputFunc($keyTerm, $bufferTerm);
         |
         |  if (shouldStop()) return;
         |}
       """.stripMargin
    }

    val aggTime = metricTerm(ctx, "aggTime")
    val beforeAgg = ctx.freshName("beforeAgg")
    s"""
     if (!$initAgg) {
       $initAgg = true;
       $createFastHashMap
       $hashMapTerm = $thisPlan.createHashMap();
       long $beforeAgg = System.nanoTime();
       $doAggFuncName();
       $aggTime.add((System.nanoTime() - $beforeAgg) / 1000000);
     }

     // output the result
     $outputFromFastHashMap
     $outputFromRegularHashMap

     $iterTerm.close();
     if ($sorterTerm == null) {
       $hashMapTerm.free();
     }
     """
  }

  private def doConsumeWithKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // create grouping key
    val unsafeRowKeyCode = GenerateUnsafeProjection.createCode(
      ctx, groupingExpressions.map(e => BindReferences.bindReference[Expression](e, child.output)))
    val fastRowKeys = ctx.generateExpressions(
      groupingExpressions.map(e => BindReferences.bindReference[Expression](e, child.output)))
    val unsafeRowKeys = unsafeRowKeyCode.value
    val unsafeRowBuffer = ctx.freshName("unsafeRowAggBuffer")
    val fastRowBuffer = ctx.freshName("fastAggBuffer")

    // only have DeclarativeAggregate
    val updateExpr = aggregateExpressions.flatMap { e =>
      e.mode match {
        case Partial | Complete =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].updateExpressions
        case PartialMerge | Final =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].mergeExpressions
      }
    }

    // generate hash code for key
    // SPARK-24076: HashAggregate uses the same hash algorithm on the same expressions
    // as ShuffleExchange, it may lead to bad hash conflict when shuffle.partitions=8192*n,
    // pick a different seed to avoid this conflict
    val hashExpr = Murmur3Hash(groupingExpressions, 48)
    val hashEval = BindReferences.bindReference(hashExpr, child.output).genCode(ctx)

    val (checkFallbackForGeneratedHashMap, checkFallbackForBytesToBytesMap, resetCounter,
    incCounter) = if (testFallbackStartsAt.isDefined) {
      val countTerm = ctx.addMutableState(CodeGenerator.JAVA_INT, "fallbackCounter")
      (s"$countTerm < ${testFallbackStartsAt.get._1}",
        s"$countTerm < ${testFallbackStartsAt.get._2}", s"$countTerm = 0;", s"$countTerm += 1;")
    } else {
      ("true", "true", "", "")
    }

    val findOrInsertRegularHashMap: String =
      s"""
         |// generate grouping key
         |${unsafeRowKeyCode.code}
         |${hashEval.code}
         |if ($checkFallbackForBytesToBytesMap) {
         |  // try to get the buffer from hash map
         |  $unsafeRowBuffer =
         |    $hashMapTerm.getAggregationBufferFromUnsafeRow($unsafeRowKeys, ${hashEval.value});
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
         |    $unsafeRowKeys, ${hashEval.value});
         |  if ($unsafeRowBuffer == null) {
         |    // failed to allocate the first page
         |    throw new OutOfMemoryError("No enough memory for aggregation");
         |  }
         |}
       """.stripMargin

    val findOrInsertHashMap: String = {
      if (isFastHashMapEnabled) {
        // If fast hash map is on, we first generate code to probe and update the fast hash map.
        // If the probe is successful the corresponding fast row buffer will hold the mutable row.
        s"""
           |if ($checkFallbackForGeneratedHashMap) {
           |  ${fastRowKeys.map(_.code).mkString("\n")}
           |  if (${fastRowKeys.map("!" + _.isNull).mkString(" && ")}) {
           |    $fastRowBuffer = $fastHashMapTerm.findOrInsert(
           |      ${fastRowKeys.map(_.value).mkString(", ")});
           |  }
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

    val inputAttr = aggregateBufferAttributes ++ child.output
    // Here we set `currentVars(0)` to `currentVars(numBufferSlots)` to null, so that when
    // generating code for buffer columns, we use `INPUT_ROW`(will be the buffer row), while
    // generating input columns, we use `currentVars`.
    ctx.currentVars = new Array[ExprCode](aggregateBufferAttributes.length) ++ input

    val updateRowInRegularHashMap: String = {
      ctx.INPUT_ROW = unsafeRowBuffer
      val boundUpdateExpr = updateExpr.map(BindReferences.bindReference(_, inputAttr))
      val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExpr)
      val effectiveCodes = subExprs.codes.mkString("\n")
      val unsafeRowBufferEvals = ctx.withSubExprEliminationExprs(subExprs.states) {
        boundUpdateExpr.map(_.genCode(ctx))
      }
      val updateUnsafeRowBuffer = unsafeRowBufferEvals.zipWithIndex.map { case (ev, i) =>
        val dt = updateExpr(i).dataType
        CodeGenerator.updateColumn(unsafeRowBuffer, dt, i, ev, updateExpr(i).nullable)
      }
      s"""
         |// common sub-expressions
         |$effectiveCodes
         |// evaluate aggregate function
         |${evaluateVariables(unsafeRowBufferEvals)}
         |// update unsafe row buffer
         |${updateUnsafeRowBuffer.mkString("\n").trim}
       """.stripMargin
    }

    val updateRowInHashMap: String = {
      if (isFastHashMapEnabled) {
        ctx.INPUT_ROW = fastRowBuffer
        val boundUpdateExpr = updateExpr.map(BindReferences.bindReference(_, inputAttr))
        val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExpr)
        val effectiveCodes = subExprs.codes.mkString("\n")
        val fastRowEvals = ctx.withSubExprEliminationExprs(subExprs.states) {
          boundUpdateExpr.map(_.genCode(ctx))
        }
        val updateFastRow = fastRowEvals.zipWithIndex.map { case (ev, i) =>
          val dt = updateExpr(i).dataType
          CodeGenerator.updateColumn(
            fastRowBuffer, dt, i, ev, updateExpr(i).nullable, isVectorizedHashMapEnabled)
        }

        // If fast hash map is on, we first generate code to update row in fast hash map, if the
        // previous loop up hit fast hash map. Otherwise, update row in regular hash map.
        s"""
           |if ($fastRowBuffer != null) {
           |  // common sub-expressions
           |  $effectiveCodes
           |  // evaluate aggregate function
           |  ${evaluateVariables(fastRowEvals)}
           |  // update fast row
           |  ${updateFastRow.mkString("\n").trim}
           |} else {
           |  $updateRowInRegularHashMap
           |}
       """.stripMargin
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
     $declareRowBuffer

     $findOrInsertHashMap

     $incCounter

     $updateRowInHashMap
     """
  }

  override def verboseString: String = toString(verbose = true)

  override def simpleString: String = toString(verbose = false)

  private def toString(verbose: Boolean): String = {
    val allAggregateExpressions = aggregateExpressions

    testFallbackStartsAt match {
      case None =>
        val keyString = Utils.truncatedString(groupingExpressions, "[", ", ", "]")
        val functionString = Utils.truncatedString(allAggregateExpressions, "[", ", ", "]")
        val outputString = Utils.truncatedString(output, "[", ", ", "]")
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
}

object HashAggregateExec {
  def supportsAggregate(aggregateBufferAttributes: Seq[Attribute]): Boolean = {
    val aggregationBufferSchema = StructType.fromAttributes(aggregateBufferAttributes)
    UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema(aggregationBufferSchema)
  }
}
