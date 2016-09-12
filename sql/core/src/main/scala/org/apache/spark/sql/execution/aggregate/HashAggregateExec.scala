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

import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}
import org.apache.spark.unsafe.KVIterator
import org.apache.spark.util.Utils

/**
 * Hash-based aggregate operator that can also fallback to sorting when data exceeds memory size.
 */
case class HashAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    isGrouped: Boolean,
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {
  assert(isGrouped || groupingExpressions.isEmpty)

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
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "aggregate time"))

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

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

    child.execute().mapPartitions { iter =>

      val hasInput = iter.hasNext
      if (!hasInput && isGrouped) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator.empty
      } else {
        val aggregationIterator =
          new TungstenAggregationIterator(
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
            spillSize)
        if (!hasInput && !isGrouped) {
          numOutputRows += 1
          Iterator.single[UnsafeRow](aggregationIterator.outputForEmptyGroupingKeyWithoutInput())
        } else {
          aggregationIterator
        }
      }
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

  protected override def doProduce(ctx: CodegenContext): String = {
    if (!isGrouped) {
      doProduceWithoutKeys(ctx)
    } else {
      doProduceWithKeys(ctx)
    }
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    if (!isGrouped) {
      doConsumeWithoutKeys(ctx, input)
    } else {
      doConsumeWithKeys(ctx, input)
    }
  }

  // The variables used as aggregation buffer
  private var bufVars: Seq[ExprCode] = _

  private def doProduceWithoutKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")

    // generate variables for aggregation buffer
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val initExpr = functions.flatMap(f => f.initialValues)
    bufVars = initExpr.map { e =>
      val isNull = ctx.freshName("bufIsNull")
      val value = ctx.freshName("bufValue")
      ctx.addMutableState("boolean", isNull, "")
      ctx.addMutableState(ctx.javaType(e.dataType), value, "")
      // The initial expression should not access any column
      val ev = e.genCode(ctx)
      val initVars = s"""
         | $isNull = ${ev.isNull};
         | $value = ${ev.value};
       """.stripMargin
      ExprCode(ev.code + initVars, isNull, value)
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
    ctx.addNewFunction(doAgg,
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
       |   $doAgg();
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
      TaskContext.get().taskMemoryManager(),
      1024 * 16, // initial capacity
      TaskContext.get().taskMemoryManager().pageSizeBytes,
      false // disable tracking of performance metrics
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
      spillSize: SQLMetric): KVIterator[UnsafeRow, UnsafeRow] = {

    // update peak execution memory
    val mapMemory = hashMap.getPeakMemoryUsedBytes
    val sorterMemory = Option(sorter).map(_.getPeakMemoryUsedBytes).getOrElse(0L)
    val maxMemory = Math.max(mapMemory, sorterMemory)
    val metrics = TaskContext.get().taskMetrics()
    peakMemory.add(maxMemory)
    metrics.incPeakExecutionMemory(maxMemory)

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
   */
  private def generateResultCode(
      ctx: CodegenContext,
      keyTerm: String,
      bufferTerm: String,
      plan: String): String = {
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
      s"""
       $evaluateKeyVars
       $evaluateBufferVars
       $evaluateAggResults
       ${consume(ctx, resultVars)}
       """

    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // This should be the last operator in a stage, we should output UnsafeRow directly
      val joinerTerm = ctx.freshName("unsafeRowJoiner")
      ctx.addMutableState(classOf[UnsafeRowJoiner].getName, joinerTerm,
        s"$joinerTerm = $plan.createUnsafeJoiner();")
      val resultRow = ctx.freshName("resultRow")
      s"""
       UnsafeRow $resultRow = $joinerTerm.join($keyTerm, $bufferTerm);
       ${consume(ctx, null, resultRow)}
       """

    } else {
      // generate result based on grouping key
      ctx.INPUT_ROW = keyTerm
      ctx.currentVars = null
      val eval = resultExpressions.map{ e =>
        BindReferences.bindReference(e, groupingAttributes).genCode(ctx)
      }
      consume(ctx, eval)
    }
  }

  /**
   * A required check for any fast hash map implementation (basically the common requirements
   * for row-based and vectorized).
   * Currently fast hash map is supported for primitive data types during partial aggregation.
   * This list of supported use-cases should be expanded over time.
   */
  private def checkIfFastHashMapSupported(ctx: CodegenContext): Boolean = {
    val isSupported =
      (groupingKeySchema ++ bufferSchema).forall(f => ctx.isPrimitiveType(f.dataType) ||
        f.dataType.isInstanceOf[DecimalType] || f.dataType.isInstanceOf[StringType]) &&
        bufferSchema.nonEmpty && modes.forall(mode => mode == Partial || mode == PartialMerge) &&
        groupingExpressions.nonEmpty

    // For vectorized hash map, We do not support byte array based decimal type for aggregate values
    // as ColumnVector.putDecimal for high-precision decimals doesn't currently support in-place
    // updates. Due to this, appending the byte array in the vectorized hash map can turn out to be
    // quite inefficient and can potentially OOM the executor.
    // For row-based hash map, while decimal update is supported in UnsafeRow, we will just act
    // conservative here, due to lack of testing and benchmarking.
    val isNotByteArrayDecimalType = bufferSchema.map(_.dataType).filter(_.isInstanceOf[DecimalType])
      .forall(!DecimalType.isByteArrayDecimalType(_))

    isSupported && isNotByteArrayDecimalType
  }

  private def enableTwoLevelHashMap(ctx: CodegenContext) = {
    if (!checkIfFastHashMapSupported(ctx)) {
      if (modes.forall(mode => mode == Partial || mode == PartialMerge) && !Utils.isTesting) {
        logInfo("spark.sql.codegen.aggregate.map.twolevel.enable is set to true, but"
          + " current version of codegened fast hashmap does not support this aggregate.")
      }
    } else {
      isFastHashMapEnabled = true

      // This is for testing/benchmarking only.
      // We enforce to first level to be a vectorized hashmap, instead of the default row-based one.
      sqlContext.getConf("spark.sql.codegen.aggregate.map.vectorized.enable", null) match {
        case "true" => isVectorizedHashMapEnabled = true
        case null | "" | "false" => None      }
    }
  }

  private def doProduceWithKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")
    if (sqlContext.conf.enableTwoLevelAggMap) {
      enableTwoLevelHashMap(ctx)
    } else {
      sqlContext.getConf("spark.sql.codegen.aggregate.map.vectorized.enable", null) match {
        case "true" => logWarning("Two level hashmap is disabled but vectorized hashmap is " +
          "enabled.")
        case null | "" | "false" => None
      }
    }
    fastHashMapTerm = ctx.freshName("fastHashMap")
    val fastHashMapClassName = ctx.freshName("FastHashMap")
    val fastHashMapGenerator =
      if (isVectorizedHashMapEnabled) {
        new VectorizedHashMapGenerator(ctx, aggregateExpressions,
          fastHashMapClassName, groupingKeySchema, bufferSchema)
      } else {
        new RowBasedHashMapGenerator(ctx, aggregateExpressions,
          fastHashMapClassName, groupingKeySchema, bufferSchema)
      }

    val thisPlan = ctx.addReferenceObj("plan", this)

    // Create a name for iterator from vectorized HashMap
    val iterTermForFastHashMap = ctx.freshName("fastHashMapIter")
    if (isFastHashMapEnabled) {
      if (isVectorizedHashMapEnabled) {
        ctx.addMutableState(fastHashMapClassName, fastHashMapTerm,
          s"$fastHashMapTerm = new $fastHashMapClassName();")
        ctx.addMutableState(
          "java.util.Iterator<org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row>",
          iterTermForFastHashMap, "")
      } else {
        ctx.addMutableState(fastHashMapClassName, fastHashMapTerm,
          s"$fastHashMapTerm = new $fastHashMapClassName(" +
            s"agg_plan.getTaskMemoryManager(), agg_plan.getEmptyAggregationBuffer());")
        ctx.addMutableState(
          "org.apache.spark.unsafe.KVIterator",
          iterTermForFastHashMap, "")
      }
    }

    // create hashMap
    hashMapTerm = ctx.freshName("hashMap")
    val hashMapClassName = classOf[UnsafeFixedWidthAggregationMap].getName
    ctx.addMutableState(hashMapClassName, hashMapTerm, "")
    sorterTerm = ctx.freshName("sorter")
    ctx.addMutableState(classOf[UnsafeKVExternalSorter].getName, sorterTerm, "")

    // Create a name for iterator from HashMap
    val iterTerm = ctx.freshName("mapIter")
    ctx.addMutableState(classOf[KVIterator[UnsafeRow, UnsafeRow]].getName, iterTerm, "")

    val doAgg = ctx.freshName("doAggregateWithKeys")
    val peakMemory = metricTerm(ctx, "peakMemory")
    val spillSize = metricTerm(ctx, "spillSize")

    def generateGenerateCode(): String = {
      if (isFastHashMapEnabled) {
        if (isVectorizedHashMapEnabled) {
          s"""
               | ${fastHashMapGenerator.asInstanceOf[VectorizedHashMapGenerator].generate()}
          """.stripMargin
        } else {
          s"""
               | ${fastHashMapGenerator.asInstanceOf[RowBasedHashMapGenerator].generate()}
          """.stripMargin
        }
      } else ""
    }

    ctx.addNewFunction(doAgg,
      s"""
        ${generateGenerateCode}
        private void $doAgg() throws java.io.IOException {
          $hashMapTerm = $thisPlan.createHashMap();
          ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}

          ${if (isFastHashMapEnabled) {
              s"$iterTermForFastHashMap = $fastHashMapTerm.rowIterator();"} else ""}

          $iterTerm = $thisPlan.finishAggregate($hashMapTerm, $sorterTerm, $peakMemory, $spillSize);
        }
       """)

    // generate code for output
    val keyTerm = ctx.freshName("aggKey")
    val bufferTerm = ctx.freshName("aggBuffer")
    val outputCode = generateResultCode(ctx, keyTerm, bufferTerm, thisPlan)
    val numOutput = metricTerm(ctx, "numOutputRows")

    // The child could change `copyResult` to true, but we had already consumed all the rows,
    // so `copyResult` should be reset to `false`.
    ctx.copyResult = false

    def outputFromGeneratedMap: String = {
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
       while ($iterTermForFastHashMap.next()) {
         $numOutput.add(1);
         UnsafeRow $keyTerm = (UnsafeRow) $iterTermForFastHashMap.getKey();
         UnsafeRow $bufferTerm = (UnsafeRow) $iterTermForFastHashMap.getValue();
         $outputCode

         if (shouldStop()) return;
       }
       $fastHashMapTerm.close();
     """
    }

    // Iterate over the aggregate rows and convert them from ColumnarBatch.Row to UnsafeRow
    def outputFromVectorizedMap: String = {
        val row = ctx.freshName("fastHashMapRow")
        ctx.currentVars = null
        ctx.INPUT_ROW = row
        var schema: StructType = groupingKeySchema
        bufferSchema.foreach(i => schema = schema.add(i))
        val generateRow = GenerateUnsafeProjection.createCode(ctx, schema.toAttributes.zipWithIndex
          .map { case (attr, i) => BoundReference(i, attr.dataType, attr.nullable) })
        s"""
           | while ($iterTermForFastHashMap.hasNext()) {
           |   $numOutput.add(1);
           |   org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row $row =
           |     (org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row)
           |     $iterTermForFastHashMap.next();
           |   ${generateRow.code}
           |   ${consume(ctx, Seq.empty, {generateRow.value})}
           |
           |   if (shouldStop()) return;
           | }
           |
           | $fastHashMapTerm.close();
         """.stripMargin
    }


    val aggTime = metricTerm(ctx, "aggTime")
    val beforeAgg = ctx.freshName("beforeAgg")
    s"""
     if (!$initAgg) {
       $initAgg = true;
       long $beforeAgg = System.nanoTime();
       $doAgg();
       $aggTime.add((System.nanoTime() - $beforeAgg) / 1000000);
     }

     // output the result
     ${outputFromGeneratedMap}

     while ($iterTerm.next()) {
       $numOutput.add(1);
       UnsafeRow $keyTerm = (UnsafeRow) $iterTerm.getKey();
       UnsafeRow $bufferTerm = (UnsafeRow) $iterTerm.getValue();
       $outputCode

       if (shouldStop()) return;
     }

     $iterTerm.close();
     if ($sorterTerm == null) {
       $hashMapTerm.free();
     }
     """
  }

  private def doConsumeWithKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {

    // create grouping key
    ctx.currentVars = input
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
    val hashExpr = Murmur3Hash(groupingExpressions, 42)
    ctx.currentVars = input
    val hashEval = BindReferences.bindReference(hashExpr, child.output).genCode(ctx)

    val inputAttr = aggregateBufferAttributes ++ child.output
    ctx.currentVars = new Array[ExprCode](aggregateBufferAttributes.length) ++ input

    val (checkFallbackForGeneratedHashMap, checkFallbackForBytesToBytesMap, resetCounter,
    incCounter) = if (testFallbackStartsAt.isDefined) {
      val countTerm = ctx.freshName("fallbackCounter")
      ctx.addMutableState("int", countTerm, s"$countTerm = 0;")
      (s"$countTerm < ${testFallbackStartsAt.get._1}",
        s"$countTerm < ${testFallbackStartsAt.get._2}", s"$countTerm = 0;", s"$countTerm += 1;")
    } else {
      ("true", "true", "", "")
    }

    // We first generate code to probe and update the fast hash map. If the probe is
    // successful the corresponding fast row buffer will hold the mutable row
    val findOrInsertFastHashMap: Option[String] = {
      if (isFastHashMapEnabled) {
        Option(
          s"""
             |
             |if ($checkFallbackForGeneratedHashMap) {
             |  ${fastRowKeys.map(_.code).mkString("\n")}
             |  if (${fastRowKeys.map("!" + _.isNull).mkString(" && ")}) {
             |    $fastRowBuffer = $fastHashMapTerm.findOrInsert(
             |        ${fastRowKeys.map(_.value).mkString(", ")});
             |  }
             |}
         """.stripMargin)
      } else {
        None
      }
    }


    def updateRowInFastHashMap(isVectorized: Boolean): Option[String] = {
      ctx.INPUT_ROW = fastRowBuffer
      val boundUpdateExpr = updateExpr.map(BindReferences.bindReference(_, inputAttr))
      val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExpr)
      val effectiveCodes = subExprs.codes.mkString("\n")
      val fastRowEvals = ctx.withSubExprEliminationExprs(subExprs.states) {
        boundUpdateExpr.map(_.genCode(ctx))
      }
      val updateFastRow = fastRowEvals.zipWithIndex.map { case (ev, i) =>
        val dt = updateExpr(i).dataType
        ctx.updateColumn(fastRowBuffer, dt, i, ev, updateExpr(i).nullable, isVectorized)
      }
      Option(
        s"""
           |// common sub-expressions
           |$effectiveCodes
           |// evaluate aggregate function
           |${evaluateVariables(fastRowEvals)}
           |// update fast row
           |${updateFastRow.mkString("\n").trim}
           |
         """.stripMargin)
    }

    // Next, we generate code to probe and update the unsafe row hash map.
    val findOrInsertInUnsafeRowMap: String = {
      s"""
         | if ($fastRowBuffer == null) {
         |   // generate grouping key
         |   ${unsafeRowKeyCode.code.trim}
         |   ${hashEval.code.trim}
         |   if ($checkFallbackForBytesToBytesMap) {
         |     // try to get the buffer from hash map
         |     $unsafeRowBuffer =
         |       $hashMapTerm.getAggregationBufferFromUnsafeRow($unsafeRowKeys, ${hashEval.value});
         |   }
         |   if ($unsafeRowBuffer == null) {
         |     if ($sorterTerm == null) {
         |       $sorterTerm = $hashMapTerm.destructAndCreateExternalSorter();
         |     } else {
         |       $sorterTerm.merge($hashMapTerm.destructAndCreateExternalSorter());
         |     }
         |     $resetCounter
         |     // the hash map had be spilled, it should have enough memory now,
         |     // try  to allocate buffer again.
         |     $unsafeRowBuffer =
         |       $hashMapTerm.getAggregationBufferFromUnsafeRow($unsafeRowKeys, ${hashEval.value});
         |     if ($unsafeRowBuffer == null) {
         |       // failed to allocate the first page
         |       throw new OutOfMemoryError("No enough memory for aggregation");
         |     }
         |   }
         | }
       """.stripMargin
    }

    val updateRowInUnsafeRowMap: String = {
      ctx.INPUT_ROW = unsafeRowBuffer
      val boundUpdateExpr = updateExpr.map(BindReferences.bindReference(_, inputAttr))
      val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExpr)
      val effectiveCodes = subExprs.codes.mkString("\n")
      val unsafeRowBufferEvals = ctx.withSubExprEliminationExprs(subExprs.states) {
        boundUpdateExpr.map(_.genCode(ctx))
      }
      val updateUnsafeRowBuffer = unsafeRowBufferEvals.zipWithIndex.map { case (ev, i) =>
        val dt = updateExpr(i).dataType
        ctx.updateColumn(unsafeRowBuffer, dt, i, ev, updateExpr(i).nullable)
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


    // We try to do hash map based in-memory aggregation first. If there is not enough memory (the
    // hash map will return null for new key), we spill the hash map to disk to free memory, then
    // continue to do in-memory aggregation and spilling until all the rows had been processed.
    // Finally, sort the spilled aggregate buffers by key, and merge them together for same key.
    s"""
     UnsafeRow $unsafeRowBuffer = null;
     ${
        if (isVectorizedHashMapEnabled) {
          s"""
             | org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row $fastRowBuffer = null;
           """.stripMargin
        } else {
          s"""
             | UnsafeRow $fastRowBuffer = null;
           """.stripMargin
        }
      }

     ${findOrInsertFastHashMap.getOrElse("")}

     $findOrInsertInUnsafeRowMap

     $incCounter

     if ($fastRowBuffer != null) {
       // update fast row
       ${
          if (isFastHashMapEnabled) {
            updateRowInFastHashMap(isVectorizedHashMapEnabled).getOrElse("")
          } else ""
        }
     } else {
       // update unsafe row
       $updateRowInUnsafeRowMap
     }
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
