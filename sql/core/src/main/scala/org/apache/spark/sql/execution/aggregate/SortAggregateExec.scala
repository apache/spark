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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{CodegenSupport, OrderPreservingUnaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf

/**
 * Sort-based aggregate operator.
 */
case class SortAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    isStreaming: Boolean,
    numShufflePartitions: Option[Int],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends AggregateCodegenSupport
  with OrderPreservingUnaryExecNode {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in aggregation build"))

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    groupingExpressions.map(SortOrder(_, Ascending)) :: Nil
  }

  override protected def orderingExpressions: Seq[SortOrder] = {
    groupingExpressions.map(SortOrder(_, Ascending))
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val aggTime = longMetric("aggTime")
    child.execute().mapPartitionsWithIndexInternal { (partIndex, iter) =>
      // Because the constructor of an aggregation iterator will read at least the first row,
      // we need to get the value of iter.hasNext first.
      val hasInput = iter.hasNext
      if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator[UnsafeRow]()
      } else {
        val outputIter = new SortBasedAggregationIterator(
          partIndex,
          groupingExpressions,
          inputAttributes,
          iter,
          aggregateExpressions,
          aggregateAttributes,
          initialInputBufferOffset,
          resultExpressions,
          (expressions, inputSchema) =>
            MutableProjection.create(expressions, inputSchema),
          numOutputRows,
          aggTime)
        if (!hasInput && groupingExpressions.isEmpty) {
          // There is no input and there is no grouping expressions.
          // We need to output a single row as the output.
          numOutputRows += 1
          Iterator[UnsafeRow](outputIter.outputForEmptyGroupingKeyWithoutInput())
        } else {
          outputIter
        }
      }
    }
  }

  override def supportCodegen: Boolean = {
    super.supportCodegen && conf.getConf(SQLConf.ENABLE_SORT_AGGREGATE_CODEGEN) &&
      (groupingExpressions.isEmpty || supportCodegenWithKeys)
  }

  private def supportCodegenWithKeys: Boolean = {
    conf.getConf(SQLConf.ENABLE_SORT_AGGREGATE_CODEGEN_WITH_KEYS) &&
      groupingExpressions.forall(e => UnsafeRowUtils.isBinaryStable(e.dataType))
  }

  protected override def needHashTable: Boolean = false

  // For the with-keys path, results are produced incrementally while scanning the sorted input: a
  // group's result is emitted (appended to the output buffer) as soon as the next group starts. The
  // child's producing loop must therefore honor `shouldStop()`, so it yields right after a result
  // row is buffered and resumes scanning on the next `processNext` call (see `doProduceWithKeys`).
  // Otherwise the child would scan the whole partition in one go, and every emitted row would alias
  // the single reused output row buffer. The with-keys path is thus not fully blocking. The
  // without-keys path produces its single result only after the scan completes, so the blocking
  // default (no stop check) still applies there.
  override def needStopCheck: Boolean = groupingExpressions.nonEmpty

  override protected def canCheckLimitNotReached: Boolean = groupingExpressions.nonEmpty

  // The global UnsafeRow holding the grouping key of the group currently being aggregated.
  private var currentGroupingKeyTerm: String = _

  // The global boolean flag indicating whether the current group has been started, i.e. at least
  // one input row has been processed.
  private var initGroupTerm: String = _

  // The code that (re)initializes the aggregation buffer variables to the initial values of the
  // aggregate functions. Used to reset the buffer when a new group starts.
  private var reInitBufferCode: String = _

  // The name of the generated function that outputs the result of the current group.
  private var outputFuncName: String = _

  /**
   * Generate the code for output. The aggregation buffer is held in the global `bufVars` and the
   * grouping key in `currentGroupingKeyTerm`, both populated while scanning the current group.
   * @return function name for the result code.
   */
  private def generateResultFunctionForKeys(ctx: CodegenContext): String = {
    val funcName = ctx.freshName("doAggregateWithKeysOutput")
    val numOutput = metricTerm(ctx, "numOutputRows")
    val flatBufVars = bufVars.flatten
    val groupingAttributes = groupingExpressions.map(_.toAttribute)

    val body =
      if (modes.contains(Final) || modes.contains(Complete)) {
        // generate output using resultExpressions
        ctx.currentVars = null
        ctx.INPUT_ROW = currentGroupingKeyTerm
        val keyVars = groupingExpressions.zipWithIndex.map { case (e, i) =>
          BoundReference(i, e.dataType, e.nullable).genCode(ctx)
        }
        val evaluateKeyVars = evaluateVariables(keyVars)
        // evaluate the aggregation result from the buffer variables
        ctx.currentVars = flatBufVars
        ctx.INPUT_ROW = null
        val functions =
          aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
        val aggResults = bindReferences(
          functions.map(_.evaluateExpression),
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
        ctx.INPUT_ROW = currentGroupingKeyTerm
        val keyVars = groupingExpressions.zipWithIndex.map { case (e, i) =>
          BoundReference(i, e.dataType, e.nullable).genCode(ctx)
        }
        val evaluateKeyVars = evaluateVariables(keyVars)

        // the aggregation buffer values are output directly
        ctx.currentVars = keyVars ++ flatBufVars
        ctx.INPUT_ROW = null
        val inputAttrs = resultExpressions.map(_.toAttribute)
        val resultVars = bindReferences[Expression](
          resultExpressions,
          inputAttrs).map(_.genCode(ctx))
        s"""
           |$evaluateKeyVars
           |${consume(ctx, resultVars)}
         """.stripMargin
      } else {
        // generate result based on grouping key
        ctx.INPUT_ROW = currentGroupingKeyTerm
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
         |private void $funcName() throws java.io.IOException {
         |  $numOutput.add(1);
         |  $body
         |}
       """.stripMargin)
  }

  protected override def doProduceWithKeys(ctx: CodegenContext): String = {
    ctx.INPUT_ROW = null
    // Generate the global variables for the aggregation buffer of the current group. Capture the
    // buffer initialization code (and clear it from `bufVars`, so the result/update code does not
    // re-emit it); it is reused in `doConsumeWithKeys` to reset the buffer when a new group starts.
    reInitBufferCode = evaluateVariables(createAggBufVars(ctx).flatten)
    // Global state to track the current group. Inline the grouping-key row (rather than letting it
    // be compacted into a shared array) so its term is a plain variable name: it is used as
    // `ctx.INPUT_ROW` when generating the output, and an array-subscript term there would break the
    // generated code when key expressions are extracted into split functions.
    currentGroupingKeyTerm =
      ctx.addMutableState("UnsafeRow", "currentGroupingKey", forceInline = true)
    initGroupTerm = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "initGroup")
    // Whether the whole sorted input has been consumed.
    val noMoreInputTerm = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "noMoreInputTerm")
    // Generate the output function before `child.produce`, so that `doConsumeWithKeys` can call it
    // when it detects a group boundary.
    outputFuncName = generateResultFunctionForKeys(ctx)

    val doAgg = ctx.freshName("doAggregateWithKeys")
    // Pass `partitionIndex` as a parameter so bare references in the child's
    // produce resolve to the local, not the protected superclass field.
    // Required when `addNewFunction` spills this helper to a nested class.
    val doAggFuncName = ctx.addNewFunction(doAgg,
      s"""
         |private void $doAgg(int partitionIndex) throws java.io.IOException {
         |  ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         |}
       """.stripMargin)

    // Sort-based aggregation consumes the sorted input row by row, emitting a group's result as
    // soon as the next group starts (see `doConsumeWithKeys`). Emitting a row appends to the output
    // buffer, which makes the child's `shouldStop()` return true, so the child's producing loop
    // returns to here mid-scan. We therefore must be able to resume scanning across multiple
    // `processNext` invocations:
    //  - `$noMoreInputTerm` guards against re-running once the input is fully consumed;
    //  - after `$doAggFuncName()` returns, `shouldStop()` distinguishes a real end-of-input (the
    //    child's loop exhausted, so `shouldStop()` is false) from a mid-scan pause (`shouldStop()`
    //    is true because an output row is buffered). Only on a real end-of-input do we mark the
    //    scan done and flush the last group. If the last input row also triggered an output (so
    //    `shouldStop()` is still true at exhaustion), the next `processNext` re-enters, the child's
    //    loop produces nothing, `shouldStop()` is then false, and the last group is flushed.
    s"""
       |if (!$noMoreInputTerm) {
       |  $doAggFuncName(partitionIndex);
       |  if (!shouldStop()) {
       |    $noMoreInputTerm = true;
       |    if ($initGroupTerm) {
       |      $outputFuncName();
       |    }
       |  }
       |}
     """.stripMargin
  }

  protected override def doConsumeWithKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // Create the grouping key. `ctx.currentVars` is still set to `input` here.
    val groupingUnsafeRowKeyCode = GenerateUnsafeProjection.createCode(
      ctx, bindReferences[Expression](groupingExpressions, child.output))
    val groupingUnsafeRowKey = groupingUnsafeRowKeyCode.value
    // The code to update the aggregation buffer with the current input row.
    val updateBufferCode = generateAggBufferUpdateCode(ctx, input)

    // `reInitBufferCode` was captured in `doProduceWithKeys`; it (re)initializes the aggregation
    // buffer variables to the aggregate functions' initial values, resetting them for a new group.
    // The input is sorted by the grouping key, so rows of the same group are contiguous. When the
    // grouping key changes, the current group is complete: output it and reset the buffer for the
    // new group. Group equality uses the binary representation of the key, which is valid because
    // `supportCodegen` restricts grouping keys to binary-stable types.
    s"""
       |${groupingUnsafeRowKeyCode.code}
       |if (!$initGroupTerm) {
       |  $initGroupTerm = true;
       |  $currentGroupingKeyTerm = $groupingUnsafeRowKey.copy();
       |  $reInitBufferCode
       |} else if (!$currentGroupingKeyTerm.equals($groupingUnsafeRowKey)) {
       |  $outputFuncName();
       |  $currentGroupingKeyTerm = $groupingUnsafeRowKey.copy();
       |  $reInitBufferCode
       |}
       |$updateBufferCode
     """.stripMargin
  }

  override def simpleString(maxFields: Int): String = toString(verbose = false, maxFields)

  override def verboseString(maxFields: Int): String = toString(verbose = true, maxFields)

  private def toString(verbose: Boolean, maxFields: Int): String = {
    val allAggregateExpressions = aggregateExpressions

    val keyString = truncatedString(groupingExpressions, "[", ", ", "]", maxFields)
    val functionString = truncatedString(allAggregateExpressions, "[", ", ", "]", maxFields)
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    if (verbose) {
      s"SortAggregate(key=$keyString, functions=$functionString, output=$outputString)"
    } else {
      s"SortAggregate(key=$keyString, functions=$functionString)"
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SortAggregateExec =
    copy(child = newChild)
}
