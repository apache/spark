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
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * Sort-based aggregate operator.
 */
case class SortAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends AggregateExec with CodegenSupport with AggregateCodegenHelper {

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
      AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
      AttributeSet(aggregateBufferAttributes)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "aggregate time"))

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) if exprs.nonEmpty => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    groupingExpressions.map(SortOrder(_, Ascending)) :: Nil
  }

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = {
    groupingExpressions.map(SortOrder(_, Ascending))
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsInternal { iter =>
      // Because the constructor of an aggregation iterator will read at least the first row,
      // we need to get the value of iter.hasNext first.
      val hasInput = iter.hasNext
      if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator[UnsafeRow]()
      } else {
        val outputIter = new SortBasedAggregationIterator(
          groupingExpressions,
          child.output,
          iter,
          aggregateExpressions,
          aggregateAttributes,
          initialInputBufferOffset,
          resultExpressions,
          (expressions, inputSchema) =>
            newMutableProjection(expressions, inputSchema, subexpressionEliminationEnabled),
          numOutputRows)
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
    val aggregationBufferSchema = StructType.fromAttributes(aggregateBufferAttributes)
    aggregationBufferSchema.forall(f => UnsafeRow.isMutable(f.dataType)) &&
      // ImperativeAggregate is not supported right now
      !aggregateExpressions.exists(_.aggregateFunction.isInstanceOf[ImperativeAggregate])
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  override protected def doProduce(ctx: CodegenContext): String = {
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

  private def doProduceWithoutKeys(ctx: CodegenContext): String = {
    generateBufVarsEvalCode(ctx)
  }

  private def doConsumeWithoutKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    generateBufVarsUpdateCode(ctx, input)
  }

  // The grouping keys of a current partition
  private var currentGroupingKeyTerm: String = _

  // The output code for a single partition
  private var outputCode: String = _

  private def generateOutputCode(ctx: CodegenContext): String = {
    ctx.currentVars = bufVars
    val bufferEv = GenerateUnsafeProjection.createCode(
      ctx, aggregateBufferAttributes.map(
        BindReferences.bindReference[Expression](_, aggregateBufferAttributes)))
    val sortAggregate = ctx.addReferenceObj("sortAggregate", this)
    s"""
       |${bufferEv.code}
       |${generateResultCode(ctx, currentGroupingKeyTerm, bufferEv.value, sortAggregate)}
     """.stripMargin
  }

  private def doProduceWithKeys(ctx: CodegenContext): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")
    s"""
      |${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
      |
      |if ($currentGroupingKeyTerm != null) {
      |  // for the last aggregation
      |  do {
      |    $numOutput.add(1);
      |    $outputCode
      |    $currentGroupingKeyTerm = null;
      |
      |    if (shouldStop()) return;
      |  } while (false);
      |}
    """.stripMargin
  }

  def doConsumeWithKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // Create the grouping keys of a current partition
    currentGroupingKeyTerm = ctx.freshName("currentGroupingKey")
    ctx.addMutableState("UnsafeRow", currentGroupingKeyTerm, s"$currentGroupingKeyTerm = null;")

    // Generate buffer-handling code
    val initBufVarsCodes = generateBufVarsInitCode(ctx)
    val updateBufVarsCode = generateBufVarsUpdateCode(ctx, input)

    // Create grouping keys for input
    ctx.currentVars = input
    val groupingEv = GenerateUnsafeProjection.createCode(
      ctx, groupingExpressions.map(BindReferences.bindReference[Expression](_, child.output)))
    val groupingKeys = groupingEv.value

    // Generate code for output
    outputCode = generateOutputCode(ctx)

    val numOutput = metricTerm(ctx, "numOutputRows")
    s"""
      |// generate grouping keys
      |${groupingEv.code.trim}
      |
      |if ($currentGroupingKeyTerm == null) {
      |  $currentGroupingKeyTerm = $groupingKeys.copy();
      |  // init aggregation buffer vars
      |  $initBufVarsCodes
      |  // do aggregation
      |  $updateBufVarsCode
      |} else {
      |  if ($currentGroupingKeyTerm.equals($groupingKeys)) {
      |    $updateBufVarsCode
      |  } else {
      |    do {
      |      $numOutput.add(1);
      |      $outputCode
      |    } while (false);
      |
      |    // init buffer vars for a next partition
      |    $currentGroupingKeyTerm = $groupingKeys.copy();
      |    $initBufVarsCodes
      |    $updateBufVarsCode
      |
      |    if (shouldStop()) return;
      |  }
      |}
    """.stripMargin
  }

  override def simpleString: String = toString(verbose = false)

  override def verboseString: String = toString(verbose = true)

  private def toString(verbose: Boolean): String = {
    val allAggregateExpressions = aggregateExpressions

    val keyString = Utils.truncatedString(groupingExpressions, "[", ", ", "]")
    val functionString = Utils.truncatedString(allAggregateExpressions, "[", ", ", "]")
    val outputString = Utils.truncatedString(output, "[", ", ", "]")
    if (verbose) {
      s"SortAggregate(key=$keyString, functions=$functionString, output=$outputString)"
    } else {
      s"SortAggregate(key=$keyString, functions=$functionString)"
    }
  }
}
