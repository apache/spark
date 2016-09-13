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
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.DataType
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
  extends UnaryExecNode with CodegenSupport{
  private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
      AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
      AttributeSet(aggregateBufferAttributes)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

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

  override def usedInputs: AttributeSet = inputSet

  override def supportCodegen: Boolean = {
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

  private val modes = aggregateExpressions.map(_.mode).distinct
  // The variables used as aggregation buffer
  private var bufVars: Seq[ExprCode] = _
  private var bufVarsType: Seq[DataType] = _
  private var currentGroupingKey: String = _
  private val groupingAttributes = groupingExpressions.map(_.toAttribute)
  private var initBufVarsCodes: String = _
  private var numOutput: String = _

  private def generateInitBufVarsCodes(ctx: CodegenContext): String = {
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
    bufVarsType = initExpr.map { e =>
      e.dataType
    }
    evaluateVariables(bufVars)
  }

  private def generateCalBufVarsCodes(ctx: CodegenContext, input: Seq[ExprCode]): String = {
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
         | if (${bufVars(i).value} != ${ev.value})
         |     ${bufVars(i).value} = ${ctx.copyValue(ev.value, bufVarsType(i))};
       """.stripMargin
    }
    s"""
       | // common sub-expressions
       | $effectiveCodes
       | // evaluate aggregate function
       | ${evaluateVariables(aggVals)}
       | // update aggregation buffer
       | ${updates.mkString("\n").trim}
     """.stripMargin
  }

  private def generateResultCodes(ctx: CodegenContext): String = {
    if (modes.contains(Final) || modes.contains(Complete)) {
      // generate output using resultExpressions
      ctx.currentVars = null
      ctx.INPUT_ROW = currentGroupingKey
      val keyVars = groupingExpressions.zipWithIndex.map { case (e, i) =>
        BoundReference(i, e.dataType, e.nullable).genCode(ctx)
      }
      val evaluateKeyVars = evaluateVariables(keyVars)
      // evaluate the aggregation result
      ctx.currentVars = bufVars
      val functions = aggregateExpressions.map(
        _.aggregateFunction.asInstanceOf[DeclarativeAggregate])
      val aggResults = functions.map(_.evaluateExpression).map { e =>
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
       $evaluateAggResults
       ${consume(ctx, resultVars)}
       """
    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // This should be the last operator in a stage, we should output UnsafeRow directly
      val allAttributes = groupingAttributes ++ aggregateBufferAttributes
      ctx.currentVars = new Array[ExprCode](groupingAttributes.length) ++ bufVars
      ctx.INPUT_ROW = currentGroupingKey
      val unsafeRowProjection = GenerateUnsafeProjection.createCode(
        ctx, allAttributes.map(e => BindReferences.bindReference[Expression](e, allAttributes)))
      s"""
         |${unsafeRowProjection.code.trim}
         |${consume(ctx, null, unsafeRowProjection.value)}
       """.stripMargin
    } else {
      // generate result based on grouping key
      ctx.INPUT_ROW = currentGroupingKey
      ctx.currentVars = null
      val eval = resultExpressions.map{ e =>
        BindReferences.bindReference(e, groupingAttributes).genCode(ctx)
      }
      consume(ctx, eval)
    }
  }

  private def doProduceWithoutKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")

    // generate variables for aggregation buffer
    val initBufVarsCodes = generateInitBufVarsCodes(ctx)

    // generate variables for output
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
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

    numOutput = metricTerm(ctx, "numOutputRows")

    val doAgg = ctx.freshName("doAggregateWithoutKey")
    ctx.addNewFunction(doAgg,
      s"""
         | private void $doAgg() throws java.io.IOException {
         |   // initialize aggregation buffer
         |   $initBufVarsCodes
         |
         |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         | }
       """.stripMargin)

    s"""
       | while (!$initAgg) {
       |   $initAgg = true;
       |   $doAgg();
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
    generateCalBufVarsCodes(ctx, input)
  }

  private def doProduceWithKeys(ctx: CodegenContext): String = {
    // init buffer vars
    initBufVarsCodes = generateInitBufVarsCodes(ctx)
    // grouping key
    currentGroupingKey = ctx.freshName("currentGroupingKey")
    ctx.addMutableState("UnsafeRow", currentGroupingKey, s"$currentGroupingKey = null;")
    numOutput = metricTerm(ctx, "numOutputRows")
    s"""
       |${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
       |// for the last aggregation
       |if ($currentGroupingKey != null) {
       |    do {
       |        $numOutput.add(1);
       |        ${generateResultCodes(ctx)}
       |    } while (false);
       |    $currentGroupingKey = null;
       |}
      """.stripMargin
  }

  def doConsumeWithKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // grouping key
    ctx.INPUT_ROW = null
    ctx.currentVars = input
    val groupingExprCode: ExprCode = GenerateUnsafeProjection.createCode(
      ctx, groupingExpressions.map(e => BindReferences.bindReference[Expression](e, child.output)))
    val groupingKey = groupingExprCode.value
    // calculate buffer vars
    val calBufVarsCodes: String = generateCalBufVarsCodes(ctx, input)
   s"""
       |// generate grouping key
       |${groupingExprCode.code.trim}
       |
       |if ($currentGroupingKey == null) {
       |    $currentGroupingKey = $groupingKey.copy();
       |    // init aggregation buffer vars
       |    $initBufVarsCodes
       |    // do aggregation
       |    $calBufVarsCodes
       |    continue;
       |} else {
       |    if ($currentGroupingKey.equals($groupingKey)) {
       |        // do aggregation
       |        $calBufVarsCodes
       |        continue;
       |    } else {
       |        do {
       |            $numOutput.add(1);
       |            ${generateResultCodes(ctx)}
       |        } while (false);
       |        // new grouping starts
       |        $currentGroupingKey = $groupingKey.copy();
       |        $initBufVarsCodes
       |        $calBufVarsCodes
       |    }
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
