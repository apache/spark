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
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryNode, UnsafeFixedWidthAggregationMap}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType

case class TungstenAggregate(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode with CodegenSupport {

  private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  require(TungstenAggregate.supportsAggregate(aggregateBufferAttributes))

  override private[sql] lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"),
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
    AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
    AttributeSet(aggregateBufferAttributes)

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.length == 0 => AllTuples :: Nil
      case Some(exprs) if exprs.length > 0 => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  // This is for testing. We force TungstenAggregationIterator to fall back to sort-based
  // aggregation once it has processed a given number of input rows.
  private val testFallbackStartsAt: Option[Int] = {
    sqlContext.getConf("spark.sql.TungstenAggregate.testFallbackStartsAt", null) match {
      case null | "" => None
      case fallbackStartsAt => Some(fallbackStartsAt.toInt)
    }
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    val dataSize = longMetric("dataSize")
    val spillSize = longMetric("spillSize")

    child.execute().mapPartitions { iter =>

      val hasInput = iter.hasNext
      if (!hasInput && groupingExpressions.nonEmpty) {
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
            numInputRows,
            numOutputRows,
            dataSize,
            spillSize)
        if (!hasInput && groupingExpressions.isEmpty) {
          numOutputRows += 1
          Iterator.single[UnsafeRow](aggregationIterator.outputForEmptyGroupingKeyWithoutInput())
        } else {
          aggregationIterator
        }
      }
    }
  }

  override def supportCodegen: Boolean = {
    groupingExpressions.isEmpty &&
      // ImperativeAggregate is not supported right now
      !aggregateExpressions.exists(_.aggregateFunction.isInstanceOf[ImperativeAggregate]) &&
      // final aggregation only have one row, do not need to codegen
      !aggregateExpressions.exists(e => e.mode == Final || e.mode == Complete)
  }

  // The variables used as aggregation buffer
  private var bufVars: Seq[ExprCode] = _

  private val modes = aggregateExpressions.map(_.mode).distinct

  protected override def doProduce(ctx: CodegenContext): (RDD[InternalRow], String) = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")

    // generate variables for aggregation buffer
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val initExpr = functions.flatMap(f => f.initialValues)
    bufVars = initExpr.map { e =>
      val isNull = ctx.freshName("bufIsNull")
      val value = ctx.freshName("bufValue")
      // The initial expression should not access any column
      val ev = e.gen(ctx)
      val initVars = s"""
         | boolean $isNull = ${ev.isNull};
         | ${ctx.javaType(e.dataType)} $value = ${ev.value};
       """.stripMargin
      ExprCode(ev.code + initVars, isNull, value)
    }

    val (rdd, childSource) = child.asInstanceOf[CodegenSupport].produce(ctx, this)
    val source =
      s"""
         | if (!$initAgg) {
         |   $initAgg = true;
         |
         |   // initialize aggregation buffer
         |   ${bufVars.map(_.code).mkString("\n")}
         |
         |   $childSource
         |
         |   // output the result
         |   ${consume(ctx, bufVars)}
         | }
       """.stripMargin

    (rdd, source)
  }

  override def doConsume(ctx: CodegenContext, child: SparkPlan, input: Seq[ExprCode]): String = {
    // only have DeclarativeAggregate
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    // the mode could be only Partial or PartialMerge
    val updateExpr = if (modes.contains(Partial)) {
      functions.flatMap(_.updateExpressions)
    } else {
      functions.flatMap(_.mergeExpressions)
    }

    val inputAttr = functions.flatMap(_.aggBufferAttributes) ++ child.output
    val boundExpr = updateExpr.map(e => BindReferences.bindReference(e, inputAttr))
    ctx.currentVars = bufVars ++ input
    // TODO: support subexpression elimination
    val codes = boundExpr.zipWithIndex.map { case (e, i) =>
      val ev = e.gen(ctx)
      s"""
         | ${ev.code}
         | ${bufVars(i).isNull} = ${ev.isNull};
         | ${bufVars(i).value} = ${ev.value};
       """.stripMargin
    }

    s"""
       | // do aggregate and update aggregation buffer
       | ${codes.mkString("")}
     """.stripMargin
  }

  override def simpleString: String = {
    val allAggregateExpressions = aggregateExpressions

    testFallbackStartsAt match {
      case None =>
        val keyString = groupingExpressions.mkString("[", ",", "]")
        val functionString = allAggregateExpressions.mkString("[", ",", "]")
        val outputString = output.mkString("[", ",", "]")
        s"TungstenAggregate(key=$keyString, functions=$functionString, output=$outputString)"
      case Some(fallbackStartsAt) =>
        s"TungstenAggregateWithControlledFallback $groupingExpressions " +
          s"$allAggregateExpressions $resultExpressions fallbackStartsAt=$fallbackStartsAt"
    }
  }
}

object TungstenAggregate {
  def supportsAggregate(aggregateBufferAttributes: Seq[Attribute]): Boolean = {
    val aggregationBufferSchema = StructType.fromAttributes(aggregateBufferAttributes)
    UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema(aggregationBufferSchema)
  }
}
