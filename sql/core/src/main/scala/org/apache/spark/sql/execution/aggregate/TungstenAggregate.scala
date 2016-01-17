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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
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
            newMutableProjection,
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
      // final aggregation only have one row, do not need to codegen
      !aggregateExpressions.exists(e => e.mode == Final || e.mode == Complete)
  }

  // For declarative functions
  private val declFunctions = aggregateExpressions.map(_.aggregateFunction)
    .filter(_.isInstanceOf[DeclarativeAggregate])
    .map(_.asInstanceOf[DeclarativeAggregate])
  private var declBufVars: Seq[ExprCode] = _

  // For imperative functions
  private val impFunctions = AggregationIterator.initializeAggregateFunctions(
    aggregateExpressions.filter(_.aggregateFunction.isInstanceOf[ImperativeAggregate]),
    child.output,
    0
  ).map(_.asInstanceOf[ImperativeAggregate])
  private var impBuffTerm: String = _
  private var impFunctionTerms: Array[String] = _

  private val modes = aggregateExpressions.map(_.mode).distinct

  protected override def doProduce(ctx: CodegenContext): (RDD[InternalRow], String) = {
    // generate buffer variables for declarative aggregation functions
    val declInitExpr = declFunctions.flatMap(f => f.initialValues)
    declBufVars = declInitExpr.map { e =>
      val isNull = ctx.freshName("bufIsNull")
      ctx.addMutableState("boolean", isNull, "")
      val value = ctx.freshName("bufValue")
      ctx.addMutableState(ctx.javaType(e.dataType), value, "")
      // The initial expression should not access any column
      val ev = e.gen(ctx)
      val code = if (e.nullable) {
        s"""
           | $isNull = ${ev.isNull};
           | if (!${ev.isNull}) {
           |   $value = ${ev.value};
           | }
         """.stripMargin
      } else {
        s"$value = ${ev.value};"
      }
      ExprCode(ev.code + code, isNull, value)
    }

    // generate aggregation buffer for imperative functions
    val impInitCode = if (impFunctions.nonEmpty) {

      // create aggregation buffer
      val aggBuffer = new SpecificMutableRow(
        impFunctions.flatMap(_.aggBufferAttributes).map(_.dataType))
      val buffIndex = ctx.references.length
      ctx.references += aggBuffer
      impBuffTerm = ctx.freshName("impBuff")
      ctx.addMutableState("MutableRow", impBuffTerm,
        s"this.$impBuffTerm = (MutableRow) references[$buffIndex];")

      // create varialbles for imperative functions
      val funcName = classOf[ImperativeAggregate].getName
      impFunctionTerms = impFunctions.map { f =>
        val idx = ctx.references.length
        ctx.references += f
        val funcTerm = ctx.freshName("aggFunc")
        ctx.addMutableState(funcName, funcTerm, s"this.$funcTerm = ($funcName) references[$idx];")
        funcTerm
      }

      // call initialize() of imperative functions
      impFunctionTerms.map { f =>
        s"$f.initialize($impBuffTerm);"
      }.mkString("\n")
    } else {
      ""
    }

    // create variables for result (aggregation buffer)
    val modes = aggregateExpressions.map(_.mode).distinct
    // Final aggregation only output one row, do not need codegen
    assert(!modes.contains(Final) && !modes.contains(Complete))
    assert(modes.contains(Partial) || modes.contains(PartialMerge))

    // create variables for imperative functions
    // TODO: the next operator should be Exchange, we could output the aggregation buffer
    // directly without creating any variables, if there is no declarative function.
    ctx.INPUT_ROW = impBuffTerm
    ctx.currentVars = null
    val impAttrs = impFunctions.flatMap(_.aggBufferAttributes)
    val impBufVars = impAttrs.zipWithIndex.map { case (e, i) =>
      BoundReference(i, e.dataType, e.nullable).gen(ctx)
    }

    val (rdd, childSource) = child.asInstanceOf[CodegenSupport].produce(ctx, this)

    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")
    val source =
      s"""
         | if (!$initAgg) {
         |   $initAgg = true;
         |
         |   // initialize declarative aggregation buffer
         |   ${declBufVars.map(_.code).mkString("\n")}
         |
         |   // initialize imperative aggregate buffer
         |   $impInitCode
         |
         |   $childSource
         |
         |   // output the result
         |   ${impBufVars.map(_.code).mkString("\n")}
         |   ${consume(ctx, declBufVars ++ impBufVars)}
         | }
       """.stripMargin

    (rdd, source)
  }

  override def doConsume(ctx: CodegenContext, child: SparkPlan, input: Seq[ExprCode]): String = {
    // update expressions for declarative functions
    val updateExpr = if (modes.contains(Partial)) {
      declFunctions.flatMap(_.updateExpressions)
    } else {
      declFunctions.flatMap(_.mergeExpressions)
    }

    // evaluate update expression to update buffer variables
    val declInputAttr = declFunctions.flatMap(_.aggBufferAttributes) ++ child.output
    val boundExpr = updateExpr.map(e => BindReferences.bindReference(e, declInputAttr))
    ctx.currentVars = declBufVars ++ input
    val declUpdateCode = boundExpr.zipWithIndex.map { case (e, i) =>
      val ev = e.gen(ctx)
      if (e.nullable) {
        s"""
           | ${ev.code}
           | ${declBufVars(i).isNull} = ${ev.isNull};
           | if (!${ev.isNull}) {
           |   ${declBufVars(i).value} = ${ev.value};
           | }
       """.stripMargin
      } else {
        s"""
           | ${ev.code}
           | ${declBufVars(i).isNull} = false;
           | ${declBufVars(i).value} = ${ev.value};
         """.stripMargin
      }
    }

    val impUpdateCode = if (impFunctions.nonEmpty) {
      // create a UnsafeRow as input for imperative functions
      // TODO: only create the columns that are needed
      val columns = child.output.zipWithIndex.map {
        case (a, i) => new BoundReference(i, a.dataType, a.nullable)
      }
      ctx.currentVars = input
      val rowCode = GenerateUnsafeProjection.createCode(ctx, columns)

      // call agg functions
      // all aggregation expression should have the same mode
      val updates = if (modes.contains(Partial)) {
        impFunctionTerms.map { f => s"$f.update($impBuffTerm, ${rowCode.value});" }
      } else {
        impFunctionTerms.map { f => s"$f.merge($impBuffTerm, ${rowCode.value});" }
      }
      s"""
         | // create an UnsafeRow for imperative functions
         | ${rowCode.code}
         | // call update()/merge() on imperative functions
         | ${updates.mkString("\n")}
       """.stripMargin
    } else {
      ""
    }

    s"""
       | // declarative aggregation
       | ${declUpdateCode.mkString("\n")}
       |
       | // imperative aggregation
       | $impUpdateCode
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
