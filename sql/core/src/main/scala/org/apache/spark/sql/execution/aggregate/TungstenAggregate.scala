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

  protected override def supportCodegen: Boolean = {
    groupingExpressions.isEmpty
  }

  private val functions = aggregateExpressions.map(_.aggregateFunction)

  private val declFunctions = functions.filter(_.isInstanceOf[DeclarativeAggregate])
    .map(_.asInstanceOf[DeclarativeAggregate])
  private var bufVars: Array[ExprCode] = _

  private val imperativeFunctions = AggregationIterator.initializeAggregateFunctions(
    aggregateExpressions.filter(_.aggregateFunction.isInstanceOf[ImperativeAggregate]),
    child.output,
    0
  ).map(_.asInstanceOf[ImperativeAggregate])
  private var aggBuffTerm: String = _
  private var imperativeFunctionsTerm: Array[String] = _


  protected override def doProduce(ctx: CodegenContext): (RDD[InternalRow], String) = {
    val initExpr = declFunctions.flatMap(f => f.initialValues)
    val resultExpr = declFunctions.map(_.evaluateExpression)

    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")

    // generate buffer variables for declarative aggregation functions
    bufVars = initExpr.map { e =>
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
    }.toArray

    val initImpBuff = if (imperativeFunctions.nonEmpty) {
      val aggBuffer = new SpecificMutableRow(
        imperativeFunctions.flatMap(_.aggBufferAttributes).map(_.dataType))
      val buffIndex = ctx.references.length
      ctx.references += aggBuffer
      aggBuffTerm = ctx.freshName("impBuff")
      ctx.addMutableState("MutableRow", aggBuffTerm,
        s"$aggBuffTerm = (MutableRow) references[$buffIndex];")

      val funcName = classOf[ImperativeAggregate].getName
      imperativeFunctionsTerm = imperativeFunctions.map { f =>
        val idx = ctx.references.length
        ctx.references += f
        val t = ctx.freshName("aggFunc")
        ctx.addMutableState(funcName, t, s"this.$t = ($funcName) references[$idx];")
        t
      }
      imperativeFunctionsTerm.map { f =>
        s"$f.initialize($aggBuffTerm);"
      }.mkString("\n")
    } else {
      ""
    }

    val modes = aggregateExpressions.map(_.mode).distinct
    val resultVar = if (modes.contains(Final) || modes.contains(Complete)) {
      val input = functions.flatMap(_.aggBufferAttributes)
      val impInput = imperativeFunctions.flatMap(_.aggBufferAttributes)
      ctx.INPUT_ROW = aggBuffTerm
      ctx.currentVars = bufVars ++ (new Array[ExprCode](impInput.length))
      resultExpr.map { e =>
        BindReferences.bindReference(e, input).gen(ctx)
      }

    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      val impInput = imperativeFunctions.flatMap(_.aggBufferAttributes)
      ctx.INPUT_ROW = aggBuffTerm
      ctx.currentVars = null
      bufVars ++ impInput.zipWithIndex.map { case (e, i) =>
        BoundReference(i, e.dataType, e.nullable).gen(ctx)
      }
    } else {
      Seq()
    }

    val (rdd, childSource) = child.asInstanceOf[CodegenSupport].produce(ctx, this)
    val source =
      s"""
         | if (!$initAgg) {
         |   $initAgg = true;
         |
         |   // initialize declarative aggregation buffer
         |   ${bufVars.map(_.code).mkString("\n")}
         |
         |   // initialize imperative aggregate buffer
         |   $initImpBuff
         |
         |   $childSource
         |
         |   // output the result
         |   ${resultVar.map(_.code).mkString("\n")}
         |   ${consume(ctx, resultVar)}
         | }
       """.stripMargin

    (rdd, source)
  }

  override def doConsume(ctx: CodegenContext, child: SparkPlan, input: Seq[ExprCode]): String = {
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val updateExpr = aggregateExpressions.flatMap {
      case AggregateExpression(f: DeclarativeAggregate, Partial | Complete, _) =>
        f.updateExpressions
      case AggregateExpression(f: DeclarativeAggregate, PartialMerge | Final, _) =>
        f.mergeExpressions
    }

    val inputAttr = functions.flatMap(_.aggBufferAttributes) ++ child.output
    val boundExpr = updateExpr.map(e => BindReferences.bindReference(e, inputAttr))

    ctx.currentVars = bufVars.toSeq ++ input
    val codes = boundExpr.zipWithIndex.map { case (e, i) =>
      val ev = e.gen(ctx)
      if (e.nullable) {
        s"""
           | ${ev.code}
           | ${bufVars(i).isNull} = ${ev.isNull};
           | if (!${ev.isNull}) {
           |   ${bufVars(i).value} = ${ev.value};
           | }
       """.stripMargin
      } else {
        s"""
           | ${ev.code}
           | ${bufVars(i).value} = ${ev.value};
         """.stripMargin
      }
    }

    val impAgg = if (imperativeFunctions.nonEmpty) {
      // TODO: only materialize the columns that is used
      val columns = child.output.zipWithIndex.map {
        case (a, i) => new BoundReference(i, a.dataType, a.nullable)
      }
      val code = GenerateUnsafeProjection.createCode(ctx, columns)
      val row = ctx.freshName("inputRow")
      val createRow = s"""
         | ${code.code}
         | InternalRow $row = ${code.value};
       """.stripMargin

      // call agg functions
      // all aggregation expression should have the same mode
      val mode = aggregateExpressions.map(_.mode).distinct.head
      val update = imperativeFunctionsTerm.map { f =>
        mode match {
          case Partial | Complete =>
            s"$f.update($aggBuffTerm, $row);"
          case PartialMerge | Final =>
            s"$f.merge($aggBuffTerm, $row);"
        }
      }.mkString("\n")

      createRow + update
    } else {
      ""
    }

    s"""
       | // declarative aggregation
       | ${codes.mkString("")}
       |
       | // imperative aggregation
       | $impAgg
       |
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
