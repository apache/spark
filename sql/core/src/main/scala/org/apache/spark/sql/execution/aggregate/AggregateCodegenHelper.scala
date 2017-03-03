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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.execution.CodegenSupport
import org.apache.spark.sql.types.StructType

trait AggregateCodegenHelper {
  self: AggregateExec with CodegenSupport =>

  protected val groupingAttributes = groupingExpressions.map(_.toAttribute)
  protected val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
  protected val bufferSchema = StructType.fromAttributes(aggregateBufferAttributes)

  protected lazy val declFunctions =
    aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])

  protected var bufVars: Seq[ExprCode] = _

  override def usedInputs: AttributeSet = inputSet

  protected def generateBufVarsInitCode(ctx: CodegenContext): String = {
    // generate variables for aggregation buffer
    val initExpr = declFunctions.flatMap(f => f.initialValues)
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
    evaluateVariables(bufVars)
  }

  protected def generateBufVarsEvalCode(ctx: CodegenContext): String = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")

    val initBufVar = generateBufVarsInitCode(ctx)

    // generate variables for output
    val (resultVars, genResult) = if (modes.contains(Final) || modes.contains(Complete)) {
      // evaluate aggregate results
      ctx.currentVars = bufVars
      val aggResults = declFunctions.map(_.evaluateExpression).map { e =>
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

  protected def generateBufVarsUpdateCode(ctx: CodegenContext, input: Seq[ExprCode]): String = {
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

  /**
   * This is called by generated Java class, should be public.
   */
  def createUnsafeJoiner(): UnsafeRowJoiner = {
    GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)
  }

  /**
   * Generate the code for output.
   */
  protected def generateResultCode(
      ctx: CodegenContext,
      keyTerm: String,
      bufferTerm: String,
      self: String): String = {
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
        s"$joinerTerm = $self.createUnsafeJoiner();")
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
}
