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

import org.apache.spark.SparkException
import org.apache.spark.internal.LogKeys.MAX_JVM_METHOD_PARAMS_LENGTH
import org.apache.spark.internal.MDC
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, ExpressionEquals, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MILLIS
import org.apache.spark.sql.execution.{BlockingOperatorWithCodegen, CodegenSupport, GeneratePredicateHelper}
import org.apache.spark.util.Utils

/**
 * An interface for those aggregate physical operators that support codegen.
 */
trait AggregateCodegenSupport
  extends BaseAggregateExec
  with BlockingOperatorWithCodegen
  with GeneratePredicateHelper {

  /**
   * All the modes of aggregate expressions.
   */
  protected val modes: Seq[AggregateMode] = aggregateExpressions.map(_.mode).distinct

  /**
   * The variables are used as aggregation buffers and each aggregate function has one or more
   * ExprCode to initialize its buffer slots. Only used for aggregation without keys.
   */
  private var bufVars: Seq[Seq[ExprCode]] = _

  /**
   * Whether this operator needs to build hash table.
   */
  protected def needHashTable: Boolean

  /**
   * The generated code for `doProduce` call when aggregate has grouping keys.
   */
  protected def doProduceWithKeys(ctx: CodegenContext): String

  /**
   * The generated code for `doConsume` call when aggregate has grouping keys.
   */
  protected def doConsumeWithKeys(ctx: CodegenContext, input: Seq[ExprCode]): String

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

  override def supportCodegen: Boolean = {
    val isMutableAggBuffer = aggregateBufferAttributes.forall(a => UnsafeRow.isMutable(a.dataType))
    // ImperativeAggregate are not supported right now
    isMutableAggBuffer &&
      !aggregateExpressions.exists(_.aggregateFunction.isInstanceOf[ImperativeAggregate])
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  override def usedInputs: AttributeSet = inputSet

  /**
   * The generated code for `doProduce` call when aggregate does not have grouping keys.
   */
  private def doProduceWithoutKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "initAgg")
    // The generated function doesn't have input row in the code context.
    ctx.INPUT_ROW = null

    // generate variables for aggregation buffer
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val initExpr = functions.map(f => f.initialValues)
    bufVars = initExpr.map { exprs =>
      exprs.map { e =>
        val isNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "bufIsNull")
        val value = ctx.addMutableState(CodeGenerator.javaType(e.dataType), "bufValue")
        // The initial expression should not access any column
        val ev = e.genCode(ctx)
        val initVars =
          code"""
                |$isNull = ${ev.isNull};
                |$value = ${ev.value};
              """.stripMargin
        ExprCode(
          ev.code + initVars,
          JavaCode.isNullGlobal(isNull),
          JavaCode.global(value, e.dataType))
      }
    }
    val flatBufVars = bufVars.flatten
    val initBufVar = evaluateVariables(flatBufVars)

    // generate variables for output
    val (resultVars, genResult) = if (modes.contains(Final) || modes.contains(Complete)) {
      // evaluate aggregate results
      ctx.currentVars = flatBufVars
      val aggResults = bindReferences(
        functions.map(_.evaluateExpression),
        aggregateBufferAttributes).map(_.genCode(ctx))
      val evaluateAggResults = evaluateVariables(aggResults)
      // evaluate result expressions
      ctx.currentVars = aggResults
      val resultVars = bindReferences(resultExpressions, aggregateAttributes).map(_.genCode(ctx))
      (resultVars,
        s"""
           |$evaluateAggResults
           |${evaluateVariables(resultVars)}
         """.stripMargin)
    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // output the aggregate buffer directly
      (flatBufVars, "")
    } else {
      // no aggregate function, the result should be literals
      val resultVars = resultExpressions.map(_.genCode(ctx))
      (resultVars, evaluateVariables(resultVars))
    }

    val doAgg = ctx.freshName("doAggregateWithoutKey")
    val doAggFuncName = ctx.addNewFunction(doAgg,
      s"""
         |private void $doAgg() throws java.io.IOException {
         |  // initialize aggregation buffer
         |  $initBufVar
         |
         |  ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         |}
       """.stripMargin)

    val numOutput = metricTerm(ctx, "numOutputRows")
    val doAggWithRecordMetric =
      if (needHashTable) {
        val aggTime = metricTerm(ctx, "aggTime")
        val beforeAgg = ctx.freshName("beforeAgg")
        s"""
           |long $beforeAgg = System.nanoTime();
           |$doAggFuncName();
           |$aggTime.add((System.nanoTime() - $beforeAgg) / $NANOS_PER_MILLIS);
         """.stripMargin
      } else {
        s"$doAggFuncName();"
      }

    s"""
       |while (!$initAgg) {
       |  $initAgg = true;
       |  $doAggWithRecordMetric
       |
       |  // output the result
       |  ${genResult.trim}
       |
       |  $numOutput.add(1);
       |  ${consume(ctx, resultVars).trim}
       |}
     """.stripMargin
  }

  /**
   * The generated code for `doConsume` call when aggregate does not have grouping keys.
   */
  private def doConsumeWithoutKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // only have DeclarativeAggregate
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val inputAttrs = functions.flatMap(_.aggBufferAttributes) ++ inputAttributes
    // To individually generate code for each aggregate function, an element in `updateExprs` holds
    // all the expressions for the buffer of an aggregation function.
    val updateExprs = aggregateExpressions.map { e =>
      e.mode match {
        case Partial | Complete =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].updateExpressions
        case PartialMerge | Final =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].mergeExpressions
      }
    }
    ctx.currentVars = bufVars.flatten ++ input
    val boundUpdateExprs = updateExprs.map { updateExprsForOneFunc =>
      bindReferences(updateExprsForOneFunc, inputAttrs)
    }
    val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExprs.flatten)
    val effectiveCodes = ctx.evaluateSubExprEliminationState(subExprs.states.values)
    val bufferEvals = boundUpdateExprs.map { boundUpdateExprsForOneFunc =>
      ctx.withSubExprEliminationExprs(subExprs.states) {
        boundUpdateExprsForOneFunc.map(_.genCode(ctx))
      }
    }

    val aggNames = functions.map(_.prettyName)
    val aggCodeBlocks = bufferEvals.zipWithIndex.map { case (bufferEvalsForOneFunc, i) =>
      val bufVarsForOneFunc = bufVars(i)
      // All the update code for aggregation buffers should be placed in the end
      // of each aggregation function code.
      val updates = bufferEvalsForOneFunc.zip(bufVarsForOneFunc).map { case (ev, bufVar) =>
        s"""
           |${bufVar.isNull} = ${ev.isNull};
           |${bufVar.value} = ${ev.value};
         """.stripMargin
      }
      code"""
            |${ctx.registerComment(s"do aggregate for ${aggNames(i)}")}
            |${ctx.registerComment("evaluate aggregate function")}
            |${evaluateVariables(bufferEvalsForOneFunc)}
            |${ctx.registerComment("update aggregation buffers")}
            |${updates.mkString("\n").trim}
       """.stripMargin
    }

    val codeToEvalAggFuncs = generateEvalCodeForAggFuncs(
      ctx, input, inputAttrs, boundUpdateExprs, aggNames, aggCodeBlocks, subExprs)
    s"""
       |// do aggregate
       |// common sub-expressions
       |$effectiveCodes
       |// evaluate aggregate functions and update aggregation buffers
       |$codeToEvalAggFuncs
     """.stripMargin
  }

  /**
   * The generated code to evaluate aggregate functions.
   */
  protected def generateEvalCodeForAggFuncs(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      inputAttrs: Seq[Attribute],
      boundUpdateExprs: Seq[Seq[Expression]],
      aggNames: Seq[String],
      aggCodeBlocks: Seq[Block],
      subExprs: SubExprCodes): String = {
    val aggCodes = if (conf.codegenSplitAggregateFunc &&
      aggCodeBlocks.map(_.length).sum > conf.methodSplitThreshold) {
      val maybeSplitCodes = splitAggregateExpressions(
        ctx, aggNames, boundUpdateExprs, aggCodeBlocks, subExprs.states)

      maybeSplitCodes.getOrElse(aggCodeBlocks.map(_.code))
    } else {
      aggCodeBlocks.map(_.code)
    }

    aggCodes.zip(aggregateExpressions.map(ae => (ae.mode, ae.filter))).map {
      case (aggCode, (Partial | Complete, Some(condition))) =>
        // Note: wrap in "do { } while(false);", so the generated checks can jump out
        // with "continue;"
        s"""
           |do {
           |  ${generatePredicateCode(ctx, condition, inputAttrs, input)}
           |  $aggCode
           |} while(false);
         """.stripMargin
      case (aggCode, _) =>
        aggCode
    }.mkString("\n")
  }

  /**
   * Splits aggregate code into small functions because the most of JVM implementations
   * can not compile too long functions. Returns None if we are not able to split the given code.
   *
   * Note: The difference from `CodeGenerator.splitExpressions` is that we define an individual
   * function for each aggregation function (e.g., SUM and AVG). For example, in a query
   * `SELECT SUM(a), AVG(a) FROM VALUES(1) t(a)`, we define two functions
   * for `SUM(a)` and `AVG(a)`.
   */
  private def splitAggregateExpressions(
      ctx: CodegenContext,
      aggNames: Seq[String],
      aggBufferUpdatingExprs: Seq[Seq[Expression]],
      aggCodeBlocks: Seq[Block],
      subExprs: Map[ExpressionEquals, SubExprEliminationState]): Option[Seq[String]] = {
    val exprValsInSubExprs = subExprs.flatMap { case (_, s) =>
      s.eval.value :: s.eval.isNull :: Nil
    }
    if (exprValsInSubExprs.exists(_.isInstanceOf[SimpleExprValue])) {
      // `SimpleExprValue`s cannot be used as an input variable for split functions, so
      // we give up splitting functions if it exists in `subExprs`.
      None
    } else {
      val inputVars = aggBufferUpdatingExprs.map { aggExprsForOneFunc =>
        val inputVarsForOneFunc = aggExprsForOneFunc.map(
          CodeGenerator.getLocalInputVariableValues(ctx, _, subExprs)._1).reduce(_ ++ _).toSeq
        val paramLength = CodeGenerator.calculateParamLengthFromExprValues(inputVarsForOneFunc)

        // Checks if a parameter length for the `aggExprsForOneFunc` does not go over the JVM limit
        if (CodeGenerator.isValidParamLength(paramLength)) {
          Some(inputVarsForOneFunc)
        } else {
          None
        }
      }

      // Checks if all the aggregate code can be split into pieces.
      // If the parameter length of at lease one `aggExprsForOneFunc` goes over the limit,
      // we totally give up splitting aggregate code.
      if (inputVars.forall(_.isDefined)) {
        val splitCodes = inputVars.flatten.zipWithIndex.map { case (args, i) =>
          val doAggFunc = ctx.freshName(s"doAggregate_${aggNames(i)}")
          val argList = args.map { v =>
            s"${CodeGenerator.typeName(v.javaType)} ${v.variableName}"
          }.mkString(", ")
          val doAggFuncName = ctx.addNewFunction(doAggFunc,
            s"""
               |private void $doAggFunc($argList) throws java.io.IOException {
               |  ${aggCodeBlocks(i)}
               |}
             """.stripMargin)

          val inputVariables = args.map(_.variableName).mkString(", ")
          s"$doAggFuncName($inputVariables);"
        }
        Some(splitCodes)
      } else {
        val errMsg = log"Failed to split aggregate code into small functions because the " +
          log"parameter length of at least one split function went over the JVM limit: " +
          log"${MDC(MAX_JVM_METHOD_PARAMS_LENGTH, CodeGenerator.MAX_JVM_METHOD_PARAMS_LENGTH)}"
        if (Utils.isTesting) {
          throw SparkException.internalError(errMsg.message)
        } else {
          logInfo(errMsg)
          None
        }
      }
    }
  }
}
