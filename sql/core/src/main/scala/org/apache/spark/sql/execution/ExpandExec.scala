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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf

/**
 * Apply all of the GroupExpressions to every input row, hence we will get
 * multiple output rows for an input row.
 * @param projections The group of expressions, all of the group expressions should
 *                    output the same schema specified bye the parameter `output`
 * @param output      The output Schema
 * @param child       Child operator
 */
case class ExpandExec(
    projections: Seq[Seq[Expression]],
    output: Seq[Attribute],
    child: SparkPlan,
    // When true, this Expand is part of a plan marked for single-task execution by the
    // `MarkSingleTaskExecution` optimizer rule, and forwards the child's `SinglePartition`
    // output partitioning (see `outputPartitioning`).
    useSingleTask: Boolean = false)
  extends UnaryExecNode with CodegenSupport {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning. Expand only replicates rows within a partition and never moves rows
  // across partitions, so when this Expand is part of a plan marked for single-task execution
  // and the child produces a single partition, we can forward the `SinglePartition` property to
  // avoid an unneeded shuffle.
  override def outputPartitioning: Partitioning = {
    if (useSingleTask && child.outputPartitioning == SinglePartition) {
      SinglePartition
    } else {
      UnknownPartitioning(0)
    }
  }

  // Show `useSingleTask` in the string representation only when it is set, so that plans not
  // using single-task execution (the default) keep their existing explain output.
  override protected def stringArgs: Iterator[Any] = {
    if (useSingleTask) {
      super.stringArgs
    } else {
      Iterator(projections, output, child)
    }
  }

  @transient
  override lazy val references: AttributeSet =
    AttributeSet(projections.flatten.flatMap(_.references))

  private[this] val projection =
    (exprs: Seq[Expression]) => UnsafeProjection.create(exprs, child.output)

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      val groups = projections.map(projection).toArray
      groups.foreach(_.initialize(index))
      iter.flatMap { input =>
        groups.iterator.map { group =>
          numOutputRows += 1
          group(input)
        }
      }
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def needCopyResult: Boolean = true

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    /*
     * When the projections list looks like:
     *   expr1A, exprB, expr1C
     *   expr2A, exprB, expr2C
     *   ...
     *   expr(N-1)A, exprB, expr(N-1)C
     *
     * i.e. column A and C have different values for each output row, but column B stays constant.
     *
     * The generated code looks something like (note that B is only computed once in declaration):
     *
     * // part 1: declare all the columns
     * colA = ...
     * colB = ...
     * colC = ...
     *
     * // part 2: code that computes the columns
     * for (row = 0; row < N; row++) {
     *   switch (row) {
     *     case 0:
     *       colA = ...
     *       colC = ...
     *     case 1:
     *       colA = ...
     *       colC = ...
     *     ...
     *     case N - 1:
     *       colA = ...
     *       colC = ...
     *   }
     *   // increment metrics and consume output values
     * }
     *
     * We use a for loop here so we only includes one copy of the consume code and avoid code
     * size explosion.
     *
     * In addition, when subexpression elimination is enabled, common subexpressions shared by
     * the branch expressions (e.g. an expensive condition repeated in many branches) are
     * evaluated only once per input row, before the loop, since all the branches consume the
     * same input row.
     */

    // Tracks whether a column has the same output for all rows.
    // Size of sameOutput array should equal N.
    // If sameOutput(i) is true, then the i-th column has the same value for all output rows given
    // an input row.
    val sameOutput: Array[Boolean] = output.indices.map { colIndex =>
      projections.map(p => p(colIndex)).toSet.size == 1
    }.toArray

    // Bind all the branch expressions once up front, so that identical expressions appearing in
    // different branches bind to identical trees and can be deduplicated by subexpression
    // elimination below.
    val boundProjections: Seq[Seq[Expression]] = projections.map { exprs =>
      BindReferences.bindReferences(exprs, child.output)
    }

    // Set up subexpression elimination over all the branch expressions. This deduplicates
    // repeated subexpressions both within a branch and across branches. The code evaluating the
    // common subexpressions is emitted once before the branch loop (see the end of this method).
    val subExprs: SubExprCodes = if (conf.subexpressionEliminationEnabled) {
      ctx.subexpressionEliminationForWholeStageCodegen(boundProjections.flatten)
    } else {
      SubExprCodes(Map.empty, Seq.empty)
    }

    // Part 1: declare variables for each column
    // If a column has the same value for all output rows, then we also generate its computation
    // right after declaration. Otherwise its value is computed in the part 2.
    val outputColumns = ctx.withSubExprEliminationExprs(subExprs.states) {
      output.indices.map { col =>
        val firstExpr = boundProjections.head(col)
        if (sameOutput(col)) {
          // This column is the same across all output rows. Just generate code for it here.
          firstExpr.genCode(ctx)
        } else {
          val isNull = ctx.addMutableState(
            CodeGenerator.JAVA_BOOLEAN,
            "resultIsNull",
            v => s"$v = true;")
          val value = ctx.addMutableState(
            CodeGenerator.javaType(firstExpr.dataType),
            "resultValue",
            v => s"$v = ${CodeGenerator.defaultValue(firstExpr.dataType)};")

          ExprCode(
            JavaCode.isNullVariable(isNull),
            JavaCode.variable(value, firstExpr.dataType))
        }
      }
    }

    // Part 2: switch/case statements
    val switchCaseExprs = projections.indices.map { row =>
      val colsToGenerate = projections(row).indices.filter(col => !sameOutput(col))
      val exprCodes = ctx.withSubExprEliminationExprs(subExprs.states) {
        colsToGenerate.map(col => boundProjections(row)(col).genCode(ctx))
      }
      val (exprCodesWithIndices, inputVarSets) = colsToGenerate.zip(exprCodes).map {
        case (col, exprCode) =>
          // Pass `subExprs.states` so that the input variables of the split switch/case
          // functions below include the variables holding the common subexpression values,
          // which are evaluated outside of the split functions.
          val inputVars = CodeGenerator.getLocalInputVariableValues(
            ctx, boundProjections(row)(col), subExprs.states)._1
          ((col, exprCode), inputVars)
      }.unzip

      val inputVars = inputVarSets.foldLeft(Set.empty[VariableValue])(_ ++ _)
      (row, exprCodesWithIndices, inputVars.toSeq)
    }

    val updateCodes = switchCaseExprs.map { case (_, exprCodes, _) =>
      exprCodes.map { case (col, ev) =>
        s"""
           |${ev.code}
           |${outputColumns(col).isNull} = ${ev.isNull};
           |${outputColumns(col).value} = ${ev.value};
         """.stripMargin
      }.mkString("\n")
    }

    val splitThreshold = SQLConf.get.methodSplitThreshold
    val cases = if (switchCaseExprs.flatMap(_._2.map(_._2.code.length)).sum > splitThreshold) {
      switchCaseExprs.zip(updateCodes).map { case ((row, _, inputVars), updateCode) =>
        val paramLength = CodeGenerator.calculateParamLengthFromExprValues(inputVars)
        val maybeSplitUpdateCode = if (CodeGenerator.isValidParamLength(paramLength)) {
          val switchCaseFunc = ctx.freshName("switchCaseCode")
          val argList = inputVars.map { v =>
            s"${CodeGenerator.typeName(v.javaType)} ${v.variableName}"
          }
          ctx.addNewFunction(switchCaseFunc,
            s"""
               |private void $switchCaseFunc(${argList.mkString(", ")}) {
               |  $updateCode
               |}
             """.stripMargin)

          s"$switchCaseFunc(${inputVars.map(_.variableName).mkString(", ")});"
        } else {
          updateCode
        }
        s"""
           |case $row:
           |  $maybeSplitUpdateCode
           |  break;
         """.stripMargin
      }
    } else {
      switchCaseExprs.map(_._1).zip(updateCodes).map { case (row, updateCode) =>
        s"""
           |case $row:
           |  $updateCode
           |  break;
         """.stripMargin
      }
    }

    val numOutput = metricTerm(ctx, "numOutputRows")
    val i = ctx.freshName("i")
    // these column have to declared before the loop.
    val evaluate = evaluateVariables(outputColumns)
    // The input variables used by the common subexpressions have to be evaluated first, then
    // the common subexpressions themselves, both before the loop since every branch consumes
    // the same input row.
    val evaluateSubExprInputs = evaluateVariables(subExprs.exprCodesNeedEvaluate)
    val evaluateSubExprs = ctx.evaluateSubExprEliminationState(subExprs.states.values)
    s"""
       |$evaluateSubExprInputs
       |$evaluateSubExprs
       |$evaluate
       |for (int $i = 0; $i < ${projections.length}; $i ++) {
       |  switch ($i) {
       |    ${cases.mkString("\n").trim}
       |  }
       |  $numOutput.add(1);
       |  ${consume(ctx, outputColumns)}
       |}
     """.stripMargin
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ExpandExec =
    copy(child = newChild)
}
