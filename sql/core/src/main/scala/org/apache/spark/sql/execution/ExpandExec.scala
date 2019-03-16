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
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.metric.SQLMetrics

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
    child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def references: AttributeSet =
    AttributeSet(projections.flatten.flatMap(_.references))

  private[this] val projection =
    (exprs: Seq[Expression]) => UnsafeProjection.create(exprs, child.output)

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val numOutputRows = longMetric("numOutputRows")

    child.execute().mapPartitions { iter =>
      val groups = projections.map(projection).toArray
      new Iterator[InternalRow] {
        private[this] var result: InternalRow = _
        private[this] var idx = -1  // -1 means the initial state
        private[this] var input: InternalRow = _

        override final def hasNext: Boolean = (-1 < idx && idx < groups.length) || iter.hasNext

        override final def next(): InternalRow = {
          if (idx <= 0) {
            // in the initial (-1) or beginning(0) of a new input row, fetch the next input tuple
            input = iter.next()
            idx = 0
          }

          result = groups(idx)(input)
          idx += 1

          if (idx == groups.length && iter.hasNext) {
            idx = 0
          }

          numOutputRows += 1
          result
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
     */

    // Tracks whether a column has the same output for all rows.
    // Size of sameOutput array should equal N.
    // If sameOutput(i) is true, then the i-th column has the same value for all output rows given
    // an input row.
    val sameOutput: Array[Boolean] = output.indices.map { colIndex =>
      projections.map(p => p(colIndex)).toSet.size == 1
    }.toArray

    // Part 1: declare variables for each column
    // If a column has the same value for all output rows, then we also generate its computation
    // right after declaration. Otherwise its value is computed in the part 2.
    lazy val attributeSeq: AttributeSeq = child.output
    val outputColumns = output.indices.map { col =>
      val firstExpr = projections.head(col)
      if (sameOutput(col)) {
        // This column is the same across all output rows. Just generate code for it here.
        BindReferences.bindReference(firstExpr, attributeSeq).genCode(ctx)
      } else {
        val isNull = ctx.freshName("isNull")
        val value = ctx.freshName("value")
        val code = code"""
          |boolean $isNull = true;
          |${CodeGenerator.javaType(firstExpr.dataType)} $value =
          |  ${CodeGenerator.defaultValue(firstExpr.dataType)};
         """.stripMargin
        ExprCode(
          code,
          JavaCode.isNullVariable(isNull),
          JavaCode.variable(value, firstExpr.dataType))
      }
    }

    // Part 2: switch/case statements
    val cases = projections.zipWithIndex.map { case (exprs, row) =>
      var updateCode = ""
      for (col <- exprs.indices) {
        if (!sameOutput(col)) {
          val ev = BindReferences.bindReference(exprs(col), attributeSeq).genCode(ctx)
          updateCode +=
            s"""
               |${ev.code}
               |${outputColumns(col).isNull} = ${ev.isNull};
               |${outputColumns(col).value} = ${ev.value};
            """.stripMargin
        }
      }

      s"""
         |case $row:
         |  ${updateCode.trim}
         |  break;
       """.stripMargin
    }

    val numOutput = metricTerm(ctx, "numOutputRows")
    val i = ctx.freshName("i")
    // these column have to declared before the loop.
    val evaluate = evaluateVariables(outputColumns)
    s"""
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
}
