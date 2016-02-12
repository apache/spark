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

import scala.collection.immutable.IndexedSeq

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Apply the all of the GroupExpressions to every input row, hence we will get
 * multiple output rows for a input row.
 * @param projections The group of expressions, all of the group expressions should
 *                    output the same schema specified bye the parameter `output`
 * @param output      The output Schema
 * @param child       Child operator
 */
case class Expand(
    projections: Seq[Seq[Expression]],
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryNode with CodegenSupport {

  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

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

  override def upstream(): RDD[InternalRow] = {
    child.asInstanceOf[CodegenSupport].upstream()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // Some columns have the same expression in all the projections, so collect the unique
    // expressions.
    val columnUniqueExpressions: IndexedSeq[Set[Expression]] = output.indices.map { i =>
      projections.map(p => p(i)).toSet
    }

    // Create the variables for output
    ctx.currentVars = input
    val resultVars = columnUniqueExpressions.zipWithIndex.map { case (exprs, i) =>
      val firstExpr = exprs.head
      if (exprs.size == 1) {
        // The value of this column will not change, use the variables directly.
        BindReferences.bindReference(firstExpr, child.output).gen(ctx)
      } else {
        // The value of this column will change, so create new variables for them, because they
        // could be constants in some expressions.
        val isNull = ctx.freshName("isNull")
        val value = ctx.freshName("value")
        val code = s"""
           |boolean $isNull = true;
           |${ctx.javaType(firstExpr.dataType)} $value = ${ctx.defaultValue(firstExpr.dataType)};
         """.stripMargin
        ExprCode(code, isNull, value)
      }
    }

    // The source code returned by `consume()` could be huge, we can't call `consume()` for each of
    // the projections, otherwise the generated code will have lots of duplicated codes. Instead,
    // we should generate a loop for all the projections, use switch/case to select a projection
    // based on the loop index, then only call `consume()` once.
    //
    // These output variables will be created before the loop, their values will be updated in
    // switch/case inside the loop.
    val cases = projections.zipWithIndex.map { case (exprs, i) =>
      val changes: Seq[(Expression, Int)] = exprs.zipWithIndex.filter { case (e, j) =>
        // the column with single unique expression does not need to be updated inside the loop.
        columnUniqueExpressions(j).size > 1
      }
      val updates = changes.map { case (e, j) =>
        val ev = BindReferences.bindReference(e, child.output).gen(ctx)
        s"""
           |${ev.code}
           |${resultVars(j).isNull} = ${ev.isNull};
           |${resultVars(j).value} = ${ev.value};
         """.stripMargin
      }
      s"""
         |case $i:
         |  ${updates.mkString("\n").trim}
         |  break;
       """.stripMargin
    }

    val numOutput = metricTerm(ctx, "numOutputRows")
    val i = ctx.freshName("i")
    s"""
       |${resultVars.map(_.code).mkString("\n").trim}
       |for (int $i = 0; $i < ${projections.length}; $i ++) {
       |  switch ($i) {
       |    ${cases.mkString("\n").trim}
       |  }
       |  $numOutput.add(1);
       |  ${consume(ctx, resultVars)}
       |}
     """.stripMargin
  }
}
