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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

case class ReusedProjectExec(override val output: Seq[Attribute], child: ProjectExec)
  extends UnaryExecNode with CodegenSupport {

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val exprs = output.map(x =>
      ExpressionCanonicalizer.execute(BindReferences.bindReference(x, output)))
    ctx.currentVars = input
    val resultVars = exprs.map(_.genCode(ctx))
    // Evaluation of non-deterministic expressions can't be deferred.
    val nonDeterministicAttrs = output.filterNot(_.deterministic).map(_.toAttribute)
    s"""
       |${evaluateRequiredVariables(output, resultVars, AttributeSet(nonDeterministicAttrs))}
       |${consume(ctx, resultVars)}
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = child.execute()

}

/**
 * The class for reuse project in SparkPlan.
 */
case class ReuseProject(conf: SQLConf) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.exchangeReuseEnabled) {
      return plan
    }
    // Build a hash map using schema of project to avoid O(N*N) sameResult calls.
    val projects = mutable.HashMap[StructType, ArrayBuffer[ProjectExec]]()
    plan.transformUp {
      case project: ProjectExec =>
        // the projects that have same results usually also have same schemas (same column names).
        val sameSchema = projects.getOrElseUpdate(project.schema, ArrayBuffer[ProjectExec]())
        val samePlan = sameSchema.find { e =>
          project.sameResult(e)
        }
        if (samePlan.isDefined) {
          ReusedProjectExec(project.output, samePlan.get)
        } else {
          sameSchema += project
          project
        }
    }
  }
}
