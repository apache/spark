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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * The primary workflow for executing relational queries using Spark.  Designed to allow easy
 * access to the intermediate phases of query execution for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class QueryExecution(val sqlContext: SQLContext, val logical: LogicalPlan) {

  def assertAnalyzed(): Unit = sqlContext.analyzer.checkAnalysis(analyzed)

  lazy val analyzed: LogicalPlan = sqlContext.analyzer.execute(logical)

  lazy val withCachedData: LogicalPlan = {
    assertAnalyzed()
    sqlContext.cacheManager.useCachedData(analyzed)
  }

  lazy val optimizedPlan: LogicalPlan = sqlContext.optimizer.execute(withCachedData)

  lazy val sparkPlan: SparkPlan = {
    SQLContext.setActive(sqlContext)
    sqlContext.planner.plan(optimizedPlan).next()
  }

  private def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: SupportCodegen if plan.supportCodegen =>
      // Non-leaf with CodegenFallback does not work with whole stage codegen
      val willFallback = plan.expressions.exists(
        _.find(e => e.isInstanceOf[CodegenFallback] && !e.isInstanceOf[LeafExpression]).isDefined
      )
      // the generated code will be huge if there are too many columns
      val haveManyColumns = plan.output.length > 200
      !willFallback && !haveManyColumns
    case _ => false
  }

  /**
    * Collapse multiple chained plans into WholeStageCodegen to compile them into a single Java
    * function for better performance.
    */
  private def collapse(plan: SparkPlan): SparkPlan = plan transform {
    case plan: SupportCodegen if supportCodegen(plan) &&
      // Whole stage codegen is only useful when there are at least two levels of operators that
      // support it (save at least one projection/iterator).
      plan.children.exists(supportCodegen) =>

      var inputs = ArrayBuffer[SparkPlan]()
      val combined = plan.transform {
        case p if !supportCodegen(p) =>
          inputs += p
          InputAdapter(p)
      }.asInstanceOf[SupportCodegen]
      WholeStageCodegen(combined, inputs)
  }

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = {
    val plan = sqlContext.prepareForExecution.execute(sparkPlan)
    if (sqlContext.conf.wholeStageEnabled) {
      collapse(plan)
    } else {
      plan
    }
  }

  /** Internal version of the RDD. Avoids copies and has no schema */
  lazy val toRdd: RDD[InternalRow] = executedPlan.execute()

  protected def stringOrError[A](f: => A): String =
    try f.toString catch { case e: Throwable => e.toString }

  def simpleString: String = {
    s"""== Physical Plan ==
       |${stringOrError(executedPlan)}
      """.stripMargin.trim
  }

  override def toString: String = {
    def output =
      analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}").mkString(", ")

    s"""== Parsed Logical Plan ==
       |${stringOrError(logical)}
       |== Analyzed Logical Plan ==
       |${stringOrError(output)}
       |${stringOrError(analyzed)}
       |== Optimized Logical Plan ==
       |${stringOrError(optimizedPlan)}
       |== Physical Plan ==
       |${stringOrError(executedPlan)}
    """.stripMargin.trim
  }
}
