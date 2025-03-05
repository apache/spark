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

import org.apache.spark.SparkException
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalLimit, LogicalPlan, Project, Union, UnionLoopRef}
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf


/**
 * The physical node for recursion. Currently only UNION ALL case is supported.
 * For the details about the execution, look at the comment above doExecute function.
 *
 * A simple recursive query:
 * {{{
 * WITH RECURSIVE t(n) AS (
 *     SELECT 1
 *     UNION ALL
 *     SELECT n+1 FROM t WHERE n < 5)
 * SELECT * FROM t;
 * }}}
 * Corresponding logical plan for the recursive query above:
 * {{{
 * WithCTE
 * :- CTERelationDef 0, false
 * :  +- SubqueryAlias t
 * :     +- Project [1#0 AS n#3]
 * :        +- UnionLoop 0
 * :           :- Project [1 AS 1#0]
 * :           :  +- OneRowRelation
 * :           +- Project [(n#1 + 1) AS (n + 1)#2]
 * :              +- Filter (n#1 < 5)
 * :                 +- SubqueryAlias t
 * :                    +- Project [1#0 AS n#1]
 * :                       +- UnionLoopRef 0, [1#0], false
 * +- Project [n#3]
 * +- SubqueryAlias t
 * +- CTERelationRef 0, true, [n#3], false, false
 * }}}
 *
 * @param loopId This is id of the CTERelationDef containing the recursive query. Its value is
 *               first passed down to UnionLoop when creating it, and then to UnionLoopExec in
 *               SparkStrategies.
 * @param anchor The logical plan of the initial element of the loop.
 * @param recursion The logical plan that describes the recursion with an [[UnionLoopRef]] node.
 *                  CTERelationRef, which is marked as recursive, gets substituted with
 *                  [[UnionLoopRef]] in ResolveWithCTE.
 *                  Both anchor and recursion are marked with @transient annotation, so that they
 *                  are not serialized.
 * @param output The output attributes of this loop.
 * @param limit If defined, the total number of rows output by this operator will be bounded by
 *              limit.
 *              Its value is pushed down to UnionLoop in Optimizer in case Limit node is present
 *              in the logical plan and then transferred to UnionLoopExec in SparkStrategies.
 *              Note here: limit can be applied in the main query calling the recursive CTE, and not
 *              inside the recursive term of recursive CTE.
 */
case class UnionLoopExec(
    loopId: Long,
    @transient anchor: LogicalPlan,
    @transient recursion: LogicalPlan,
    override val output: Seq[Attribute],
    localLimit: Option[Int] = None,
    globalLimit: Option[Int] = None) extends LeafExecNode {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(anchor, recursion)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numIterations" -> SQLMetrics.createMetric(sparkContext, "number of recursive iterations"))

  private val simpleRecursion = {
    recursion match {
      case Project(_, Filter(_, Project(_, UnionLoopRef(_, _, _)))) =>
        true
      case Filter(_, Project(_, UnionLoopRef(_, _, _))) =>
        true
      case Project(_, Filter(_, UnionLoopRef(_, _, _))) =>
        true
      case Project(_, UnionLoopRef(_, _, _)) =>
        true
      case _ =>
        false
  }
  }
  /**
   * This function executes the plan (optionally with appended limit node) and caches the result,
   * with the caching mode specified in config.
   */
  private def executeAndCacheAndCount(
                                       plan: LogicalPlan, currentLimit: Int) = {
    // In case limit is defined, we create a (local) limit node above the plan and execute
    // the newly created plan.
    val planOrLimitedPlan = if (globalLimit.isDefined || localLimit.isDefined) {
      LocalLimit(Literal(currentLimit), plan)
    } else {
      plan
    }
    val df = Dataset.ofRows(session, planOrLimitedPlan)
    val newDF = {
      if (!simpleRecursion) {
        df.repartition()
      } else {
        df
      }
    }
    val count = newDF.count()
    (newDF, count)
  }

  /**
   * In the first iteration, anchor term is executed.
   * Then, in each following iteration, the UnionLoopRef node is substituted with the plan from the
   * previous iteration, and such plan is executed.
   * After every iteration, the dataframe is repartitioned.
   * The recursion stops when the generated dataframe is empty, or either the limit or
   * the specified maximum depth from the config is reached.
   */
  override protected def doExecute(): RDD[InternalRow] = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val numOutputRows = longMetric("numOutputRows")
    val numIterations = longMetric("numIterations")
    val levelLimit = conf.getConf(SQLConf.CTE_RECURSION_LEVEL_LIMIT)

    // currentLimit is initialized from the limit argument, and in each step it is decreased by
    // the number of rows generated in that step.
    // If limit is not passed down, currentLimit is set to be zero and won't be considered in the
    // condition of while loop down (limit.isEmpty will be true).
    var currentLimit = {
      if (globalLimit.isDefined) {
        globalLimit.get
      } else if (localLimit.isDefined) {
        localLimit.get
      }
      else {
        -1
      }
    }

    val unionChildren = mutable.ArrayBuffer.empty[LogicalRDD]

    var (prevDF, prevCount) = executeAndCacheAndCount(anchor, currentLimit)

    var currentLevel = 1

    // Main loop for obtaining the result of the recursive query.
    while (prevCount > 0 && ((globalLimit.isEmpty && localLimit.isEmpty) || currentLimit > 0)) {

      if (levelLimit != -1 && currentLevel > levelLimit) {
        throw new SparkException(
          errorClass = "RECURSION_LEVEL_LIMIT_EXCEEDED",
          messageParameters = Map("levelLimit" -> levelLimit.toString),
          cause = null)
      }

      // Inherit stats and constraints from the dataset of the previous iteration.
      val prevPlan = LogicalRDD.fromDataset(prevDF.queryExecution.toRdd, prevDF, prevDF.isStreaming)
        .newInstance()
      unionChildren += prevPlan

      // Update metrics
      numOutputRows += prevCount
      numIterations += 1
      SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)

      // the current plan is created by substituting UnionLoopRef node with the project node of
      // the previous plan.
      // This way we support only UNION ALL case. Additional case should be added for UNION case.
      // One way of supporting UNION case can be seen at SPARK-24497 PR from Peter Toth.
      val newRecursion = recursion.transform {
        case r: UnionLoopRef =>
          val prevPlanToRefMapping = prevPlan.output.zip(r.output).map {
            case (fa, ta) => Alias(fa, ta.name)(ta.exprId)
          }
          Project(prevPlanToRefMapping, prevPlan)
      }

      val (df, count) = executeAndCacheAndCount(newRecursion, currentLimit)
      prevDF = df
      prevCount = count

      currentLevel += 1
      if (globalLimit.isDefined || localLimit.isDefined) {
        currentLimit -= count.toInt
      }
    }

    if (unionChildren.isEmpty) {
      new EmptyRDD[InternalRow](sparkContext)
    } else {
      val df = {
        if (unionChildren.length == 1) {
          Dataset.ofRows(session, unionChildren.head)
        } else {
          Dataset.ofRows(session, Union(unionChildren.toSeq))
        }
      }
      val dfMaybeCoalesced = {
        if (globalLimit.isEmpty && localLimit.isDefined) {
          df.coalesce(1)
        } else {
          df
        }
      }
      dfMaybeCoalesced.queryExecution.toRdd
    }
  }

  override def doCanonicalize(): SparkPlan =
    super.doCanonicalize().asInstanceOf[UnionLoopExec]
      .copy(anchor = anchor.canonicalized, recursion = recursion.canonicalized)

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |Loop id: $loopId
       |${QueryPlan.generateFieldString("Output", output)}
       |LocalLimit: $localLimit
       |GlobalLimit: $globalLimit
       |""".stripMargin
  }
}
