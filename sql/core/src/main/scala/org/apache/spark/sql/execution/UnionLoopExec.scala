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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, InterpretedMutableProjection, Literal}
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation.hasUnevaluableExpr
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LocalLimit, LocalRelation, LogicalPlan, OneRowRelation, Project, Union, UnionLoopRef}
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.execution.LogicalRDD.rewriteStatsAndConstraints
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
 *   +- SubqueryAlias t
 *     +- CTERelationRef 0, true, [n#3], false, false
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
 *              Its value is pushed down to UnionLoop in Optimizer in case LocalLimit node is
 *              present in the logical plan and then transferred to UnionLoopExec in
 *              SparkStrategies.
 *              Note here: limit can be applied in the main query calling the recursive CTE, and not
 *              inside the recursive term of recursive CTE.
 */
case class UnionLoopExec(
    loopId: Long,
    @transient anchor: LogicalPlan,
    @transient recursion: LogicalPlan,
    override val output: Seq[Attribute],
    limit: Option[Int] = None) extends LeafExecNode {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(anchor, recursion)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numIterations" -> SQLMetrics.createMetric(sparkContext, "number of recursive iterations"),
    "numAnchorOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of anchor output rows"))

  val localRelationLimit =
    conf.getConf(SQLConf.CTE_RECURSION_ANCHOR_ROWS_LIMIT_TO_CONVERT_TO_LOCAL_RELATION)

  /**
   * This function executes the plan (optionally with appended limit node) and caches the result,
   * with the caching mode specified in config.
   */
  private def executeAndCacheAndCount(plan: LogicalPlan, currentLimit: Int) = {
    // In case limit is defined, we create a (local) limit node above the plan and execute
    // the newly created plan.
    val planWithLimit = if (limit.isDefined) {
      LocalLimit(Literal(currentLimit), plan)
    } else {
      plan
    }
    val df = Dataset.ofRows(session, planWithLimit)

    df.queryExecution.optimizedPlan match {
      case l: LocalRelation =>
        (df, l.data.length.toLong)
      case Project(projectList, _: OneRowRelation) =>
        if (localRelationLimit != 0 && !projectList.exists(hasUnevaluableExpr)) {
          val projection = new InterpretedMutableProjection(projectList, Nil)
          projection.initialize(0)
          val local = LocalRelation(projectList.map(_.toAttribute),
            Seq(projection(InternalRow.empty)))
          (Dataset.ofRows(session, local), 1.toLong)
        } else {
          (df, 1.toLong)
        }
      case _ =>
        val materializedDF = df.repartition()
        val count = materializedDF.queryExecution.toRdd.count()

        // In the case we return a sufficiently small number of rows when executing any step of the
        // recursion we convert the result into a LocalRelation, so that, if the recursion doesn't
        // reference any external tables, we are able to calculate everything in the optimizer,
        // using the ConvertToLocalRelation rule, which significantly improves runtime.
        if (count <= localRelationLimit) {
          val local = LocalRelation.fromExternalRows(anchor.output, df.collect().toIndexedSeq)
         (Dataset.ofRows(session, local), count)
        } else {
          (materializedDF, count)
        }
    }
  }

  /**
   * In the first iteration, anchor term is executed.
   * Then, in each following iteration, the UnionLoopRef node is substituted with the plan from the
   * previous iteration, and such plan is executed.
   * After every iteration, the dataframe is materialized.
   * The recursion stops when the generated dataframe is empty, or either the limit or
   * the specified maximum depth from the config is reached.
   */
  override protected def doExecute(): RDD[InternalRow] = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val numOutputRows = longMetric("numOutputRows")
    val numIterations = longMetric("numIterations")
    val numAnchorOutputRows = longMetric("numAnchorOutputRows")
    val levelLimit = conf.getConf(SQLConf.CTE_RECURSION_LEVEL_LIMIT)
    val rowLimit = conf.getConf(SQLConf.CTE_RECURSION_ROW_LIMIT)

    // currentLimit is initialized from the limit argument, and in each step it is decreased by
    // the number of rows generated in that step.
    // If limit is not passed down, currentLimit is set to be the row limit set by the
    // spark.sql.cteRecursionRowLimit flag. If we breach this limit, then we report an error so that
    // the user knows they aren't getting all the rows they requested.
    var currentLimit = limit.getOrElse(rowLimit)

    val userSpecifiedLimit = limit.isDefined

    val unionChildren = mutable.ArrayBuffer.empty[LogicalPlan]

    var (prevDF, prevCount) = executeAndCacheAndCount(anchor, currentLimit)

    numAnchorOutputRows += prevCount

    var currentLevel = 1

    var limitReached: Boolean = false

    val numPartitions = prevDF.queryExecution.toRdd.partitions.length

    // Main loop for obtaining the result of the recursive query.
    while (prevCount > 0 && !limitReached) {
      var prevPlan: LogicalPlan = null
      // the current plan is created by substituting UnionLoopRef node with the project node of
      // the previous plan.
      // This way we support only UNION ALL case. Additional case should be added for UNION case.
      // One way of supporting UNION case can be seen at SPARK-24497 PR from Peter Toth.
      val newRecursion = recursion.transformWithSubqueries {
        case r: UnionLoopRef if r.loopId == loopId =>
          prevDF.queryExecution.optimizedPlan match {
            case l: LocalRelation =>
              prevPlan = l
              l.copy(output = r.output)
            // This case will be turned into a LocalRelation whenever the flag
            // SQLConf.CTE_RECURSION_ANCHOR_ROWS_LIMIT_TO_CONVERT_TO_LOCAL_RELATION is set to be
            // anything larger than 0. However, we still handle this case in a special way to
            // optimize the case when the flag is set to 0.
            case p @ Project(projectList, _: OneRowRelation) =>
              prevPlan = p
              val prevPlanToRefMapping = projectList.zip(r.output).map {
                case (fa: Alias, ta) => fa.withExprId(ta.exprId).withName(ta.name)
              }
              p.copy(projectList = prevPlanToRefMapping)
            case _ =>
              val logicalRDD = LogicalRDD.fromDataset(prevDF.queryExecution.toRdd, prevDF,
                  prevDF.isStreaming).newInstance()
              prevPlan = logicalRDD
              val logicalPlan = prevDF.logicalPlan
              val optimizedPlan = prevDF.queryExecution.optimizedPlan
              val (stats, constraints) = rewriteStatsAndConstraints(logicalPlan, optimizedPlan)
              logicalRDD.copy(output = r.output)(prevDF.sparkSession, stats, constraints)
          }
      }

      if (levelLimit != -1 && currentLevel > levelLimit) {
        throw new SparkException(
          errorClass = "RECURSION_LEVEL_LIMIT_EXCEEDED",
          messageParameters = Map("levelLimit" -> levelLimit.toString),
          cause = null)
      }

      unionChildren += prevPlan

      if (rowLimit != -1) {
        currentLimit -= prevCount.toInt
        if (currentLimit <= 0) {
          if (userSpecifiedLimit) {
            limitReached = true
          } else {
            throw new SparkException(
              errorClass = "RECURSION_ROW_LIMIT_EXCEEDED",
              messageParameters = Map("rowLimit" -> rowLimit.toString),
              cause = null)
          }
        }
      }

      // Update metrics
      numOutputRows += prevCount
      numIterations += 1

      if (!limitReached) {

        val (df, count) = executeAndCacheAndCount(newRecursion, currentLimit)
        prevDF = df
        prevCount = count

        currentLevel += 1
      }
    }

    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)

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
      val coalescedDF = df.coalesce(numPartitions)
      coalescedDF.queryExecution.toRdd
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
       |Limit: $limit
       |""".stripMargin
  }
}
