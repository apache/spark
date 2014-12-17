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

import org.apache.spark.sql.{SQLContext, Strategy, execution}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.columnar.{InMemoryRelation, InMemoryColumnarTableScan}
import org.apache.spark.sql.parquet._


private[sql] abstract class SparkStrategies extends QueryPlanner[SparkPlan] {
  self: SQLContext#SparkPlanner =>

  object LeftSemiJoin extends Strategy with PredicateHelper {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      // Find left semi joins where at least some predicates can be evaluated by matching join keys
      case ExtractEquiJoinKeys(LeftSemi, leftKeys, rightKeys, condition, left, right) =>
        val semiJoin = joins.LeftSemiJoinHash(
          leftKeys, rightKeys, planLater(left), planLater(right))
        condition.map(Filter(_, semiJoin)).getOrElse(semiJoin) :: Nil
      // no predicate can be evaluated by matching hash keys
      case logical.Join(left, right, LeftSemi, condition) =>
        joins.LeftSemiJoinBNL(planLater(left), planLater(right), condition) :: Nil
      case _ => Nil
    }
  }

  /**
   * Uses the ExtractEquiJoinKeys pattern to find joins where at least some of the predicates can be
   * evaluated by matching hash keys.
   *
   * This strategy applies a simple optimization based on the estimates of the physical sizes of
   * the two join sides.  When planning a [[joins.BroadcastHashJoin]], if one side has an
   * estimated physical size smaller than the user-settable threshold
   * [[org.apache.spark.sql.SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]], the planner would mark it as the
   * ''build'' relation and mark the other relation as the ''stream'' side.  The build table will be
   * ''broadcasted'' to all of the executors involved in the join, as a
   * [[org.apache.spark.broadcast.Broadcast]] object.  If both estimates exceed the threshold, they
   * will instead be used to decide the build side in a [[joins.ShuffledHashJoin]].
   */
  object HashJoin extends Strategy with PredicateHelper {

    private[this] def makeBroadcastHashJoin(
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Option[Expression],
        side: joins.BuildSide) = {
      val broadcastHashJoin = execution.joins.BroadcastHashJoin(
        leftKeys, rightKeys, side, planLater(left), planLater(right))
      condition.map(Filter(_, broadcastHashJoin)).getOrElse(broadcastHashJoin) :: Nil
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, right)
        if sqlContext.autoBroadcastJoinThreshold > 0 &&
           right.statistics.sizeInBytes <= sqlContext.autoBroadcastJoinThreshold =>
        makeBroadcastHashJoin(leftKeys, rightKeys, left, right, condition, joins.BuildRight)

      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, right)
        if sqlContext.autoBroadcastJoinThreshold > 0 &&
           left.statistics.sizeInBytes <= sqlContext.autoBroadcastJoinThreshold =>
          makeBroadcastHashJoin(leftKeys, rightKeys, left, right, condition, joins.BuildLeft)

      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, right) =>
        val buildSide =
          if (right.statistics.sizeInBytes <= left.statistics.sizeInBytes) {
            joins.BuildRight
          } else {
            joins.BuildLeft
          }
        val hashJoin = joins.ShuffledHashJoin(
          leftKeys, rightKeys, buildSide, planLater(left), planLater(right))
        condition.map(Filter(_, hashJoin)).getOrElse(hashJoin) :: Nil

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right) =>
        joins.HashOuterJoin(
          leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right)) :: Nil

      case _ => Nil
    }
  }

  object HashAggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      // Aggregations that can be performed in two phases, before and after the shuffle.

      // Cases where all aggregates can be codegened.
      case PartialAggregation(
             namedGroupingAttributes,
             rewrittenAggregateExpressions,
             groupingExpressions,
             partialComputation,
             child)
             if canBeCodeGened(
                  allAggregates(partialComputation) ++
                  allAggregates(rewrittenAggregateExpressions)) &&
               codegenEnabled =>
          execution.GeneratedAggregate(
            partial = false,
            namedGroupingAttributes,
            rewrittenAggregateExpressions,
            execution.GeneratedAggregate(
              partial = true,
              groupingExpressions,
              partialComputation,
              planLater(child))) :: Nil

      // Cases where some aggregate can not be codegened
      case PartialAggregation(
             namedGroupingAttributes,
             rewrittenAggregateExpressions,
             groupingExpressions,
             partialComputation,
             child) =>
        execution.Aggregate(
          partial = false,
          namedGroupingAttributes,
          rewrittenAggregateExpressions,
          execution.Aggregate(
            partial = true,
            groupingExpressions,
            partialComputation,
            planLater(child))) :: Nil

      case _ => Nil
    }

    def canBeCodeGened(aggs: Seq[AggregateExpression]) = !aggs.exists {
      case _: Sum | _: Count | _: Max | _: CombineSetsAndCount => false
      // The generated set implementation is pretty limited ATM.
      case CollectHashSet(exprs) if exprs.size == 1  &&
           Seq(IntegerType, LongType).contains(exprs.head.dataType) => false
      case _ => true
    }

    def allAggregates(exprs: Seq[Expression]) =
      exprs.flatMap(_.collect { case a: AggregateExpression => a })
  }

  object BroadcastNestedLoopJoin extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Join(left, right, joinType, condition) =>
        val buildSide =
          if (right.statistics.sizeInBytes <= left.statistics.sizeInBytes) {
            joins.BuildRight
          } else {
            joins.BuildLeft
          }
        joins.BroadcastNestedLoopJoin(
          planLater(left), planLater(right), buildSide, joinType, condition) :: Nil
      case _ => Nil
    }
  }

  object CartesianProduct extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Join(left, right, _, None) =>
        execution.joins.CartesianProduct(planLater(left), planLater(right)) :: Nil
      case logical.Join(left, right, Inner, Some(condition)) =>
        execution.Filter(condition,
          execution.joins.CartesianProduct(planLater(left), planLater(right))) :: Nil
      case _ => Nil
    }
  }

  protected lazy val singleRowRdd =
    sparkContext.parallelize(Seq(new GenericRow(Array[Any]()): Row), 1)

  object TakeOrdered extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Limit(IntegerLiteral(limit), logical.Sort(order, child)) =>
        execution.TakeOrdered(limit, order, planLater(child)) :: Nil
      case _ => Nil
    }
  }

  object ParquetOperations extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      // TODO: need to support writing to other types of files.  Unify the below code paths.
      case logical.WriteToFile(path, child) =>
        val relation =
          ParquetRelation.create(path, child, sparkContext.hadoopConfiguration, sqlContext)
        // Note: overwrite=false because otherwise the metadata we just created will be deleted
        InsertIntoParquetTable(relation, planLater(child), overwrite = false) :: Nil
      case logical.InsertIntoTable(table: ParquetRelation, partition, child, overwrite) =>
        InsertIntoParquetTable(table, planLater(child), overwrite) :: Nil
      case PhysicalOperation(projectList, filters: Seq[Expression], relation: ParquetRelation) =>
        val prunePushedDownFilters =
          if (sqlContext.parquetFilterPushDown) {
            (predicates: Seq[Expression]) => {
              // Note: filters cannot be pushed down to Parquet if they contain more complex
              // expressions than simple "Attribute cmp Literal" comparisons. Here we remove all
              // filters that have been pushed down. Note that a predicate such as "(A AND B) OR C"
              // can result in "A OR C" being pushed down. Here we are conservative in the sense
              // that even if "A" was pushed and we check for "A AND B" we still want to keep
              // "A AND B" in the higher-level filter, not just "B".
              predicates.map(p => p -> ParquetFilters.createFilter(p)).collect {
                case (predicate, None) => predicate
              }
            }
          } else {
            identity[Seq[Expression]] _
          }
        pruneFilterProject(
          projectList,
          filters,
          prunePushedDownFilters,
          ParquetTableScan(
            _,
            relation,
            if (sqlContext.parquetFilterPushDown) filters else Nil)) :: Nil

      case _ => Nil
    }
  }

  object InMemoryScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters, mem: InMemoryRelation) =>
        pruneFilterProject(
          projectList,
          filters,
          identity[Seq[Expression]], // All filters still need to be evaluated.
          InMemoryColumnarTableScan(_,  filters, mem)) :: Nil
      case _ => Nil
    }
  }

  // Can we automate these 'pass through' operations?
  object BasicOperators extends Strategy {
    def numPartitions = self.numPartitions

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Distinct(child) =>
        execution.Distinct(partial = false,
          execution.Distinct(partial = true, planLater(child))) :: Nil

      case logical.Sort(sortExprs, child) if sqlContext.externalSortEnabled =>
        execution.ExternalSort(sortExprs, global = true, planLater(child)):: Nil
      case logical.Sort(sortExprs, child) =>
        execution.Sort(sortExprs, global = true, planLater(child)):: Nil

      case logical.SortPartitions(sortExprs, child) =>
        // This sort only sorts tuples within a partition. Its requiredDistribution will be
        // an UnspecifiedDistribution.
        execution.Sort(sortExprs, global = false, planLater(child)) :: Nil
      case logical.Project(projectList, child) =>
        execution.Project(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        execution.Filter(condition, planLater(child)) :: Nil
      case logical.Expand(projections, output, child) =>
        execution.Expand(projections, output, planLater(child)) :: Nil
      case logical.Aggregate(group, agg, child) =>
        execution.Aggregate(partial = false, group, agg, planLater(child)) :: Nil
      case logical.Sample(fraction, withReplacement, seed, child) =>
        execution.Sample(fraction, withReplacement, seed, planLater(child)) :: Nil
      case SparkLogicalPlan(alreadyPlanned) => alreadyPlanned :: Nil
      case logical.LocalRelation(output, data) =>
        val nPartitions = if (data.isEmpty) 1 else numPartitions
        PhysicalRDD(
          output,
          RDDConversions.productToRowRdd(sparkContext.parallelize(data, nPartitions),
            StructType.fromAttributes(output))) :: Nil
      case logical.Limit(IntegerLiteral(limit), child) =>
        execution.Limit(limit, planLater(child)) :: Nil
      case Unions(unionChildren) =>
        execution.Union(unionChildren.map(planLater)) :: Nil
      case logical.Except(left, right) =>
        execution.Except(planLater(left), planLater(right)) :: Nil
      case logical.Intersect(left, right) =>
        execution.Intersect(planLater(left), planLater(right)) :: Nil
      case logical.Generate(generator, join, outer, _, child) =>
        execution.Generate(generator, join = join, outer = outer, planLater(child)) :: Nil
      case logical.NoRelation =>
        execution.PhysicalRDD(Nil, singleRowRdd) :: Nil
      case logical.Repartition(expressions, child) =>
        execution.Exchange(HashPartitioning(expressions, numPartitions), planLater(child)) :: Nil
      case e @ EvaluatePython(udf, child, _) =>
        BatchPythonEvaluation(udf, e.output, planLater(child)) :: Nil
      case LogicalRDD(output, rdd) => PhysicalRDD(output, rdd) :: Nil
      case _ => Nil
    }
  }

  case class CommandStrategy(context: SQLContext) extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case r: RunnableCommand => ExecutedCommand(r) :: Nil
      case logical.SetCommand(kv) =>
        Seq(execution.SetCommand(kv, plan.output)(context))
      case logical.ExplainCommand(logicalPlan, extended) =>
        Seq(execution.ExplainCommand(logicalPlan, plan.output, extended)(context))
      case logical.CacheTableCommand(tableName, optPlan, isLazy) =>
        Seq(execution.CacheTableCommand(tableName, optPlan, isLazy))
      case logical.UncacheTableCommand(tableName) =>
        Seq(execution.UncacheTableCommand(tableName))
      case _ => Nil
    }
  }
}
