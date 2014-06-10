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

import org.apache.spark.sql.{SQLConf, SQLContext, execution}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.parquet._

private[sql] abstract class SparkStrategies extends QueryPlanner[SparkPlan] {
  self: SQLContext#SparkPlanner =>

  object LeftSemiJoin extends Strategy with PredicateHelper {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      // Find left semi joins where at least some predicates can be evaluated by matching hash
      // keys using the HashFilteredJoin pattern.
      case HashFilteredJoin(LeftSemi, leftKeys, rightKeys, condition, left, right) =>
        val semiJoin = execution.LeftSemiJoinHash(
          leftKeys, rightKeys, planLater(left), planLater(right))
        condition.map(Filter(_, semiJoin)).getOrElse(semiJoin) :: Nil
      // no predicate can be evaluated by matching hash keys
      case logical.Join(left, right, LeftSemi, condition) =>
        execution.LeftSemiJoinBNL(
          planLater(left), planLater(right), condition)(sparkContext) :: Nil
      case _ => Nil
    }
  }

  object HashJoin extends Strategy with PredicateHelper {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      // Find inner joins where at least some predicates can be evaluated by matching hash keys
      // using the HashFilteredJoin pattern.
      case HashFilteredJoin(Inner, leftKeys, rightKeys, condition, left, right) =>
        val hashJoin =
          execution.HashJoin(leftKeys, rightKeys, BuildRight, planLater(left), planLater(right))
        condition.map(Filter(_, hashJoin)).getOrElse(hashJoin) :: Nil
      case _ => Nil
    }
  }

  object PartialAggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Aggregate(groupingExpressions, aggregateExpressions, child) =>
        // Collect all aggregate expressions.
        val allAggregates =
          aggregateExpressions.flatMap(_ collect { case a: AggregateExpression => a})
        // Collect all aggregate expressions that can be computed partially.
        val partialAggregates =
          aggregateExpressions.flatMap(_ collect { case p: PartialAggregate => p})

        // Only do partial aggregation if supported by all aggregate expressions.
        if (allAggregates.size == partialAggregates.size) {
          // Create a map of expressions to their partial evaluations for all aggregate expressions.
          val partialEvaluations: Map[Long, SplitEvaluation] =
            partialAggregates.map(a => (a.id, a.asPartial)).toMap

          // We need to pass all grouping expressions though so the grouping can happen a second
          // time. However some of them might be unnamed so we alias them allowing them to be
          // referenced in the second aggregation.
          val namedGroupingExpressions: Map[Expression, NamedExpression] = groupingExpressions.map {
            case n: NamedExpression => (n, n)
            case other => (other, Alias(other, "PartialGroup")())
          }.toMap

          // Replace aggregations with a new expression that computes the result from the already
          // computed partial evaluations and grouping values.
          val rewrittenAggregateExpressions = aggregateExpressions.map(_.transformUp {
            case e: Expression if partialEvaluations.contains(e.id) =>
              partialEvaluations(e.id).finalEvaluation
            case e: Expression if namedGroupingExpressions.contains(e) =>
              namedGroupingExpressions(e).toAttribute
          }).asInstanceOf[Seq[NamedExpression]]

          val partialComputation =
            (namedGroupingExpressions.values ++
             partialEvaluations.values.flatMap(_.partialEvaluations)).toSeq

          // Construct two phased aggregation.
          execution.Aggregate(
            partial = false,
            namedGroupingExpressions.values.map(_.toAttribute).toSeq,
            rewrittenAggregateExpressions,
            execution.Aggregate(
              partial = true,
              groupingExpressions,
              partialComputation,
              planLater(child))(sparkContext))(sparkContext) :: Nil
        } else {
          Nil
        }
      case _ => Nil
    }
  }

  object BroadcastNestedLoopJoin extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Join(left, right, joinType, condition) =>
        execution.BroadcastNestedLoopJoin(
          planLater(left), planLater(right), joinType, condition)(sparkContext) :: Nil
      case _ => Nil
    }
  }

  object CartesianProduct extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Join(left, right, _, None) =>
        execution.CartesianProduct(planLater(left), planLater(right)) :: Nil
      case logical.Join(left, right, Inner, Some(condition)) =>
        execution.Filter(condition,
          execution.CartesianProduct(planLater(left), planLater(right))) :: Nil
      case _ => Nil
    }
  }

  protected lazy val singleRowRdd =
    sparkContext.parallelize(Seq(new GenericRow(Array[Any]()): Row), 1)

  def convertToCatalyst(a: Any): Any = a match {
    case s: Seq[Any] => s.map(convertToCatalyst)
    case p: Product => new GenericRow(p.productIterator.map(convertToCatalyst).toArray)
    case other => other
  }

  object TakeOrdered extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Limit(IntegerLiteral(limit), logical.Sort(order, child)) =>
        execution.TakeOrdered(limit, order, planLater(child))(sparkContext) :: Nil
      case _ => Nil
    }
  }

  object ParquetOperations extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      // TODO: need to support writing to other types of files.  Unify the below code paths.
      case logical.WriteToFile(path, child) =>
        val relation =
          ParquetRelation.create(path, child, sparkContext.hadoopConfiguration)
        InsertIntoParquetTable(relation, planLater(child), overwrite=true)(sparkContext) :: Nil
      case logical.InsertIntoTable(table: ParquetRelation, partition, child, overwrite) =>
        InsertIntoParquetTable(table, planLater(child), overwrite)(sparkContext) :: Nil
      case PhysicalOperation(projectList, filters: Seq[Expression], relation: ParquetRelation) => {
        val prunePushedDownFilters =
          if (sparkContext.conf.getBoolean(ParquetFilters.PARQUET_FILTER_PUSHDOWN_ENABLED, true)) {
            (filters: Seq[Expression]) => {
              filters.filter { filter =>
                // Note: filters cannot be pushed down to Parquet if they contain more complex
                // expressions than simple "Attribute cmp Literal" comparisons. Here we remove
                // all filters that have been pushed down. Note that a predicate such as
                // "(A AND B) OR C" can result in "A OR C" being pushed down.
                val recordFilter = ParquetFilters.createFilter(filter)
                if (!recordFilter.isDefined) {
                  // First case: the pushdown did not result in any record filter.
                  true
                } else {
                  // Second case: a record filter was created; here we are conservative in
                  // the sense that even if "A" was pushed and we check for "A AND B" we
                  // still want to keep "A AND B" in the higher-level filter, not just "B".
                  !ParquetFilters.findExpression(recordFilter.get, filter).isDefined
                }
              }
            }
          } else {
            identity[Seq[Expression]] _
          }
        pruneFilterProject(
          projectList,
          filters,
          prunePushedDownFilters,
          ParquetTableScan(_, relation, filters)(sparkContext)) :: Nil
      }

      case _ => Nil
    }
  }

  // Can we automate these 'pass through' operations?
  object BasicOperators extends Strategy {
    def numPartitions = self.numPartitions

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Distinct(child) =>
        execution.Aggregate(
          partial = false, child.output, child.output, planLater(child))(sparkContext) :: Nil
      case logical.Sort(sortExprs, child) =>
        // This sort is a global sort. Its requiredDistribution will be an OrderedDistribution.
        execution.Sort(sortExprs, global = true, planLater(child)):: Nil
      case logical.SortPartitions(sortExprs, child) =>
        // This sort only sorts tuples within a partition. Its requiredDistribution will be
        // an UnspecifiedDistribution.
        execution.Sort(sortExprs, global = false, planLater(child)) :: Nil
      case logical.Project(projectList, child) =>
        execution.Project(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        execution.Filter(condition, planLater(child)) :: Nil
      case logical.Aggregate(group, agg, child) =>
        execution.Aggregate(partial = false, group, agg, planLater(child))(sparkContext) :: Nil
      case logical.Sample(fraction, withReplacement, seed, child) =>
        execution.Sample(fraction, withReplacement, seed, planLater(child)) :: Nil
      case logical.LocalRelation(output, data) =>
        val dataAsRdd =
          sparkContext.parallelize(data.map(r =>
            new GenericRow(r.productIterator.map(convertToCatalyst).toArray): Row))
        execution.ExistingRdd(output, dataAsRdd) :: Nil
      case logical.Limit(IntegerLiteral(limit), child) =>
        execution.Limit(limit, planLater(child))(sparkContext) :: Nil
      case Unions(unionChildren) =>
        execution.Union(unionChildren.map(planLater))(sparkContext) :: Nil
      case logical.Generate(generator, join, outer, _, child) =>
        execution.Generate(generator, join = join, outer = outer, planLater(child)) :: Nil
      case logical.NoRelation =>
        execution.ExistingRdd(Nil, singleRowRdd) :: Nil
      case logical.Repartition(expressions, child) =>
        execution.Exchange(HashPartitioning(expressions, numPartitions), planLater(child)) :: Nil
      case SparkLogicalPlan(existingPlan) => existingPlan :: Nil
      case _ => Nil
    }
  }

  case class CommandStrategy(context: SQLContext) extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.SetCommand(key, value) =>
        Seq(execution.SetCommandPhysical(key, value, plan.output)(context))
      case logical.ExplainCommand(child) =>
        val qe = context.executePlan(child)
        Seq(execution.ExplainCommandPhysical(qe.executedPlan, plan.output)(context))
      case _ => Nil
    }
  }

}
