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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Utils
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashSet

case class Project(projectList: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override lazy val resolved: Boolean = {
    val hasSpecialExpressions = projectList.exists ( _.collect {
        case agg: AggregateExpression => agg
        case generator: Generator => generator
        case window: WindowExpression => window
      }.nonEmpty
    )

    !expressions.exists(!_.resolved) && childrenResolved && !hasSpecialExpressions
  }
}

/**
 * Applies a [[Generator]] to a stream of input rows, combining the
 * output of each into a new stream of rows.  This operation is similar to a `flatMap` in functional
 * programming with one important additional feature, which allows the input rows to be joined with
 * their output.
 * @param generator the generator expression
 * @param join  when true, each output row is implicitly joined with the input tuple that produced
 *              it.
 * @param outer when true, each input row will be output at least once, even if the output of the
 *              given `generator` is empty. `outer` has no effect when `join` is false.
 * @param qualifier Qualifier for the attributes of generator(UDTF)
 * @param generatorOutput The output schema of the Generator.
 * @param child Children logical plan node
 */
case class Generate(
    generator: Generator,
    join: Boolean,
    outer: Boolean,
    qualifier: Option[String],
    generatorOutput: Seq[Attribute],
    child: LogicalPlan)
  extends UnaryNode {

  /** The set of all attributes produced by this node. */
  def generatedSet: AttributeSet = AttributeSet(generatorOutput)

  override lazy val resolved: Boolean = {
    generator.resolved &&
      childrenResolved &&
      generator.elementTypes.length == generatorOutput.length &&
      !generatorOutput.exists(!_.resolved)
  }

  // we don't want the gOutput to be taken as part of the expressions
  // as that will cause exceptions like unresolved attributes etc.
  override def expressions: Seq[Expression] = generator :: Nil

  def output: Seq[Attribute] = {
    val qualified = qualifier.map(q =>
      // prepend the new qualifier to the existed one
      generatorOutput.map(a => a.withQualifiers(q +: a.qualifiers))
    ).getOrElse(generatorOutput)

    if (join) child.output ++ qualified else qualified
  }
}

case class Filter(condition: Expression, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

abstract class SetOperation(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
  // TODO: These aren't really the same attributes as nullability etc might change.
  final override def output: Seq[Attribute] = left.output

  final override lazy val resolved: Boolean =
    childrenResolved &&
      left.output.length == right.output.length &&
      left.output.zip(right.output).forall { case (l, r) => l.dataType == r.dataType }
}

private[sql] object SetOperation {
  def unapply(p: SetOperation): Option[(LogicalPlan, LogicalPlan)] = Some((p.left, p.right))
}

case class Union(left: LogicalPlan, right: LogicalPlan) extends SetOperation(left, right) {

  override def statistics: Statistics = {
    val sizeInBytes = left.statistics.sizeInBytes + right.statistics.sizeInBytes
    Statistics(sizeInBytes = sizeInBytes)
  }
}

case class Intersect(left: LogicalPlan, right: LogicalPlan) extends SetOperation(left, right)

case class Except(left: LogicalPlan, right: LogicalPlan) extends SetOperation(left, right)

case class Join(
  left: LogicalPlan,
  right: LogicalPlan,
  joinType: JoinType,
  condition: Option[Expression]) extends BinaryNode {

  override def output: Seq[Attribute] = {
    joinType match {
      case LeftSemi =>
        left.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case _ =>
        left.output ++ right.output
    }
  }

  def selfJoinResolved: Boolean = left.outputSet.intersect(right.outputSet).isEmpty

  // Joins are only resolved if they don't introduce ambiguous expression ids.
  override lazy val resolved: Boolean = {
    childrenResolved &&
      expressions.forall(_.resolved) &&
      selfJoinResolved &&
      condition.forall(_.dataType == BooleanType)
  }
}

/**
 * A hint for the optimizer that we should broadcast the `child` if used in a join operator.
 */
case class BroadcastHint(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class InsertIntoTable(
    table: LogicalPlan,
    partition: Map[String, Option[String]],
    child: LogicalPlan,
    overwrite: Boolean,
    ifNotExists: Boolean)
  extends LogicalPlan {

  override def children: Seq[LogicalPlan] = child :: Nil
  override def output: Seq[Attribute] = Seq.empty

  assert(overwrite || !ifNotExists)
  override lazy val resolved: Boolean = childrenResolved && child.output.zip(table.output).forall {
    case (childAttr, tableAttr) =>
      DataType.equalsIgnoreCompatibleNullability(childAttr.dataType, tableAttr.dataType)
  }
}

/**
 * A container for holding named common table expressions (CTEs) and a query plan.
 * This operator will be removed during analysis and the relations will be substituted into child.
 * @param child The final query of this CTE.
 * @param cteRelations Queries that this CTE defined,
 *                     key is the alias of the CTE definition,
 *                     value is the CTE definition.
 */
case class With(child: LogicalPlan, cteRelations: Map[String, Subquery]) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class WithWindowDefinition(
    windowDefinitions: Map[String, WindowSpecDefinition],
    child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

/**
 * @param order  The ordering expressions
 * @param global True means global sorting apply for entire data set,
 *               False means sorting only apply within the partition.
 * @param child  Child logical plan
 */
case class Sort(
    order: Seq[SortOrder],
    global: Boolean,
    child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class Aggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: LogicalPlan)
  extends UnaryNode {

  override lazy val resolved: Boolean = {
    val hasWindowExpressions = aggregateExpressions.exists ( _.collect {
        case window: WindowExpression => window
      }.nonEmpty
    )

    !expressions.exists(!_.resolved) && childrenResolved && !hasWindowExpressions
  }

  lazy val newAggregation: Option[Aggregate] = Utils.tryConvert(this)

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)
}

case class Window(
    projectList: Seq[Attribute],
    windowExpressions: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] =
    projectList ++ windowExpressions.map(_.toAttribute)
}

/**
 * Apply the all of the GroupExpressions to every input row, hence we will get
 * multiple output rows for a input row.
 * @param bitmasks The bitmask set represents the grouping sets
 * @param groupByExprs The grouping by expressions
 * @param child       Child operator
 */
case class Expand(
    bitmasks: Seq[Int],
    groupByExprs: Seq[Expression],
    gid: Attribute,
    child: LogicalPlan) extends UnaryNode {
  override def statistics: Statistics = {
    val sizeInBytes = child.statistics.sizeInBytes * projections.length
    Statistics(sizeInBytes = sizeInBytes)
  }

  val projections: Seq[Seq[Expression]] = expand()

  /**
   * Extract attribute set according to the grouping id
   * @param bitmask bitmask to represent the selected of the attribute sequence
   * @param exprs the attributes in sequence
   * @return the attributes of non selected specified via bitmask (with the bit set to 1)
   */
  private def buildNonSelectExprSet(bitmask: Int, exprs: Seq[Expression])
  : OpenHashSet[Expression] = {
    val set = new OpenHashSet[Expression](2)

    var bit = exprs.length - 1
    while (bit >= 0) {
      if (((bitmask >> bit) & 1) == 0) set.add(exprs(bit))
      bit -= 1
    }

    set
  }

  /**
   * Create an array of Projections for the child projection, and replace the projections'
   * expressions which equal GroupBy expressions with Literal(null), if those expressions
   * are not set for this grouping set (according to the bit mask).
   */
  private[this] def expand(): Seq[Seq[Expression]] = {
    val result = new scala.collection.mutable.ArrayBuffer[Seq[Expression]]

    bitmasks.foreach { bitmask =>
      // get the non selected grouping attributes according to the bit mask
      val nonSelectedGroupExprSet = buildNonSelectExprSet(bitmask, groupByExprs)

      val substitution = (child.output :+ gid).map(expr => expr transformDown {
        case x: Expression if nonSelectedGroupExprSet.contains(x) =>
          // if the input attribute in the Invalid Grouping Expression set of for this group
          // replace it with constant null
          Literal.create(null, expr.dataType)
        case x if x == gid =>
          // replace the groupingId with concrete value (the bit mask)
          Literal.create(bitmask, IntegerType)
      })

      result += substitution
    }

    result.toSeq
  }

  override def output: Seq[Attribute] = {
    child.output :+ gid
  }
}

trait GroupingAnalytics extends UnaryNode {

  def groupByExprs: Seq[Expression]
  def aggregations: Seq[NamedExpression]

  override def output: Seq[Attribute] = aggregations.map(_.toAttribute)

  def withNewAggs(aggs: Seq[NamedExpression]): GroupingAnalytics
}

/**
 * A GROUP BY clause with GROUPING SETS can generate a result set equivalent
 * to generated by a UNION ALL of multiple simple GROUP BY clauses.
 *
 * We will transform GROUPING SETS into logical plan Aggregate(.., Expand) in Analyzer
 * @param bitmasks     A list of bitmasks, each of the bitmask indicates the selected
 *                     GroupBy expressions
 * @param groupByExprs The Group By expressions candidates, take effective only if the
 *                     associated bit in the bitmask set to 1.
 * @param child        Child operator
 * @param aggregations The Aggregation expressions, those non selected group by expressions
 *                     will be considered as constant null if it appears in the expressions
 */
case class GroupingSets(
    bitmasks: Seq[Int],
    groupByExprs: Seq[Expression],
    child: LogicalPlan,
    aggregations: Seq[NamedExpression]) extends GroupingAnalytics {

  def withNewAggs(aggs: Seq[NamedExpression]): GroupingAnalytics =
    this.copy(aggregations = aggs)
}

/**
 * Cube is a syntactic sugar for GROUPING SETS, and will be transformed to GroupingSets,
 * and eventually will be transformed to Aggregate(.., Expand) in Analyzer
 *
 * @param groupByExprs The Group By expressions candidates.
 * @param child        Child operator
 * @param aggregations The Aggregation expressions, those non selected group by expressions
 *                     will be considered as constant null if it appears in the expressions
 */
case class Cube(
    groupByExprs: Seq[Expression],
    child: LogicalPlan,
    aggregations: Seq[NamedExpression]) extends GroupingAnalytics {

  def withNewAggs(aggs: Seq[NamedExpression]): GroupingAnalytics =
    this.copy(aggregations = aggs)
}

/**
 * Rollup is a syntactic sugar for GROUPING SETS, and will be transformed to GroupingSets,
 * and eventually will be transformed to Aggregate(.., Expand) in Analyzer
 *
 * @param groupByExprs The Group By expressions candidates, take effective only if the
 *                     associated bit in the bitmask set to 1.
 * @param child        Child operator
 * @param aggregations The Aggregation expressions, those non selected group by expressions
 *                     will be considered as constant null if it appears in the expressions
 */
case class Rollup(
    groupByExprs: Seq[Expression],
    child: LogicalPlan,
    aggregations: Seq[NamedExpression]) extends GroupingAnalytics {

  def withNewAggs(aggs: Seq[NamedExpression]): GroupingAnalytics =
    this.copy(aggregations = aggs)
}

case class Limit(limitExpr: Expression, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override lazy val statistics: Statistics = {
    val limit = limitExpr.eval().asInstanceOf[Int]
    val sizeInBytes = (limit: Long) * output.map(a => a.dataType.defaultSize).sum
    Statistics(sizeInBytes = sizeInBytes)
  }
}

case class Subquery(alias: String, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output.map(_.withQualifiers(alias :: Nil))
}

/**
 * Sample the dataset.
 *
 * @param lowerBound Lower-bound of the sampling probability (usually 0.0)
 * @param upperBound Upper-bound of the sampling probability. The expected fraction sampled
 *                   will be ub - lb.
 * @param withReplacement Whether to sample with replacement.
 * @param seed the random seed
 * @param child the LogicalPlan
 */
case class Sample(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long,
    child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output
}

/**
 * Returns a new logical plan that dedups input rows.
 */
case class Distinct(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

/**
 * Return a new RDD that has exactly `numPartitions` partitions. Differs from
 * [[RepartitionByExpression]] as this method is called directly by DataFrame's, because the user
 * asked for `coalesce` or `repartition`. [[RepartitionByExpression]] is used when the consumer
 * of the output requires some specific ordering or distribution of the data.
 */
case class Repartition(numPartitions: Int, shuffle: Boolean, child: LogicalPlan)
  extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

/**
 * A relation with one row. This is used in "SELECT ..." without a from clause.
 */
case object OneRowRelation extends LeafNode {
  override def output: Seq[Attribute] = Nil

  /**
   * Computes [[Statistics]] for this plan. The default implementation assumes the output
   * cardinality is the product of of all child plan's cardinality, i.e. applies in the case
   * of cartesian joins.
   *
   * [[LeafNode]]s must override this.
   */
  override def statistics: Statistics = Statistics(sizeInBytes = 1)
}

