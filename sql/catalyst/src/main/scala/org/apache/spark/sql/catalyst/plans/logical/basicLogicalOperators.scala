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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.types._

/**
 * When planning take() or collect() operations, this special node that is inserted at the top of
 * the logical plan before invoking the query planner.
 *
 * Rules can pattern-match on this node in order to apply transformations that only take effect
 * at the top of the logical query plan.
 */
case class ReturnAnswer(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class Project(projectList: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
  override def maxRows: Option[Long] = child.maxRows

  override lazy val resolved: Boolean = {
    val hasSpecialExpressions = projectList.exists ( _.collect {
        case agg: AggregateExpression => agg
        case generator: Generator => generator
        case window: WindowExpression => window
      }.nonEmpty
    )

    !expressions.exists(!_.resolved) && childrenResolved && !hasSpecialExpressions
  }

  override def validConstraints: Set[Expression] =
    child.constraints.union(getAliasedConstraints(projectList))
}

/**
 * Applies a [[Generator]] to a stream of input rows, combining the
 * output of each into a new stream of rows.  This operation is similar to a `flatMap` in functional
 * programming with one important additional feature, which allows the input rows to be joined with
 * their output.
 *
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
      generator.elementSchema.length == generatorOutput.length &&
      generatorOutput.forall(_.resolved)
  }

  override def producedAttributes: AttributeSet = AttributeSet(generatorOutput)

  def output: Seq[Attribute] = {
    val qualified = qualifier.map(q =>
      // prepend the new qualifier to the existed one
      generatorOutput.map(a => a.withQualifier(Some(q)))
    ).getOrElse(generatorOutput)

    if (join) child.output ++ qualified else qualified
  }
}

case class Filter(condition: Expression, child: LogicalPlan)
  extends UnaryNode with PredicateHelper {
  override def output: Seq[Attribute] = child.output

  override def maxRows: Option[Long] = child.maxRows

  override protected def validConstraints: Set[Expression] = {
    val predicates = splitConjunctivePredicates(condition)
      .filterNot(SubqueryExpression.hasCorrelatedSubquery)
    child.constraints.union(predicates.toSet)
  }
}

/** Factory for constructing new `Scanner` nodes. */
object Scanner extends PredicateHelper {
  def apply(child: LogicalPlan): Scanner = {
    Scanner(child.output, Nil, child)
  }

  def apply(condition: Expression, child: LogicalPlan): Scanner = {
    Scanner(child.output, splitConjunctivePredicates(condition), child)
  }
}

case class Scanner(projectList: Seq[NamedExpression], filters: Seq[Expression], child: LogicalPlan)
  extends UnaryNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
  override def maxRows: Option[Long] = child.maxRows

  override lazy val resolved: Boolean = {
    val hasSpecialExpressions = projectList.exists ( _.collect {
      case agg: AggregateExpression => agg
      case generator: Generator => generator
      case window: WindowExpression => window
    }.nonEmpty
    )

    !expressions.exists(!_.resolved) && childrenResolved && !hasSpecialExpressions
  }

  override def validConstraints: Set[Expression] =
    child.constraints
      .union(getAliasedConstraints(projectList))
      .union(filters.filterNot(SubqueryExpression.hasCorrelatedSubquery).toSet)
}

abstract class SetOperation(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {

  protected def leftConstraints: Set[Expression] = left.constraints

  protected def rightConstraints: Set[Expression] = {
    require(left.output.size == right.output.size)
    val attributeRewrites = AttributeMap(right.output.zip(left.output))
    right.constraints.map(_ transform {
      case a: Attribute => attributeRewrites(a)
    })
  }
}

object SetOperation {
  def unapply(p: SetOperation): Option[(LogicalPlan, LogicalPlan)] = Some((p.left, p.right))
}

case class Intersect(left: LogicalPlan, right: LogicalPlan) extends SetOperation(left, right) {

  def duplicateResolved: Boolean = left.outputSet.intersect(right.outputSet).isEmpty

  override def output: Seq[Attribute] =
    left.output.zip(right.output).map { case (leftAttr, rightAttr) =>
      leftAttr.withNullability(leftAttr.nullable && rightAttr.nullable)
    }

  override protected def validConstraints: Set[Expression] =
    leftConstraints.union(rightConstraints)

  // Intersect are only resolved if they don't introduce ambiguous expression ids,
  // since the Optimizer will convert Intersect to Join.
  override lazy val resolved: Boolean =
    childrenResolved &&
      left.output.length == right.output.length &&
      left.output.zip(right.output).forall { case (l, r) => l.dataType == r.dataType } &&
      duplicateResolved

  override def maxRows: Option[Long] = {
    if (children.exists(_.maxRows.isEmpty)) {
      None
    } else {
      Some(children.flatMap(_.maxRows).min)
    }
  }

  override lazy val statistics: Statistics = {
    val leftSize = left.statistics.sizeInBytes
    val rightSize = right.statistics.sizeInBytes
    val sizeInBytes = if (leftSize < rightSize) leftSize else rightSize
    val isBroadcastable = left.statistics.isBroadcastable || right.statistics.isBroadcastable

    Statistics(sizeInBytes = sizeInBytes, isBroadcastable = isBroadcastable)
  }
}

case class Except(left: LogicalPlan, right: LogicalPlan) extends SetOperation(left, right) {

  def duplicateResolved: Boolean = left.outputSet.intersect(right.outputSet).isEmpty

  /** We don't use right.output because those rows get excluded from the set. */
  override def output: Seq[Attribute] = left.output

  override protected def validConstraints: Set[Expression] = leftConstraints

  override lazy val resolved: Boolean =
    childrenResolved &&
      left.output.length == right.output.length &&
      left.output.zip(right.output).forall { case (l, r) => l.dataType == r.dataType } &&
      duplicateResolved

  override lazy val statistics: Statistics = {
    left.statistics.copy()
  }
}

/** Factory for constructing new `Union` nodes. */
object Union {
  def apply(left: LogicalPlan, right: LogicalPlan): Union = {
    Union (left :: right :: Nil)
  }
}

case class Union(children: Seq[LogicalPlan]) extends LogicalPlan {
  override def maxRows: Option[Long] = {
    if (children.exists(_.maxRows.isEmpty)) {
      None
    } else {
      Some(children.flatMap(_.maxRows).sum)
    }
  }

  // updating nullability to make all the children consistent
  override def output: Seq[Attribute] =
    children.map(_.output).transpose.map(attrs =>
      attrs.head.withNullability(attrs.exists(_.nullable)))

  override lazy val resolved: Boolean = {
    // allChildrenCompatible needs to be evaluated after childrenResolved
    def allChildrenCompatible: Boolean =
      children.tail.forall( child =>
        // compare the attribute number with the first child
        child.output.length == children.head.output.length &&
        // compare the data types with the first child
        child.output.zip(children.head.output).forall {
          case (l, r) => l.dataType == r.dataType }
      )

    children.length > 1 && childrenResolved && allChildrenCompatible
  }

  override lazy val statistics: Statistics = {
    val sizeInBytes = children.map(_.statistics.sizeInBytes).sum
    Statistics(sizeInBytes = sizeInBytes)
  }

  /**
   * Maps the constraints containing a given (original) sequence of attributes to those with a
   * given (reference) sequence of attributes. Given the nature of union, we expect that the
   * mapping between the original and reference sequences are symmetric.
   */
  private def rewriteConstraints(
      reference: Seq[Attribute],
      original: Seq[Attribute],
      constraints: Set[Expression]): Set[Expression] = {
    require(reference.size == original.size)
    val attributeRewrites = AttributeMap(original.zip(reference))
    constraints.map(_ transform {
      case a: Attribute => attributeRewrites(a)
    })
  }

  private def merge(a: Set[Expression], b: Set[Expression]): Set[Expression] = {
    val common = a.intersect(b)
    // The constraint with only one reference could be easily inferred as predicate
    // Grouping the constraints by it's references so we can combine the constraints with same
    // reference together
    val othera = a.diff(common).filter(_.references.size == 1).groupBy(_.references.head)
    val otherb = b.diff(common).filter(_.references.size == 1).groupBy(_.references.head)
    // loose the constraints by: A1 && B1 || A2 && B2  ->  (A1 || A2) && (B1 || B2)
    val others = (othera.keySet intersect otherb.keySet).map { attr =>
      Or(othera(attr).reduceLeft(And), otherb(attr).reduceLeft(And))
    }
    common ++ others
  }

  override protected def validConstraints: Set[Expression] = {
    children
      .map(child => rewriteConstraints(children.head.output, child.output, child.constraints))
      .reduce(merge(_, _))
  }
}

case class Join(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    condition: Option[Expression])
  extends BinaryNode with PredicateHelper {

  override def output: Seq[Attribute] = {
    joinType match {
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
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

  override protected def validConstraints: Set[Expression] = {
    joinType match {
      case _: InnerLike if condition.isDefined =>
        left.constraints
          .union(right.constraints)
          .union(splitConjunctivePredicates(condition.get).toSet)
      case LeftSemi if condition.isDefined =>
        left.constraints
          .union(splitConjunctivePredicates(condition.get).toSet)
      case j: ExistenceJoin =>
        left.constraints
      case _: InnerLike =>
        left.constraints.union(right.constraints)
      case LeftExistence(_) =>
        left.constraints
      case LeftOuter =>
        left.constraints
      case RightOuter =>
        right.constraints
      case FullOuter =>
        Set.empty[Expression]
    }
  }

  def duplicateResolved: Boolean = left.outputSet.intersect(right.outputSet).isEmpty

  // Joins are only resolved if they don't introduce ambiguous expression ids.
  // NaturalJoin should be ready for resolution only if everything else is resolved here
  lazy val resolvedExceptNatural: Boolean = {
    childrenResolved &&
      expressions.forall(_.resolved) &&
      duplicateResolved &&
      condition.forall(_.dataType == BooleanType)
  }

  // if not a natural join, use `resolvedExceptNatural`. if it is a natural join or
  // using join, we still need to eliminate natural or using before we mark it resolved.
  override lazy val resolved: Boolean = joinType match {
    case NaturalJoin(_) => false
    case UsingJoin(_, _) => false
    case _ => resolvedExceptNatural
  }

  override lazy val statistics: Statistics = joinType match {
    case LeftAnti | LeftSemi =>
      // LeftSemi and LeftAnti won't ever be bigger than left
      left.statistics.copy()
    case _ =>
      // make sure we don't propagate isBroadcastable in other joins, because
      // they could explode the size.
      super.statistics.copy(isBroadcastable = false)
  }
}

/**
 * A hint for the optimizer that we should broadcast the `child` if used in a join operator.
 */
case class BroadcastHint(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  // set isBroadcastable to true so the child will be broadcasted
  override lazy val statistics: Statistics = super.statistics.copy(isBroadcastable = true)
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

  lazy val expectedColumns = {
    if (table.output.isEmpty) {
      None
    } else {
      // Note: The parser (visitPartitionSpec in AstBuilder) already turns
      // keys in partition to their lowercase forms.
      val staticPartCols = partition.filter(_._2.isDefined).keySet
      Some(table.output.filterNot(a => staticPartCols.contains(a.name)))
    }
  }

  assert(overwrite || !ifNotExists)
  assert(partition.values.forall(_.nonEmpty) || !ifNotExists)
  override lazy val resolved: Boolean =
    childrenResolved && table.resolved && expectedColumns.forall { expected =>
    child.output.size == expected.size && child.output.zip(expected).forall {
      case (childAttr, tableAttr) =>
        DataType.equalsIgnoreCompatibleNullability(childAttr.dataType, tableAttr.dataType)
    }
  }
}

/**
 * A container for holding named common table expressions (CTEs) and a query plan.
 * This operator will be removed during analysis and the relations will be substituted into child.
 *
 * @param child The final query of this CTE.
 * @param cteRelations A sequence of pair (alias, the CTE definition) that this CTE defined
 *                     Each CTE can see the base tables and the previously defined CTEs only.
 */
case class With(child: LogicalPlan, cteRelations: Seq[(String, SubqueryAlias)]) extends UnaryNode {
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
  override def maxRows: Option[Long] = child.maxRows
}

/** Factory for constructing new `Range` nodes. */
object Range {
  def apply(start: Long, end: Long, step: Long, numSlices: Option[Int]): Range = {
    val output = StructType(StructField("id", LongType, nullable = false) :: Nil).toAttributes
    new Range(start, end, step, numSlices, output)
  }
  def apply(start: Long, end: Long, step: Long, numSlices: Int): Range = {
    Range(start, end, step, Some(numSlices))
  }
}

case class Range(
    start: Long,
    end: Long,
    step: Long,
    numSlices: Option[Int],
    output: Seq[Attribute])
  extends LeafNode with MultiInstanceRelation {

  require(step != 0, s"step ($step) cannot be 0")

  val numElements: BigInt = {
    val safeStart = BigInt(start)
    val safeEnd = BigInt(end)
    if ((safeEnd - safeStart) % step == 0 || (safeEnd > safeStart) != (step > 0)) {
      (safeEnd - safeStart) / step
    } else {
      // the remainder has the same sign with range, could add 1 more
      (safeEnd - safeStart) / step + 1
    }
  }

  def toSQL(): String = {
    if (numSlices.isDefined) {
      s"SELECT id AS `${output.head.name}` FROM range($start, $end, $step, ${numSlices.get})"
    } else {
      s"SELECT id AS `${output.head.name}` FROM range($start, $end, $step)"
    }
  }

  override def newInstance(): Range = copy(output = output.map(_.newInstance()))

  override lazy val statistics: Statistics = {
    val sizeInBytes = LongType.defaultSize * numElements
    Statistics( sizeInBytes = sizeInBytes )
  }

  override def simpleString: String = {
    s"Range ($start, $end, step=$step, splits=$numSlices)"
  }
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

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)
  override def maxRows: Option[Long] = child.maxRows

  override def validConstraints: Set[Expression] = {
    val nonAgg = aggregateExpressions.filter(_.find(_.isInstanceOf[AggregateExpression]).isEmpty)
    child.constraints.union(getAliasedConstraints(nonAgg))
  }

  override lazy val statistics: Statistics = {
    if (groupingExpressions.isEmpty) {
      super.statistics.copy(sizeInBytes = 1)
    } else {
      super.statistics
    }
  }
}

case class Window(
    windowExpressions: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] =
    child.output ++ windowExpressions.map(_.toAttribute)

  def windowOutputSet: AttributeSet = AttributeSet(windowExpressions.map(_.toAttribute))
}

object Expand {
  /**
   * Extract attribute set according to the grouping id.
   *
   * @param bitmask bitmask to represent the selected of the attribute sequence
   * @param attrs the attributes in sequence
   * @return the attributes of non selected specified via bitmask (with the bit set to 1)
   */
  private def buildNonSelectAttrSet(
      bitmask: Int,
      attrs: Seq[Attribute]): AttributeSet = {
    val nonSelect = new ArrayBuffer[Attribute]()

    var bit = attrs.length - 1
    while (bit >= 0) {
      if (((bitmask >> bit) & 1) == 1) nonSelect += attrs(attrs.length - bit - 1)
      bit -= 1
    }

    AttributeSet(nonSelect)
  }

  /**
   * Apply the all of the GroupExpressions to every input row, hence we will get
   * multiple output rows for an input row.
   *
   * @param bitmasks The bitmask set represents the grouping sets
   * @param groupByAliases The aliased original group by expressions
   * @param groupByAttrs The attributes of aliased group by expressions
   * @param gid Attribute of the grouping id
   * @param child Child operator
   */
  def apply(
    bitmasks: Seq[Int],
    groupByAliases: Seq[Alias],
    groupByAttrs: Seq[Attribute],
    gid: Attribute,
    child: LogicalPlan): Expand = {
    // Create an array of Projections for the child projection, and replace the projections'
    // expressions which equal GroupBy expressions with Literal(null), if those expressions
    // are not set for this grouping set (according to the bit mask).
    val projections = bitmasks.map { bitmask =>
      // get the non selected grouping attributes according to the bit mask
      val nonSelectedGroupAttrSet = buildNonSelectAttrSet(bitmask, groupByAttrs)

      child.output ++ groupByAttrs.map { attr =>
        if (nonSelectedGroupAttrSet.contains(attr)) {
          // if the input attribute in the Invalid Grouping Expression set of for this group
          // replace it with constant null
          Literal.create(null, attr.dataType)
        } else {
          attr
        }
      // groupingId is the last output, here we use the bit mask as the concrete value for it.
      } :+ Literal.create(bitmask, IntegerType)
    }

    // the `groupByAttrs` has different meaning in `Expand.output`, it could be the original
    // grouping expression or null, so here we create new instance of it.
    val output = child.output ++ groupByAttrs.map(_.newInstance) :+ gid
    Expand(projections, output, Project(child.output ++ groupByAliases, child))
  }
}

/**
 * Apply a number of projections to every input row, hence we will get multiple output rows for
 * an input row.
 *
 * @param projections to apply
 * @param output of all projections.
 * @param child operator.
 */
case class Expand(
    projections: Seq[Seq[Expression]],
    output: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode {
  override def references: AttributeSet =
    AttributeSet(projections.flatten.flatMap(_.references))

  override lazy val statistics: Statistics = {
    val sizeInBytes = super.statistics.sizeInBytes * projections.length
    Statistics(sizeInBytes = sizeInBytes)
  }

  // This operator can reuse attributes (for example making them null when doing a roll up) so
  // the constraints of the child may no longer be valid.
  override protected def validConstraints: Set[Expression] = Set.empty[Expression]
}

/**
 * A GROUP BY clause with GROUPING SETS can generate a result set equivalent
 * to generated by a UNION ALL of multiple simple GROUP BY clauses.
 *
 * We will transform GROUPING SETS into logical plan Aggregate(.., Expand) in Analyzer
 *
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
    aggregations: Seq[NamedExpression]) extends UnaryNode {

  override def output: Seq[Attribute] = aggregations.map(_.toAttribute)

  // Needs to be unresolved before its translated to Aggregate + Expand because output attributes
  // will change in analysis.
  override lazy val resolved: Boolean = false
}

case class Pivot(
    groupByExprs: Seq[NamedExpression],
    pivotColumn: Expression,
    pivotValues: Seq[Literal],
    aggregates: Seq[Expression],
    child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = groupByExprs.map(_.toAttribute) ++ aggregates match {
    case agg :: Nil => pivotValues.map(value => AttributeReference(value.toString, agg.dataType)())
    case _ => pivotValues.flatMap{ value =>
      aggregates.map(agg => AttributeReference(value + "_" + agg.sql, agg.dataType)())
    }
  }
}

object Limit {
  def apply(limitExpr: Expression, child: LogicalPlan): UnaryNode = {
    GlobalLimit(limitExpr, LocalLimit(limitExpr, child))
  }

  def unapply(p: GlobalLimit): Option[(Expression, LogicalPlan)] = {
    p match {
      case GlobalLimit(le1, LocalLimit(le2, child)) if le1 == le2 => Some((le1, child))
      case _ => None
    }
  }
}

case class GlobalLimit(limitExpr: Expression, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def maxRows: Option[Long] = {
    limitExpr match {
      case IntegerLiteral(limit) => Some(limit)
      case _ => None
    }
  }
  override lazy val statistics: Statistics = {
    val limit = limitExpr.eval().asInstanceOf[Int]
    val sizeInBytes = if (limit == 0) {
      // sizeInBytes can't be zero, or sizeInBytes of BinaryNode will also be zero
      // (product of children).
      1
    } else {
      (limit: Long) * output.map(a => a.dataType.defaultSize).sum
    }
    child.statistics.copy(sizeInBytes = sizeInBytes)
  }
}

case class LocalLimit(limitExpr: Expression, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def maxRows: Option[Long] = {
    limitExpr match {
      case IntegerLiteral(limit) => Some(limit)
      case _ => None
    }
  }
  override lazy val statistics: Statistics = {
    val limit = limitExpr.eval().asInstanceOf[Int]
    val sizeInBytes = if (limit == 0) {
      // sizeInBytes can't be zero, or sizeInBytes of BinaryNode will also be zero
      // (product of children).
      1
    } else {
      (limit: Long) * output.map(a => a.dataType.defaultSize).sum
    }
    child.statistics.copy(sizeInBytes = sizeInBytes)
  }
}

case class SubqueryAlias(
    alias: String,
    child: LogicalPlan,
    view: Option[TableIdentifier])
  extends UnaryNode {

  override def output: Seq[Attribute] = child.output.map(_.withQualifier(Some(alias)))
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
 * @param isTableSample Is created from TABLESAMPLE in the parser.
 */
case class Sample(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long,
    child: LogicalPlan)(
    val isTableSample: java.lang.Boolean = false) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override lazy val statistics: Statistics = {
    val ratio = upperBound - lowerBound
    // BigInt can't multiply with Double
    var sizeInBytes = child.statistics.sizeInBytes * (ratio * 100).toInt / 100
    if (sizeInBytes == 0) {
      sizeInBytes = 1
    }
    child.statistics.copy(sizeInBytes = sizeInBytes)
  }

  override protected def otherCopyArgs: Seq[AnyRef] = isTableSample :: Nil
}

/**
 * Returns a new logical plan that dedups input rows.
 */
case class Distinct(child: LogicalPlan) extends UnaryNode {
  override def maxRows: Option[Long] = child.maxRows
  override def output: Seq[Attribute] = child.output
}

/**
 * Returns a new RDD that has exactly `numPartitions` partitions. Differs from
 * [[RepartitionByExpression]] as this method is called directly by DataFrame's, because the user
 * asked for `coalesce` or `repartition`. [[RepartitionByExpression]] is used when the consumer
 * of the output requires some specific ordering or distribution of the data.
 */
case class Repartition(numPartitions: Int, shuffle: Boolean, child: LogicalPlan)
  extends UnaryNode {
  require(numPartitions > 0, s"Number of partitions ($numPartitions) must be positive.")
  override def output: Seq[Attribute] = child.output
}

/**
 * A relation with one row. This is used in "SELECT ..." without a from clause.
 */
case object OneRowRelation extends LeafNode {
  override def maxRows: Option[Long] = Some(1)
  override def output: Seq[Attribute] = Nil

  /**
   * Computes [[Statistics]] for this plan. The default implementation assumes the output
   * cardinality is the product of of all child plan's cardinality, i.e. applies in the case
   * of cartesian joins.
   *
   * [[LeafNode]]s must override this.
   */
  override lazy val statistics: Statistics = Statistics(sizeInBytes = 1)
}
