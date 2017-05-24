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

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

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

/**
 * This node is inserted at the top of a subquery when it is optimized. This makes sure we can
 * recognize a subquery as such, and it allows us to write subquery aware transformations.
 */
case class Subquery(child: LogicalPlan) extends UnaryNode {
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

  override def computeStats(conf: SQLConf): Statistics = {
    if (conf.cboEnabled) {
      ProjectEstimation.estimate(conf, this).getOrElse(super.computeStats(conf))
    } else {
      super.computeStats(conf)
    }
  }
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
 *              given `generator` is empty.
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

  def qualifiedGeneratorOutput: Seq[Attribute] = {
    val qualifiedOutput = qualifier.map { q =>
      // prepend the new qualifier to the existed one
      generatorOutput.map(a => a.withQualifier(Some(q)))
    }.getOrElse(generatorOutput)
    val nullableOutput = qualifiedOutput.map {
      // if outer, make all attributes nullable, otherwise keep existing nullability
      a => a.withNullability(outer || a.nullable)
    }
    nullableOutput
  }

  def output: Seq[Attribute] = {
    if (join) child.output ++ qualifiedGeneratorOutput else qualifiedGeneratorOutput
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

  override def computeStats(conf: SQLConf): Statistics = {
    if (conf.cboEnabled) {
      FilterEstimation(this, conf).estimate.getOrElse(super.computeStats(conf))
    } else {
      super.computeStats(conf)
    }
  }
}

abstract class SetOperation(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {

  def duplicateResolved: Boolean = left.outputSet.intersect(right.outputSet).isEmpty

  protected def leftConstraints: Set[Expression] = left.constraints

  protected def rightConstraints: Set[Expression] = {
    require(left.output.size == right.output.size)
    val attributeRewrites = AttributeMap(right.output.zip(left.output))
    right.constraints.map(_ transform {
      case a: Attribute => attributeRewrites(a)
    })
  }

  override lazy val resolved: Boolean =
    childrenResolved &&
      left.output.length == right.output.length &&
      left.output.zip(right.output).forall { case (l, r) =>
        l.dataType.sameType(r.dataType)
      } && duplicateResolved
}

object SetOperation {
  def unapply(p: SetOperation): Option[(LogicalPlan, LogicalPlan)] = Some((p.left, p.right))
}

case class Intersect(left: LogicalPlan, right: LogicalPlan) extends SetOperation(left, right) {

  override def output: Seq[Attribute] =
    left.output.zip(right.output).map { case (leftAttr, rightAttr) =>
      leftAttr.withNullability(leftAttr.nullable && rightAttr.nullable)
    }

  override protected def validConstraints: Set[Expression] =
    leftConstraints.union(rightConstraints)

  override def maxRows: Option[Long] = {
    if (children.exists(_.maxRows.isEmpty)) {
      None
    } else {
      Some(children.flatMap(_.maxRows).min)
    }
  }

  override def computeStats(conf: SQLConf): Statistics = {
    val leftSize = left.stats(conf).sizeInBytes
    val rightSize = right.stats(conf).sizeInBytes
    val sizeInBytes = if (leftSize < rightSize) leftSize else rightSize
    Statistics(
      sizeInBytes = sizeInBytes,
      hints = left.stats(conf).hints.resetForJoin())
  }
}

case class Except(left: LogicalPlan, right: LogicalPlan) extends SetOperation(left, right) {

  /** We don't use right.output because those rows get excluded from the set. */
  override def output: Seq[Attribute] = left.output

  override protected def validConstraints: Set[Expression] = leftConstraints

  override def computeStats(conf: SQLConf): Statistics = {
    left.stats(conf).copy()
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
          case (l, r) => l.dataType.sameType(r.dataType)
        })
    children.length > 1 && childrenResolved && allChildrenCompatible
  }

  override def computeStats(conf: SQLConf): Statistics = {
    val sizeInBytes = children.map(_.stats(conf).sizeInBytes).sum
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

  override def computeStats(conf: SQLConf): Statistics = {
    def simpleEstimation: Statistics = joinType match {
      case LeftAnti | LeftSemi =>
        // LeftSemi and LeftAnti won't ever be bigger than left
        left.stats(conf)
      case _ =>
        // Make sure we don't propagate isBroadcastable in other joins, because
        // they could explode the size.
        val stats = super.computeStats(conf)
        stats.copy(hints = stats.hints.resetForJoin())
    }

    if (conf.cboEnabled) {
      JoinEstimation.estimate(conf, this).getOrElse(simpleEstimation)
    } else {
      simpleEstimation
    }
  }
}

/**
 * Insert some data into a table. Note that this plan is unresolved and has to be replaced by the
 * concrete implementations during analysis.
 *
 * @param table the logical plan representing the table. In the future this should be a
 *              [[org.apache.spark.sql.catalyst.catalog.CatalogTable]] once we converge Hive tables
 *              and data source tables.
 * @param partition a map from the partition key to the partition value (optional). If the partition
 *                  value is optional, dynamic partition insert will be performed.
 *                  As an example, `INSERT INTO tbl PARTITION (a=1, b=2) AS ...` would have
 *                  Map('a' -> Some('1'), 'b' -> Some('2')),
 *                  and `INSERT INTO tbl PARTITION (a=1, b) AS ...`
 *                  would have Map('a' -> Some('1'), 'b' -> None).
 * @param query the logical plan representing data to write to.
 * @param overwrite overwrite existing table or partitions.
 * @param ifPartitionNotExists If true, only write if the partition does not exist.
 *                             Only valid for static partitions.
 */
case class InsertIntoTable(
    table: LogicalPlan,
    partition: Map[String, Option[String]],
    query: LogicalPlan,
    overwrite: Boolean,
    ifPartitionNotExists: Boolean)
  extends LogicalPlan {
  // IF NOT EXISTS is only valid in INSERT OVERWRITE
  assert(overwrite || !ifPartitionNotExists)
  // IF NOT EXISTS is only valid in static partitions
  assert(partition.values.forall(_.nonEmpty) || !ifPartitionNotExists)

  // We don't want `table` in children as sometimes we don't want to transform it.
  override def children: Seq[LogicalPlan] = query :: Nil
  override def output: Seq[Attribute] = Seq.empty
  override lazy val resolved: Boolean = false
}

/**
 * A container for holding the view description(CatalogTable), and the output of the view. The
 * child should be a logical plan parsed from the `CatalogTable.viewText`, should throw an error
 * if the `viewText` is not defined.
 * This operator will be removed at the end of analysis stage.
 *
 * @param desc A view description(CatalogTable) that provides necessary information to resolve the
 *             view.
 * @param output The output of a view operator, this is generated during planning the view, so that
 *               we are able to decouple the output from the underlying structure.
 * @param child The logical plan of a view operator, it should be a logical plan parsed from the
 *              `CatalogTable.viewText`, should throw an error if the `viewText` is not defined.
 */
case class View(
    desc: CatalogTable,
    output: Seq[Attribute],
    child: LogicalPlan) extends LogicalPlan with MultiInstanceRelation {

  override lazy val resolved: Boolean = child.resolved

  override def children: Seq[LogicalPlan] = child :: Nil

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  override def simpleString: String = {
    s"View (${desc.identifier}, ${output.mkString("[", ",", "]")})"
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

  override def simpleString: String = {
    val cteAliases = Utils.truncatedString(cteRelations.map(_._1), "[", ", ", "]")
    s"CTE $cteAliases"
  }

  override def innerChildren: Seq[LogicalPlan] = cteRelations.map(_._2)
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

  override def computeStats(conf: SQLConf): Statistics = {
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

  override def computeStats(conf: SQLConf): Statistics = {
    def simpleEstimation: Statistics = {
      if (groupingExpressions.isEmpty) {
        Statistics(
          sizeInBytes = EstimationUtils.getOutputSize(output, outputRowCount = 1),
          rowCount = Some(1),
          hints = child.stats(conf).hints)
      } else {
        super.computeStats(conf)
      }
    }

    if (conf.cboEnabled) {
      AggregateEstimation.estimate(conf, this).getOrElse(simpleEstimation)
    } else {
      simpleEstimation
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
   * Build bit mask from attributes of selected grouping set. A bit in the bitmask is corresponding
   * to an attribute in group by attributes sequence, the selected attribute has corresponding bit
   * set to 0 and otherwise set to 1. For example, if we have GroupBy attributes (a, b, c, d), the
   * bitmask 5(whose binary form is 0101) represents grouping set (a, c).
   *
   * @param groupingSetAttrs The attributes of selected grouping set
   * @param attrMap Mapping group by attributes to its index in attributes sequence
   * @return The bitmask which represents the selected attributes out of group by attributes.
   */
  private def buildBitmask(
    groupingSetAttrs: Seq[Attribute],
    attrMap: Map[Attribute, Int]): Int = {
    val numAttributes = attrMap.size
    val mask = (1 << numAttributes) - 1
    // Calculate the attrbute masks of selected grouping set. For example, if we have GroupBy
    // attributes (a, b, c, d), grouping set (a, c) will produce the following sequence:
    // (15, 7, 13), whose binary form is (1111, 0111, 1101)
    val masks = (mask +: groupingSetAttrs.map(attrMap).map(index =>
      // 0 means that the column at the given index is a grouping column, 1 means it is not,
      // so we unset the bit in bitmap.
      ~(1 << (numAttributes - 1 - index))
    ))
    // Reduce masks to generate an bitmask for the selected grouping set.
    masks.reduce(_ & _)
  }

  /**
   * Apply the all of the GroupExpressions to every input row, hence we will get
   * multiple output rows for an input row.
   *
   * @param groupingSetsAttrs The attributes of grouping sets
   * @param groupByAliases The aliased original group by expressions
   * @param groupByAttrs The attributes of aliased group by expressions
   * @param gid Attribute of the grouping id
   * @param child Child operator
   */
  def apply(
    groupingSetsAttrs: Seq[Seq[Attribute]],
    groupByAliases: Seq[Alias],
    groupByAttrs: Seq[Attribute],
    gid: Attribute,
    child: LogicalPlan): Expand = {
    val attrMap = groupByAttrs.zipWithIndex.toMap

    // Create an array of Projections for the child projection, and replace the projections'
    // expressions which equal GroupBy expressions with Literal(null), if those expressions
    // are not set for this grouping set.
    val projections = groupingSetsAttrs.map { groupingSetAttrs =>
      child.output ++ groupByAttrs.map { attr =>
        if (!groupingSetAttrs.contains(attr)) {
          // if the input attribute in the Invalid Grouping Expression set of for this group
          // replace it with constant null
          Literal.create(null, attr.dataType)
        } else {
          attr
        }
      // groupingId is the last output, here we use the bit mask as the concrete value for it.
      } :+ Literal.create(buildBitmask(groupingSetAttrs, attrMap), IntegerType)
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

  override def computeStats(conf: SQLConf): Statistics = {
    val sizeInBytes = super.computeStats(conf).sizeInBytes * projections.length
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
 * @param selectedGroupByExprs A sequence of selected GroupBy expressions, all exprs should
 *                     exist in groupByExprs.
 * @param groupByExprs The Group By expressions candidates.
 * @param child        Child operator
 * @param aggregations The Aggregation expressions, those non selected group by expressions
 *                     will be considered as constant null if it appears in the expressions
 */
case class GroupingSets(
    selectedGroupByExprs: Seq[Seq[Expression]],
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
  override def computeStats(conf: SQLConf): Statistics = {
    val limit = limitExpr.eval().asInstanceOf[Int]
    val childStats = child.stats(conf)
    val rowCount: BigInt = childStats.rowCount.map(_.min(limit)).getOrElse(limit)
    // Don't propagate column stats, because we don't know the distribution after a limit operation
    Statistics(
      sizeInBytes = EstimationUtils.getOutputSize(output, rowCount, childStats.attributeStats),
      rowCount = Some(rowCount),
      hints = childStats.hints)
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
  override def computeStats(conf: SQLConf): Statistics = {
    val limit = limitExpr.eval().asInstanceOf[Int]
    val childStats = child.stats(conf)
    if (limit == 0) {
      // sizeInBytes can't be zero, or sizeInBytes of BinaryNode will also be zero
      // (product of children).
      Statistics(
        sizeInBytes = 1,
        rowCount = Some(0),
        hints = childStats.hints)
    } else {
      // The output row count of LocalLimit should be the sum of row counts from each partition.
      // However, since the number of partitions is not available here, we just use statistics of
      // the child. Because the distribution after a limit operation is unknown, we do not propagate
      // the column stats.
      childStats.copy(attributeStats = AttributeMap(Nil))
    }
  }
}

case class SubqueryAlias(
    alias: String,
    child: LogicalPlan)
  extends UnaryNode {

  override lazy val canonicalized: LogicalPlan = child.canonicalized

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

  override def computeStats(conf: SQLConf): Statistics = {
    val ratio = upperBound - lowerBound
    val childStats = child.stats(conf)
    var sizeInBytes = EstimationUtils.ceil(BigDecimal(childStats.sizeInBytes) * ratio)
    if (sizeInBytes == 0) {
      sizeInBytes = 1
    }
    val sampledRowCount = childStats.rowCount.map(c => EstimationUtils.ceil(BigDecimal(c) * ratio))
    // Don't propagate column stats, because we don't know the distribution after a sample operation
    Statistics(sizeInBytes, sampledRowCount, hints = childStats.hints)
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
 * A base interface for [[RepartitionByExpression]] and [[Repartition]]
 */
abstract class RepartitionOperation extends UnaryNode {
  def shuffle: Boolean
  def numPartitions: Int
  override def output: Seq[Attribute] = child.output
}

/**
 * Returns a new RDD that has exactly `numPartitions` partitions. Differs from
 * [[RepartitionByExpression]] as this method is called directly by DataFrame's, because the user
 * asked for `coalesce` or `repartition`. [[RepartitionByExpression]] is used when the consumer
 * of the output requires some specific ordering or distribution of the data.
 */
case class Repartition(numPartitions: Int, shuffle: Boolean, child: LogicalPlan)
  extends RepartitionOperation {
  require(numPartitions > 0, s"Number of partitions ($numPartitions) must be positive.")
}

/**
 * This method repartitions data using [[Expression]]s into `numPartitions`, and receives
 * information about the number of partitions during execution. Used when a specific ordering or
 * distribution is expected by the consumer of the query result. Use [[Repartition]] for RDD-like
 * `coalesce` and `repartition`.
 */
case class RepartitionByExpression(
    partitionExpressions: Seq[Expression],
    child: LogicalPlan,
    numPartitions: Int) extends RepartitionOperation {

  require(numPartitions > 0, s"Number of partitions ($numPartitions) must be positive.")

  override def maxRows: Option[Long] = child.maxRows
  override def shuffle: Boolean = true
}

/**
 * A relation with one row. This is used in "SELECT ..." without a from clause.
 */
case object OneRowRelation extends LeafNode {
  override def maxRows: Option[Long] = Some(1)
  override def output: Seq[Attribute] = Nil
  override def computeStats(conf: SQLConf): Statistics = Statistics(sizeInBytes = 1)
}

/** A logical plan for `dropDuplicates`. */
case class Deduplicate(
    keys: Seq[Attribute],
    child: LogicalPlan,
    streaming: Boolean) extends UnaryNode {

  override def output: Seq[Attribute] = child.output
}
