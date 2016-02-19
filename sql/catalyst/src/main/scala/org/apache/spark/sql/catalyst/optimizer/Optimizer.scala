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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.immutable.HashSet

import org.apache.spark.sql.catalyst.analysis.{CleanupAliases, EliminateSubQueries}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.{ExtractFiltersAndInnerJoins, Unions}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types._

/**
  * Abstract class all optimizers should inherit of, contains the standard batches (extending
  * Optimizers can override this.
  */
abstract class Optimizer extends RuleExecutor[LogicalPlan] {
  def batches: Seq[Batch] = {
    // Technically some of the rules in Finish Analysis are not optimizer rules and belong more
    // in the analyzer, because they are needed for correctness (e.g. ComputeCurrentTime).
    // However, because we also use the analyzer to canonicalized queries (for view definition),
    // we do not eliminate subqueries or compute current time in the analyzer.
    Batch("Finish Analysis", Once,
      EliminateSubQueries,
      ComputeCurrentTime) ::
    //////////////////////////////////////////////////////////////////////////////////////////
    // Optimizer rules start here
    //////////////////////////////////////////////////////////////////////////////////////////
    // - Do the first call of CombineUnions before starting the major Optimizer rules,
    //   since it can reduce the number of iteration and the other rules could add/move
    //   extra operators between two adjacent Union operators.
    // - Call CombineUnions again in Batch("Operator Optimizations"),
    //   since the other rules might make two separate Unions operators adjacent.
    Batch("Union", Once,
      CombineUnions) ::
    Batch("Replace Operators", FixedPoint(100),
      ReplaceIntersectWithSemiJoin,
      ReplaceDistinctWithAggregate) ::
    Batch("Aggregate", FixedPoint(100),
      RemoveLiteralFromGroupExpressions) ::
    Batch("Operator Optimizations", FixedPoint(100),
      // Operator push down
      SetOperationPushDown,
      SamplePushDown,
      ReorderJoin,
      PushPredicateThroughJoin,
      PushPredicateThroughProject,
      PushPredicateThroughGenerate,
      PushPredicateThroughAggregate,
      // LimitPushDown, // Disabled until we have whole-stage codegen for limit
      ColumnPruning,
      // Operator combine
      CollapseRepartition,
      CollapseProject,
      CombineFilters,
      CombineLimits,
      CombineUnions,
      // Constant folding and strength reduction
      NullPropagation,
      OptimizeIn,
      ConstantFolding,
      LikeSimplification,
      BooleanSimplification,
      SimplifyConditionals,
      RemoveDispensableExpressions,
      SimplifyFilters,
      SimplifyCasts,
      SimplifyCaseConversionExpressions,
      EliminateSerialization) ::
    Batch("Decimal Optimizations", FixedPoint(100),
      DecimalAggregates) ::
    Batch("LocalRelation", FixedPoint(100),
      ConvertToLocalRelation) :: Nil
  }
}

/**
  * Non-abstract representation of the standard Spark optimizing strategies
  *
  * To ensure extendability, we leave the standard rules in the abstract optimizer rules, while
  * specific rules go to the subclasses
  */
object DefaultOptimizer extends Optimizer

/**
 * Pushes operations down into a Sample.
 */
object SamplePushDown extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Push down projection into sample
    case Project(projectList, s @ Sample(lb, up, replace, seed, child)) =>
      Sample(lb, up, replace, seed,
        Project(projectList, child))
  }
}

/**
 * Removes cases where we are unnecessarily going between the object and serialized (InternalRow)
 * representation of data item.  For example back to back map operations.
 */
object EliminateSerialization extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case m @ MapPartitions(_, deserializer, _, child: ObjectOperator)
        if !deserializer.isInstanceOf[Attribute] &&
          deserializer.dataType == child.outputObject.dataType =>
      val childWithoutSerialization = child.withObjectOutput
      m.copy(
        deserializer = childWithoutSerialization.output.head,
        child = childWithoutSerialization)
  }
}

/**
 * Pushes down [[LocalLimit]] beneath UNION ALL and beneath the streamed inputs of outer joins.
 */
object LimitPushDown extends Rule[LogicalPlan] {

  private def stripGlobalLimitIfPresent(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case GlobalLimit(expr, child) => child
      case _ => plan
    }
  }

  private def maybePushLimit(limitExp: Expression, plan: LogicalPlan): LogicalPlan = {
    (limitExp, plan.maxRows) match {
      case (IntegerLiteral(maxRow), Some(childMaxRows)) if maxRow < childMaxRows =>
        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))
      case (_, None) =>
        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))
      case _ => plan
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Adding extra Limits below UNION ALL for children which are not Limit or do not have Limit
    // descendants whose maxRow is larger. This heuristic is valid assuming there does not exist any
    // Limit push-down rule that is unable to infer the value of maxRows.
    // Note: right now Union means UNION ALL, which does not de-duplicate rows, so it is safe to
    // pushdown Limit through it. Once we add UNION DISTINCT, however, we will not be able to
    // pushdown Limit.
    case LocalLimit(exp, Union(children)) =>
      LocalLimit(exp, Union(children.map(maybePushLimit(exp, _))))
    // Add extra limits below OUTER JOIN. For LEFT OUTER and FULL OUTER JOIN we push limits to the
    // left and right sides, respectively. For FULL OUTER JOIN, we can only push limits to one side
    // because we need to ensure that rows from the limited side still have an opportunity to match
    // against all candidates from the non-limited side. We also need to ensure that this limit
    // pushdown rule will not eventually introduce limits on both sides if it is applied multiple
    // times. Therefore:
    //   - If one side is already limited, stack another limit on top if the new limit is smaller.
    //     The redundant limit will be collapsed by the CombineLimits rule.
    //   - If neither side is limited, limit the side that is estimated to be bigger.
    case LocalLimit(exp, join @ Join(left, right, joinType, condition)) =>
      val newJoin = joinType match {
        case RightOuter => join.copy(right = maybePushLimit(exp, right))
        case LeftOuter => join.copy(left = maybePushLimit(exp, left))
        case FullOuter =>
          (left.maxRows, right.maxRows) match {
            case (None, None) =>
              if (left.statistics.sizeInBytes >= right.statistics.sizeInBytes) {
                join.copy(left = maybePushLimit(exp, left))
              } else {
                join.copy(right = maybePushLimit(exp, right))
              }
            case (Some(_), Some(_)) => join
            case (Some(_), None) => join.copy(left = maybePushLimit(exp, left))
            case (None, Some(_)) => join.copy(right = maybePushLimit(exp, right))

          }
        case _ => join
      }
      LocalLimit(exp, newJoin)
  }
}

/**
 * Pushes certain operations to both sides of a Union or Except operator.
 * Operations that are safe to pushdown are listed as follows.
 * Union:
 * Right now, Union means UNION ALL, which does not de-duplicate rows. So, it is
 * safe to pushdown Filters and Projections through it. Once we add UNION DISTINCT,
 * we will not be able to pushdown Projections.
 *
 * Except:
 * It is not safe to pushdown Projections through it because we need to get the
 * intersect of rows by comparing the entire rows. It is fine to pushdown Filters
 * with deterministic condition.
 */
object SetOperationPushDown extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Maps Attributes from the left side to the corresponding Attribute on the right side.
   */
  private def buildRewrites(left: LogicalPlan, right: LogicalPlan): AttributeMap[Attribute] = {
    assert(left.output.size == right.output.size)
    AttributeMap(left.output.zip(right.output))
  }

  /**
   * Rewrites an expression so that it can be pushed to the right side of a
   * Union or Except operator. This method relies on the fact that the output attributes
   * of a union/intersect/except are always equal to the left child's output.
   */
  private def pushToRight[A <: Expression](e: A, rewrites: AttributeMap[Attribute]) = {
    val result = e transform {
      case a: Attribute => rewrites(a)
    }

    // We must promise the compiler that we did not discard the names in the case of project
    // expressions.  This is safe since the only transformation is from Attribute => Attribute.
    result.asInstanceOf[A]
  }

  /**
   * Splits the condition expression into small conditions by `And`, and partition them by
   * deterministic, and finally recombine them by `And`. It returns an expression containing
   * all deterministic expressions (the first field of the returned Tuple2) and an expression
   * containing all non-deterministic expressions (the second field of the returned Tuple2).
   */
  private def partitionByDeterministic(condition: Expression): (Expression, Expression) = {
    val andConditions = splitConjunctivePredicates(condition)
    andConditions.partition(_.deterministic) match {
      case (deterministic, nondeterministic) =>
        deterministic.reduceOption(And).getOrElse(Literal(true)) ->
        nondeterministic.reduceOption(And).getOrElse(Literal(true))
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {

    // Push down deterministic projection through UNION ALL
    case p @ Project(projectList, Union(children)) =>
      assert(children.nonEmpty)
      if (projectList.forall(_.deterministic)) {
        val newFirstChild = Project(projectList, children.head)
        val newOtherChildren = children.tail.map ( child => {
          val rewrites = buildRewrites(children.head, child)
          Project(projectList.map(pushToRight(_, rewrites)), child)
        } )
        Union(newFirstChild +: newOtherChildren)
      } else {
        p
      }

    // Push down filter into union
    case Filter(condition, Union(children)) =>
      assert(children.nonEmpty)
      val (deterministic, nondeterministic) = partitionByDeterministic(condition)
      val newFirstChild = Filter(deterministic, children.head)
      val newOtherChildren = children.tail.map {
        child => {
          val rewrites = buildRewrites(children.head, child)
          Filter(pushToRight(deterministic, rewrites), child)
        }
      }
      Filter(nondeterministic, Union(newFirstChild +: newOtherChildren))

    // Push down filter through EXCEPT
    case Filter(condition, Except(left, right)) =>
      val (deterministic, nondeterministic) = partitionByDeterministic(condition)
      val rewrites = buildRewrites(left, right)
      Filter(nondeterministic,
        Except(
          Filter(deterministic, left),
          Filter(pushToRight(deterministic, rewrites), right)
        )
      )
  }
}

/**
 * Attempts to eliminate the reading of unneeded columns from the query plan using the following
 * transformations:
 *
 *  - Inserting Projections beneath the following operators:
 *   - Aggregate
 *   - Generate
 *   - Project <- Join
 *   - LeftSemiJoin
 */
object ColumnPruning extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(_, _, e @ Expand(projects, output, child))
      if (e.outputSet -- a.references).nonEmpty =>
      val newOutput = output.filter(a.references.contains(_))
      val newProjects = projects.map { proj =>
        proj.zip(output).filter { case (e, a) =>
          newOutput.contains(a)
        }.unzip._1
      }
      a.copy(child = Expand(newProjects, newOutput, child))

    case a @ Aggregate(_, _, e @ Expand(_, _, child))
      if (child.outputSet -- e.references -- a.references).nonEmpty =>
      a.copy(child = e.copy(child = prunedChild(child, e.references ++ a.references)))

    // Eliminate attributes that are not needed to calculate the specified aggregates.
    case a @ Aggregate(_, _, child) if (child.outputSet -- a.references).nonEmpty =>
      a.copy(child = Project(a.references.toSeq, child))

    // Eliminate attributes that are not needed to calculate the Generate.
    case g: Generate if !g.join && (g.child.outputSet -- g.references).nonEmpty =>
      g.copy(child = Project(g.references.toSeq, g.child))

    case p @ Project(_, g: Generate) if g.join && p.references.subsetOf(g.generatedSet) =>
      p.copy(child = g.copy(join = false))

    case p @ Project(projectList, g: Generate) if g.join =>
      val neededChildOutput = p.references -- g.generatorOutput ++ g.references
      if (neededChildOutput == g.child.outputSet) {
        p
      } else {
        Project(projectList, g.copy(child = Project(neededChildOutput.toSeq, g.child)))
      }

    case p @ Project(projectList, a @ Aggregate(groupingExpressions, aggregateExpressions, child))
        if (a.outputSet -- p.references).nonEmpty =>
      Project(
        projectList,
        Aggregate(
          groupingExpressions,
          aggregateExpressions.filter(e => p.references.contains(e)),
          child))

    // Eliminate unneeded attributes from either side of a Join.
    case Project(projectList, Join(left, right, joinType, condition)) =>
      // Collect the list of all references required either above or to evaluate the condition.
      val allReferences: AttributeSet =
        AttributeSet(
          projectList.flatMap(_.references.iterator)) ++
          condition.map(_.references).getOrElse(AttributeSet(Seq.empty))

      /** Applies a projection only when the child is producing unnecessary attributes */
      def pruneJoinChild(c: LogicalPlan): LogicalPlan = prunedChild(c, allReferences)

      Project(projectList, Join(pruneJoinChild(left), pruneJoinChild(right), joinType, condition))

    // Eliminate unneeded attributes from right side of a LeftSemiJoin.
    case Join(left, right, LeftSemi, condition) =>
      // Collect the list of all references required to evaluate the condition.
      val allReferences: AttributeSet =
        condition.map(_.references).getOrElse(AttributeSet(Seq.empty))

      Join(left, prunedChild(right, allReferences), LeftSemi, condition)

    // Push down project through limit, so that we may have chance to push it further.
    case Project(projectList, Limit(exp, child)) =>
      Limit(exp, Project(projectList, child))

    // Push down project if possible when the child is sort.
    case p @ Project(projectList, s @ Sort(_, _, grandChild)) =>
      if (s.references.subsetOf(p.outputSet)) {
        s.copy(child = Project(projectList, grandChild))
      } else {
        val neededReferences = s.references ++ p.references
        if (neededReferences == grandChild.outputSet) {
          // No column we can prune, return the original plan.
          p
        } else {
          // Do not use neededReferences.toSeq directly, should respect grandChild's output order.
          val newProjectList = grandChild.output.filter(neededReferences.contains)
          p.copy(child = s.copy(child = Project(newProjectList, grandChild)))
        }
      }

    // Eliminate no-op Projects
    case Project(projectList, child) if child.output == projectList => child
  }

  /** Applies a projection only when the child is producing unnecessary attributes */
  private def prunedChild(c: LogicalPlan, allReferences: AttributeSet) =
    if ((c.outputSet -- allReferences.filter(c.outputSet.contains)).nonEmpty) {
      Project(allReferences.filter(c.outputSet.contains).toSeq, c)
    } else {
      c
    }
}

/**
 * Combines two adjacent [[Project]] operators into one and perform alias substitution,
 * merging the expressions into one single expression.
 */
object CollapseProject extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p @ Project(projectList1, Project(projectList2, child)) =>
      // Create a map of Aliases to their values from the child projection.
      // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
      val aliasMap = AttributeMap(projectList2.collect {
        case a: Alias => (a.toAttribute, a)
      })

      // We only collapse these two Projects if their overlapped expressions are all
      // deterministic.
      val hasNondeterministic = projectList1.exists(_.collect {
        case a: Attribute if aliasMap.contains(a) => aliasMap(a).child
      }.exists(!_.deterministic))

      if (hasNondeterministic) {
        p
      } else {
        // Substitute any attributes that are produced by the child projection, so that we safely
        // eliminate it.
        // e.g., 'SELECT c + 1 FROM (SELECT a + b AS C ...' produces 'SELECT a + b + 1 ...'
        // TODO: Fix TransformBase to avoid the cast below.
        val substitutedProjection = projectList1.map(_.transform {
          case a: Attribute => aliasMap.getOrElse(a, a)
        }).asInstanceOf[Seq[NamedExpression]]
        // collapse 2 projects may introduce unnecessary Aliases, trim them here.
        val cleanedProjection = substitutedProjection.map(p =>
          CleanupAliases.trimNonTopLevelAliases(p).asInstanceOf[NamedExpression]
        )
        Project(cleanedProjection, child)
      }

    // TODO Eliminate duplicate code
    // This clause is identical to the one above except that the inner operator is an `Aggregate`
    // rather than a `Project`.
    case p @ Project(projectList1, agg @ Aggregate(_, projectList2, child)) =>
      // Create a map of Aliases to their values from the child projection.
      // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
      val aliasMap = AttributeMap(projectList2.collect {
        case a: Alias => (a.toAttribute, a)
      })

      // We only collapse these two Projects if their overlapped expressions are all
      // deterministic.
      val hasNondeterministic = projectList1.exists(_.collect {
        case a: Attribute if aliasMap.contains(a) => aliasMap(a).child
      }.exists(!_.deterministic))

      if (hasNondeterministic) {
        p
      } else {
        // Substitute any attributes that are produced by the child projection, so that we safely
        // eliminate it.
        // e.g., 'SELECT c + 1 FROM (SELECT a + b AS C ...' produces 'SELECT a + b + 1 ...'
        // TODO: Fix TransformBase to avoid the cast below.
        val substitutedProjection = projectList1.map(_.transform {
          case a: Attribute => aliasMap.getOrElse(a, a)
        }).asInstanceOf[Seq[NamedExpression]]
        // collapse 2 projects may introduce unnecessary Aliases, trim them here.
        val cleanedProjection = substitutedProjection.map(p =>
          CleanupAliases.trimNonTopLevelAliases(p).asInstanceOf[NamedExpression]
        )
        agg.copy(aggregateExpressions = cleanedProjection)
      }
  }
}

/**
 * Combines adjacent [[Repartition]] operators by keeping only the last one.
 */
object CollapseRepartition extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case r @ Repartition(numPartitions, shuffle, Repartition(_, _, child)) =>
      Repartition(numPartitions, shuffle, child)
  }
}

/**
 * Simplifies LIKE expressions that do not need full regular expressions to evaluate the condition.
 * For example, when the expression is just checking to see if a string starts with a given
 * pattern.
 */
object LikeSimplification extends Rule[LogicalPlan] {
  // if guards below protect from escapes on trailing %.
  // Cases like "something\%" are not optimized, but this does not affect correctness.
  private val startsWith = "([^_%]+)%".r
  private val endsWith = "%([^_%]+)".r
  private val contains = "%([^_%]+)%".r
  private val equalTo = "([^_%]*)".r

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Like(l, Literal(utf, StringType)) =>
      utf.toString match {
        case startsWith(pattern) if !pattern.endsWith("\\") =>
          StartsWith(l, Literal(pattern))
        case endsWith(pattern) =>
          EndsWith(l, Literal(pattern))
        case contains(pattern) if !pattern.endsWith("\\") =>
          Contains(l, Literal(pattern))
        case equalTo(pattern) =>
          EqualTo(l, Literal(pattern))
        case _ =>
          Like(l, Literal.create(utf, StringType))
      }
  }
}

/**
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values. This rule is more specific with
 * Null value propagation from bottom to top of the expression tree.
 */
object NullPropagation extends Rule[LogicalPlan] {
  def nonNullLiteral(e: Expression): Boolean = e match {
    case Literal(null, _) => false
    case _ => true
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case e @ AggregateExpression(Count(exprs), _, _) if !exprs.exists(nonNullLiteral) =>
        Cast(Literal(0L), e.dataType)
      case e @ IsNull(c) if !c.nullable => Literal.create(false, BooleanType)
      case e @ IsNotNull(c) if !c.nullable => Literal.create(true, BooleanType)
      case e @ GetArrayItem(Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ GetArrayItem(_, Literal(null, _)) => Literal.create(null, e.dataType)
      case e @ GetMapValue(Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ GetMapValue(_, Literal(null, _)) => Literal.create(null, e.dataType)
      case e @ GetStructField(Literal(null, _), _, _) => Literal.create(null, e.dataType)
      case e @ GetArrayStructFields(Literal(null, _), _, _, _, _) =>
        Literal.create(null, e.dataType)
      case e @ EqualNullSafe(Literal(null, _), r) => IsNull(r)
      case e @ EqualNullSafe(l, Literal(null, _)) => IsNull(l)
      case e @ AggregateExpression(Count(exprs), mode, false) if !exprs.exists(_.nullable) =>
        // This rule should be only triggered when isDistinct field is false.
        AggregateExpression(Count(Literal(1)), mode, isDistinct = false)

      // For Coalesce, remove null literals.
      case e @ Coalesce(children) =>
        val newChildren = children.filter(nonNullLiteral)
        if (newChildren.length == 0) {
          Literal.create(null, e.dataType)
        } else if (newChildren.length == 1) {
          newChildren.head
        } else {
          Coalesce(newChildren)
        }

      case e @ Substring(Literal(null, _), _, _) => Literal.create(null, e.dataType)
      case e @ Substring(_, Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ Substring(_, _, Literal(null, _)) => Literal.create(null, e.dataType)

      // MaxOf and MinOf can't do null propagation
      case e: MaxOf => e
      case e: MinOf => e

      // Put exceptional cases above if any
      case e @ BinaryArithmetic(Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ BinaryArithmetic(_, Literal(null, _)) => Literal.create(null, e.dataType)

      case e @ BinaryComparison(Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ BinaryComparison(_, Literal(null, _)) => Literal.create(null, e.dataType)

      case e: StringRegexExpression => e.children match {
        case Literal(null, _) :: right :: Nil => Literal.create(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal.create(null, e.dataType)
        case _ => e
      }

      case e: StringPredicate => e.children match {
        case Literal(null, _) :: right :: Nil => Literal.create(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal.create(null, e.dataType)
        case _ => e
      }

      // If the value expression is NULL then transform the In expression to
      // Literal(null)
      case In(Literal(null, _), list) => Literal.create(null, BooleanType)

    }
  }
}

/**
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values.
 */
object ConstantFolding extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      // Skip redundant folding of literals. This rule is technically not necessary. Placing this
      // here avoids running the next rule for Literal values, which would create a new Literal
      // object and running eval unnecessarily.
      case l: Literal => l

      // Fold expressions that are foldable.
      case e if e.foldable => Literal.create(e.eval(EmptyRow), e.dataType)
    }
  }
}

/**
 * Replaces [[In (value, seq[Literal])]] with optimized version[[InSet (value, HashSet[Literal])]]
 * which is much faster
 */
object OptimizeIn extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      case In(v, list) if !list.exists(!_.isInstanceOf[Literal]) && list.size > 10 =>
        val hSet = list.map(e => e.eval(EmptyRow))
        InSet(v, HashSet() ++ hSet)
    }
  }
}

/**
 * Simplifies boolean expressions:
 * 1. Simplifies expressions whose answer can be determined without evaluating both sides.
 * 2. Eliminates / extracts common factors.
 * 3. Merge same expressions
 * 4. Removes `Not` operator.
 */
object BooleanSimplification extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case TrueLiteral And e => e
      case e And TrueLiteral => e
      case FalseLiteral Or e => e
      case e Or FalseLiteral => e

      case FalseLiteral And _ => FalseLiteral
      case _ And FalseLiteral => FalseLiteral
      case TrueLiteral Or _ => TrueLiteral
      case _ Or TrueLiteral => TrueLiteral

      case a And b if a.semanticEquals(b) => a
      case a Or b if a.semanticEquals(b) => a

      case a And (b Or c) if Not(a).semanticEquals(b) => And(a, c)
      case a And (b Or c) if Not(a).semanticEquals(c) => And(a, b)
      case (a Or b) And c if a.semanticEquals(Not(c)) => And(b, c)
      case (a Or b) And c if b.semanticEquals(Not(c)) => And(a, c)

      case a Or (b And c) if Not(a).semanticEquals(b) => Or(a, c)
      case a Or (b And c) if Not(a).semanticEquals(c) => Or(a, b)
      case (a And b) Or c if a.semanticEquals(Not(c)) => Or(b, c)
      case (a And b) Or c if b.semanticEquals(Not(c)) => Or(a, c)

      // Common factor elimination for conjunction
      case and @ (left And right) =>
        // 1. Split left and right to get the disjunctive predicates,
        //   i.e. lhs = (a, b), rhs = (a, c)
        // 2. Find the common predict between lhsSet and rhsSet, i.e. common = (a)
        // 3. Remove common predict from lhsSet and rhsSet, i.e. ldiff = (b), rdiff = (c)
        // 4. Apply the formula, get the optimized predicate: common || (ldiff && rdiff)
        val lhs = splitDisjunctivePredicates(left)
        val rhs = splitDisjunctivePredicates(right)
        val common = lhs.filter(e => rhs.exists(e.semanticEquals))
        if (common.isEmpty) {
          // No common factors, return the original predicate
          and
        } else {
          val ldiff = lhs.filterNot(e => common.exists(e.semanticEquals))
          val rdiff = rhs.filterNot(e => common.exists(e.semanticEquals))
          if (ldiff.isEmpty || rdiff.isEmpty) {
            // (a || b || c || ...) && (a || b) => (a || b)
            common.reduce(Or)
          } else {
            // (a || b || c || ...) && (a || b || d || ...) =>
            // ((c || ...) && (d || ...)) || a || b
            (common :+ And(ldiff.reduce(Or), rdiff.reduce(Or))).reduce(Or)
          }
        }

      // Common factor elimination for disjunction
      case or @ (left Or right) =>
        // 1. Split left and right to get the conjunctive predicates,
        //   i.e.  lhs = (a, b), rhs = (a, c)
        // 2. Find the common predict between lhsSet and rhsSet, i.e. common = (a)
        // 3. Remove common predict from lhsSet and rhsSet, i.e. ldiff = (b), rdiff = (c)
        // 4. Apply the formula, get the optimized predicate: common && (ldiff || rdiff)
        val lhs = splitConjunctivePredicates(left)
        val rhs = splitConjunctivePredicates(right)
        val common = lhs.filter(e => rhs.exists(e.semanticEquals))
        if (common.isEmpty) {
          // No common factors, return the original predicate
          or
        } else {
          val ldiff = lhs.filterNot(e => common.exists(e.semanticEquals))
          val rdiff = rhs.filterNot(e => common.exists(e.semanticEquals))
          if (ldiff.isEmpty || rdiff.isEmpty) {
            // (a && b) || (a && b && c && ...) => a && b
            common.reduce(And)
          } else {
            // (a && b && c && ...) || (a && b && d && ...) =>
            // ((c && ...) || (d && ...)) && a && b
            (common :+ Or(ldiff.reduce(And), rdiff.reduce(And))).reduce(And)
          }
        }

      case Not(TrueLiteral) => FalseLiteral
      case Not(FalseLiteral) => TrueLiteral

      case Not(a GreaterThan b) => LessThanOrEqual(a, b)
      case Not(a GreaterThanOrEqual b) => LessThan(a, b)

      case Not(a LessThan b) => GreaterThanOrEqual(a, b)
      case Not(a LessThanOrEqual b) => GreaterThan(a, b)

      case Not(a Or b) => And(Not(a), Not(b))
      case Not(a And b) => Or(Not(a), Not(b))

      case Not(Not(e)) => e
    }
  }
}

/**
 * Simplifies conditional expressions (if / case).
 */
object SimplifyConditionals extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case If(TrueLiteral, trueValue, _) => trueValue
      case If(FalseLiteral, _, falseValue) => falseValue

      case e @ CaseWhen(branches, elseValue) if branches.exists(_._1 == FalseLiteral) =>
        // If there are branches that are always false, remove them.
        // If there are no more branches left, just use the else value.
        // Note that these two are handled together here in a single case statement because
        // otherwise we cannot determine the data type for the elseValue if it is None (i.e. null).
        val newBranches = branches.filter(_._1 != FalseLiteral)
        if (newBranches.isEmpty) {
          elseValue.getOrElse(Literal.create(null, e.dataType))
        } else {
          e.copy(branches = newBranches)
        }

      case e @ CaseWhen(branches, _) if branches.headOption.map(_._1) == Some(TrueLiteral) =>
        // If the first branch is a true literal, remove the entire CaseWhen and use the value
        // from that. Note that CaseWhen.branches should never be empty, and as a result the
        // headOption (rather than head) added above is just a extra (and unnecessary) safeguard.
        branches.head._2
    }
  }
}

/**
 * Combines all adjacent [[Union]] operators into a single [[Union]].
 */
object CombineUnions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Unions(children) => Union(children)
  }
}

/**
 * Combines two adjacent [[Filter]] operators into one, merging the
 * conditions into one conjunctive predicate.
 */
object CombineFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ff @ Filter(fc, nf @ Filter(nc, grandChild)) => Filter(And(nc, fc), grandChild)
  }
}

/**
 * Removes filters that can be evaluated trivially.  This is done either by eliding the filter for
 * cases where it will always evaluate to `true`, or substituting a dummy empty relation when the
 * filter will always evaluate to `false`.
 */
object SimplifyFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // If the filter condition always evaluate to true, remove the filter.
    case Filter(Literal(true, BooleanType), child) => child
    // If the filter condition always evaluate to null or false,
    // replace the input with an empty relation.
    case Filter(Literal(null, _), child) => LocalRelation(child.output, data = Seq.empty)
    case Filter(Literal(false, BooleanType), child) => LocalRelation(child.output, data = Seq.empty)
  }
}

/**
 * Pushes [[Filter]] operators through [[Project]] operators, in-lining any [[Alias Aliases]]
 * that were defined in the projection.
 *
 * This heuristic is valid assuming the expression evaluation cost is minimal.
 */
object PushPredicateThroughProject extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, project @ Project(fields, grandChild)) =>
      // Create a map of Aliases to their values from the child projection.
      // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> a + b).
      val aliasMap = AttributeMap(fields.collect {
        case a: Alias => (a.toAttribute, a.child)
      })

      // Split the condition into small conditions by `And`, so that we can push down part of this
      // condition without nondeterministic expressions.
      val andConditions = splitConjunctivePredicates(condition)

      val (deterministic, nondeterministic) = andConditions.partition(_.collect {
        case a: Attribute if aliasMap.contains(a) => aliasMap(a)
      }.forall(_.deterministic))

      // If there is no nondeterministic conditions, push down the whole condition.
      if (nondeterministic.isEmpty) {
        project.copy(child = Filter(replaceAlias(condition, aliasMap), grandChild))
      } else {
        // If they are all nondeterministic conditions, leave it un-changed.
        if (deterministic.isEmpty) {
          filter
        } else {
          // Push down the small conditions without nondeterministic expressions.
          val pushedCondition =
            deterministic.map(replaceAlias(_, aliasMap)).reduce(And)
          Filter(nondeterministic.reduce(And),
            project.copy(child = Filter(pushedCondition, grandChild)))
        }
      }
  }

}

/**
 * Push [[Filter]] operators through [[Generate]] operators. Parts of the predicate that reference
 * attributes generated in [[Generate]] will remain above, and the rest should be pushed beneath.
 */
object PushPredicateThroughGenerate extends Rule[LogicalPlan] with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, g: Generate) =>
      // Predicates that reference attributes produced by the `Generate` operator cannot
      // be pushed below the operator.
      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition { cond =>
        cond.references subsetOf g.child.outputSet
      }
      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val newGenerate = Generate(g.generator, join = g.join, outer = g.outer,
          g.qualifier, g.generatorOutput, Filter(pushDownPredicate, g.child))
        if (stayUp.isEmpty) newGenerate else Filter(stayUp.reduce(And), newGenerate)
      } else {
        filter
      }
  }
}

/**
 * Push [[Filter]] operators through [[Aggregate]] operators, iff the filters reference only
 * non-aggregate attributes (typically literals or grouping expressions).
 */
object PushPredicateThroughAggregate extends Rule[LogicalPlan] with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, aggregate: Aggregate) =>
      // Find all the aliased expressions in the aggregate list that don't include any actual
      // AggregateExpression, and create a map from the alias to the expression
      val aliasMap = AttributeMap(aggregate.aggregateExpressions.collect {
        case a: Alias if a.child.find(_.isInstanceOf[AggregateExpression]).isEmpty =>
          (a.toAttribute, a.child)
      })

      // For each filter, expand the alias and check if the filter can be evaluated using
      // attributes produced by the aggregate operator's child operator.
      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition { cond =>
        val replaced = replaceAlias(cond, aliasMap)
        replaced.references.subsetOf(aggregate.child.outputSet) && replaced.deterministic
      }

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val replaced = replaceAlias(pushDownPredicate, aliasMap)
        val newAggregate = aggregate.copy(child = Filter(replaced, aggregate.child))
        // If there is no more filter to stay up, just eliminate the filter.
        // Otherwise, create "Filter(stayUp) <- Aggregate <- Filter(pushDownPredicate)".
        if (stayUp.isEmpty) newAggregate else Filter(stayUp.reduce(And), newAggregate)
      } else {
        filter
      }
  }
}

/**
  * Reorder the joins and push all the conditions into join, so that the bottom ones have at least
  * one condition.
  *
  * The order of joins will not be changed if all of them already have at least one condition.
  */
object ReorderJoin extends Rule[LogicalPlan] with PredicateHelper {

  /**
    * Join a list of plans together and push down the conditions into them.
    *
    * The joined plan are picked from left to right, prefer those has at least one join condition.
    *
    * @param input a list of LogicalPlans to join.
    * @param conditions a list of condition for join.
    */
  def createOrderedJoin(input: Seq[LogicalPlan], conditions: Seq[Expression]): LogicalPlan = {
    assert(input.size >= 2)
    if (input.size == 2) {
      Join(input(0), input(1), Inner, conditions.reduceLeftOption(And))
    } else {
      val left :: rest = input.toList
      // find out the first join that have at least one join condition
      val conditionalJoin = rest.find { plan =>
        val refs = left.outputSet ++ plan.outputSet
        conditions.filterNot(canEvaluate(_, left)).filterNot(canEvaluate(_, plan))
          .exists(_.references.subsetOf(refs))
      }
      // pick the next one if no condition left
      val right = conditionalJoin.getOrElse(rest.head)

      val joinedRefs = left.outputSet ++ right.outputSet
      val (joinConditions, others) = conditions.partition(_.references.subsetOf(joinedRefs))
      val joined = Join(left, right, Inner, joinConditions.reduceLeftOption(And))

      // should not have reference to same logical plan
      createOrderedJoin(Seq(joined) ++ rest.filterNot(_ eq right), others)
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case j @ ExtractFiltersAndInnerJoins(input, conditions)
        if input.size > 2 && conditions.nonEmpty =>
      createOrderedJoin(input, conditions)
  }
}

/**
 * Pushes down [[Filter]] operators where the `condition` can be
 * evaluated using only the attributes of the left or right side of a join.  Other
 * [[Filter]] conditions are moved into the `condition` of the [[Join]].
 *
 * And also pushes down the join filter, where the `condition` can be evaluated using only the
 * attributes of the left or right side of sub query when applicable.
 *
 * Check https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior for more details
 */
object PushPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Splits join condition expressions into three categories based on the attributes required
   * to evaluate them.
   *
   * @return (canEvaluateInLeft, canEvaluateInRight, haveToEvaluateInBoth)
   */
  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftEvaluateCondition, rest) =
        condition.partition(_.references subsetOf left.outputSet)
    val (rightEvaluateCondition, commonCondition) =
        rest.partition(_.references subsetOf right.outputSet)

    (leftEvaluateCondition, rightEvaluateCondition, commonCondition)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // push the where condition down into join filter
    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition)) =>
      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
        split(splitConjunctivePredicates(filterCondition), left, right)

      joinType match {
        case Inner =>
          // push down the single side `where` condition into respective sides
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (commonFilterCondition ++ joinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, Inner, newJoinCond)
        case RightOuter =>
          // push down the right side only `where` condition
          val newLeft = left
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond)

          (leftFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case _ @ (LeftOuter | LeftSemi) =>
          // push down the left side only `where` condition
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, joinType, newJoinCond)

          (rightFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case FullOuter => f // DO Nothing for Full Outer Join
        case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
      }

    // push down the join filter into sub query scanning if applicable
    case f @ Join(left, right, joinType, joinCondition) =>
      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
        split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)

      joinType match {
        case _ @ (Inner | LeftSemi) =>
          // push down the single side only join filter for both sides sub queries
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = commonJoinCondition.reduceLeftOption(And)

          Join(newLeft, newRight, joinType, newJoinCond)
        case RightOuter =>
          // push down the left side only join filter for left side sub query
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, RightOuter, newJoinCond)
        case LeftOuter =>
          // push down the right side only join filter for right sub query
          val newLeft = left
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, LeftOuter, newJoinCond)
        case FullOuter => f
        case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
      }
  }
}

/**
 * Removes [[Cast Casts]] that are unnecessary because the input is already the correct type.
 */
object SimplifyCasts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Cast(e, dataType) if e.dataType == dataType => e
  }
}

/**
 * Removes nodes that are not necessary.
 */
object RemoveDispensableExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case UnaryPositive(child) => child
    case PromotePrecision(child) => child
  }
}

/**
 * Combines two adjacent [[Limit]] operators into one, merging the
 * expressions into one single expression.
 */
object CombineLimits extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ll @ GlobalLimit(le, nl @ GlobalLimit(ne, grandChild)) =>
      GlobalLimit(Least(Seq(ne, le)), grandChild)
    case ll @ LocalLimit(le, nl @ LocalLimit(ne, grandChild)) =>
      LocalLimit(Least(Seq(ne, le)), grandChild)
    case ll @ Limit(le, nl @ Limit(ne, grandChild)) =>
      Limit(Least(Seq(ne, le)), grandChild)
  }
}

/**
 * Removes the inner case conversion expressions that are unnecessary because
 * the inner conversion is overwritten by the outer one.
 */
object SimplifyCaseConversionExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case Upper(Upper(child)) => Upper(child)
      case Upper(Lower(child)) => Upper(child)
      case Lower(Upper(child)) => Lower(child)
      case Lower(Lower(child)) => Lower(child)
    }
  }
}

/**
 * Speeds up aggregates on fixed-precision decimals by executing them on unscaled Long values.
 *
 * This uses the same rules for increasing the precision and scale of the output as
 * [[org.apache.spark.sql.catalyst.analysis.DecimalPrecision]].
 */
object DecimalAggregates extends Rule[LogicalPlan] {
  import Decimal.MAX_LONG_DIGITS

  /** Maximum number of decimal digits representable precisely in a Double */
  private val MAX_DOUBLE_DIGITS = 15

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case AggregateExpression(Sum(e @ DecimalType.Expression(prec, scale)), mode, isDistinct)
      if prec + 10 <= MAX_LONG_DIGITS =>
      MakeDecimal(AggregateExpression(Sum(UnscaledValue(e)), mode, isDistinct), prec + 10, scale)

    case AggregateExpression(Average(e @ DecimalType.Expression(prec, scale)), mode, isDistinct)
      if prec + 4 <= MAX_DOUBLE_DIGITS =>
      val newAggExpr = AggregateExpression(Average(UnscaledValue(e)), mode, isDistinct)
      Cast(
        Divide(newAggExpr, Literal.create(math.pow(10.0, scale), DoubleType)),
        DecimalType(prec + 4, scale + 4))
  }
}

/**
 * Converts local operations (i.e. ones that don't require data exchange) on LocalRelation to
 * another LocalRelation.
 *
 * This is relatively simple as it currently handles only a single case: Project.
 */
object ConvertToLocalRelation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Project(projectList, LocalRelation(output, data)) =>
      val projection = new InterpretedProjection(projectList, output)
      LocalRelation(projectList.map(_.toAttribute), data.map(projection))
  }
}

/**
 * Replaces logical [[Distinct]] operator with an [[Aggregate]] operator.
 * {{{
 *   SELECT DISTINCT f1, f2 FROM t  ==>  SELECT f1, f2 FROM t GROUP BY f1, f2
 * }}}
 */
object ReplaceDistinctWithAggregate extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Distinct(child) => Aggregate(child.output, child.output, child)
  }
}

/**
 * Replaces logical [[Intersect]] operator with a left-semi [[Join]] operator.
 * {{{
 *   SELECT a1, a2 FROM Tab1 INTERSECT SELECT b1, b2 FROM Tab2
 *   ==>  SELECT DISTINCT a1, a2 FROM Tab1 LEFT SEMI JOIN Tab2 ON a1<=>b1 AND a2<=>b2
 * }}}
 *
 * Note:
 * 1. This rule is only applicable to INTERSECT DISTINCT. Do not use it for INTERSECT ALL.
 * 2. This rule has to be done after de-duplicating the attributes; otherwise, the generated
 *    join conditions will be incorrect.
 */
object ReplaceIntersectWithSemiJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Intersect(left, right) =>
      assert(left.output.size == right.output.size)
      val joinCond = left.output.zip(right.output).map { case (l, r) => EqualNullSafe(l, r) }
      Distinct(Join(left, right, LeftSemi, joinCond.reduceLeftOption(And)))
  }
}

/**
 * Removes literals from group expressions in [[Aggregate]], as they have no effect to the result
 * but only makes the grouping key bigger.
 */
object RemoveLiteralFromGroupExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(grouping, _, _) =>
      val newGrouping = grouping.filter(!_.foldable)
      a.copy(groupingExpressions = newGrouping)
  }
}

/**
 * Computes the current date and time to make sure we return the same result in a single query.
 */
object ComputeCurrentTime extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val dateExpr = CurrentDate()
    val timeExpr = CurrentTimestamp()
    val currentDate = Literal.create(dateExpr.eval(EmptyRow), dateExpr.dataType)
    val currentTime = Literal.create(timeExpr.eval(EmptyRow), timeExpr.dataType)

    plan transformAllExpressions {
      case CurrentDate() => currentDate
      case CurrentTimestamp() => currentTime
    }
  }
}
