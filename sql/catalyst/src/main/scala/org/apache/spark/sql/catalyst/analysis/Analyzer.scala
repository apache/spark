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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, AggregateExpression2, AggregateFunction2}
import org.apache.spark.sql.catalyst.plans.{LeftSemi, LeftAnti}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.catalyst.{SimpleCatalystConf, CatalystConf}
import org.apache.spark.sql.types._

/**
 * A trivial [[Analyzer]] with an [[EmptyCatalog]] and [[EmptyFunctionRegistry]]. Used for testing
 * when all relations are already filled in and the analyzer needs only to resolve attribute
 * references.
 */
object SimpleAnalyzer
  extends Analyzer(EmptyCatalog, EmptyFunctionRegistry, new SimpleCatalystConf(true))

/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a schema [[Catalog]] and
 * a [[FunctionRegistry]].
 */
class Analyzer(
    catalog: Catalog,
    registry: FunctionRegistry,
    conf: CatalystConf,
    maxIterations: Int = 100)
  extends RuleExecutor[LogicalPlan] with CheckAnalysis {

  def resolver: Resolver = {
    if (conf.caseSensitiveAnalysis) {
      caseSensitiveResolution
    } else {
      caseInsensitiveResolution
    }
  }

  val fixedPoint = FixedPoint(maxIterations)

  /**
   * Override to provide additional rules for the "Resolution" batch.
   */
  val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Nil

  lazy val batches: Seq[Batch] = Seq(
    Batch("Substitution", fixedPoint,
      CTESubstitution ::
      WindowsSubstitution ::
      Nil : _*),
    Batch("Resolution", fixedPoint,
      RewriteFilterSubQuery ::
      ResolveRelations ::
      ResolveReferences ::
      ResolveGroupingAnalytics ::
      ResolveSortReferences ::
      ResolveGenerate ::
      ResolveFunctions ::
      ResolveAliases ::
      ExtractWindowExpressions ::
      GlobalAggregates ::
      ResolveAggregateFunctions ::
      HiveTypeCoercion.typeCoercionRules ++
      extendedResolutionRules : _*),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic),
    Batch("Cleanup", fixedPoint,
      CleanupAliases)
  )

  /**
   * Substitute child plan with cte definitions
   */
  object CTESubstitution extends Rule[LogicalPlan] {
    // TODO allow subquery to define CTE
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators  {
      case With(child, relations) => substituteCTE(child, relations)
      case other => other
    }

    def substituteCTE(plan: LogicalPlan, cteRelations: Map[String, LogicalPlan]): LogicalPlan = {
      plan transform {
        // In hive, if there is same table name in database and CTE definition,
        // hive will use the table in database, not the CTE one.
        // Taking into account the reasonableness and the implementation complexity,
        // here use the CTE definition first, check table name only and ignore database name
        // see https://github.com/apache/spark/pull/4929#discussion_r27186638 for more info
        case u : UnresolvedRelation =>
          val substituted = cteRelations.get(u.tableIdentifier.table).map { relation =>
            val withAlias = u.alias.map(Subquery(_, relation))
            withAlias.getOrElse(relation)
          }
          substituted.getOrElse(u)
      }
    }
  }

  /**
   * Substitute child plan with WindowSpecDefinitions.
   */
  object WindowsSubstitution extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      // Lookup WindowSpecDefinitions. This rule works with unresolved children.
      case WithWindowDefinition(windowDefinitions, child) =>
        child.transform {
          case plan => plan.transformExpressions {
            case UnresolvedWindowExpression(c, WindowSpecReference(windowName)) =>
              val errorMessage =
                s"Window specification $windowName is not defined in the WINDOW clause."
              val windowSpecDefinition =
                windowDefinitions
                  .get(windowName)
                  .getOrElse(failAnalysis(errorMessage))
              WindowExpression(c, windowSpecDefinition)
          }
        }
    }
  }

  /**
   * Replaces [[UnresolvedAlias]]s with concrete aliases.
   */
  object ResolveAliases extends Rule[LogicalPlan] {
    private def assignAliases(exprs: Seq[NamedExpression]) = {
      exprs.zipWithIndex.map {
        case (expr, i) =>
          expr transform {
            case u @ UnresolvedAlias(child) => child match {
              case ne: NamedExpression => ne
              case e if !e.resolved => u
              case g: Generator if g.elementTypes.size > 1 => MultiAlias(g, Nil)
              case c @ Cast(ne: NamedExpression, _) => Alias(c, ne.name)()
              case other => Alias(other, s"_c$i")()
            }
          }
      }.asInstanceOf[Seq[NamedExpression]]
    }

    private def hasUnresolvedAlias(exprs: Seq[NamedExpression]) =
      exprs.exists(_.find(_.isInstanceOf[UnresolvedAlias]).isDefined)

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case Aggregate(groups, aggs, child) if child.resolved && hasUnresolvedAlias(aggs) =>
        Aggregate(groups, assignAliases(aggs), child)

      case g: GroupingAnalytics if g.child.resolved && hasUnresolvedAlias(g.aggregations) =>
        g.withNewAggs(assignAliases(g.aggregations))

      case Project(projectList, child) if child.resolved && hasUnresolvedAlias(projectList) =>
        Project(assignAliases(projectList), child)
    }
  }

  object ResolveGroupingAnalytics extends Rule[LogicalPlan] {
    /*
     *  GROUP BY a, b, c WITH ROLLUP
     *  is equivalent to
     *  GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (a), ( ) ).
     *  Group Count: N + 1 (N is the number of group expressions)
     *
     *  We need to get all of its subsets for the rule described above, the subset is
     *  represented as the bit masks.
     */
    def bitmasks(r: Rollup): Seq[Int] = {
      Seq.tabulate(r.groupByExprs.length + 1)(idx => {(1 << idx) - 1})
    }

    /*
     *  GROUP BY a, b, c WITH CUBE
     *  is equivalent to
     *  GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (b, c), (a, c), (a), (b), (c), ( ) ).
     *  Group Count: 2 ^ N (N is the number of group expressions)
     *
     *  We need to get all of its subsets for a given GROUPBY expression, the subsets are
     *  represented as the bit masks.
     */
    def bitmasks(c: Cube): Seq[Int] = {
      Seq.tabulate(1 << c.groupByExprs.length)(i => i)
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case a if !a.childrenResolved => a // be sure all of the children are resolved.
      case a: Cube =>
        GroupingSets(bitmasks(a), a.groupByExprs, a.child, a.aggregations)
      case a: Rollup =>
        GroupingSets(bitmasks(a), a.groupByExprs, a.child, a.aggregations)
      case x: GroupingSets =>
        val gid = AttributeReference(VirtualColumn.groupingIdName, IntegerType, false)()
        // We will insert another Projection if the GROUP BY keys contains the
        // non-attribute expressions. And the top operators can references those
        // expressions by its alias.
        // e.g. SELECT key%5 as c1 FROM src GROUP BY key%5 ==>
        //      SELECT a as c1 FROM (SELECT key%5 AS a FROM src) GROUP BY a

        // find all of the non-attribute expressions in the GROUP BY keys
        val nonAttributeGroupByExpressions = new ArrayBuffer[Alias]()

        // The pair of (the original GROUP BY key, associated attribute)
        val groupByExprPairs = x.groupByExprs.map(_ match {
          case e: NamedExpression => (e, e.toAttribute)
          case other => {
            val alias = Alias(other, other.toString)()
            nonAttributeGroupByExpressions += alias // add the non-attributes expression alias
            (other, alias.toAttribute)
          }
        })

        // substitute the non-attribute expressions for aggregations.
        val aggregation = x.aggregations.map(expr => expr.transformDown {
          case e => groupByExprPairs.find(_._1.semanticEquals(e)).map(_._2).getOrElse(e)
        }.asInstanceOf[NamedExpression])

        // substitute the group by expressions.
        val newGroupByExprs = groupByExprPairs.map(_._2)

        val child = if (nonAttributeGroupByExpressions.length > 0) {
          // insert additional projection if contains the
          // non-attribute expressions in the GROUP BY keys
          Project(x.child.output ++ nonAttributeGroupByExpressions, x.child)
        } else {
          x.child
        }

        Aggregate(
          newGroupByExprs :+ VirtualColumn.groupingIdAttribute,
          aggregation,
          Expand(x.bitmasks, newGroupByExprs, gid, child))
    }
  }

  /**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
   */
  object ResolveRelations extends Rule[LogicalPlan] {
    def getTable(u: UnresolvedRelation): LogicalPlan = {
      try {
        catalog.lookupRelation(u.tableIdentifier, u.alias)
      } catch {
        case _: NoSuchTableException =>
          u.failAnalysis(s"Table not found: ${u.tableName}")
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case i @ InsertIntoTable(u: UnresolvedRelation, _, _, _, _) =>
        i.copy(table = EliminateSubQueries(getTable(u)))
      case u: UnresolvedRelation =>
        try {
          getTable(u)
        } catch {
          case _: AnalysisException if u.tableIdentifier.database.isDefined =>
            // delay the exception into CheckAnalysis, then it could be resolved as data source.
            u
        }
    }
  }

  /**
   * Rewrite the [[Exists]] [[In]] with left semi join or anti join.
   *
   * 1. Some of the key concepts:
   *  Correlated:
   *   References the attributes of the parent query within subquery, we call that Correlated.
   *  e.g. We reference the "a.value", which is the attribute in parent query, in the subquery.
   *
   *   SELECT a.value FROM src a
   *   WHERE a.key in (
   *     SELECT b.key FROM src1 b
   *     WHERE a.value > b.value)
   *
   *  Unrelated:
   *   Do not have any attribute reference to its parent query in the subquery.
   *  e.g.
   *   SELECT a.value FROM src a WHERE a.key IN (SELECT key FROM src WHERE key > 100);
   *
   * 2. Basic Logic for the Transformation
   *    EXISTS / IN => LEFT SEMI JOIN
   *    NOT EXISTS / NOT IN => LEFT ANTI JOIN
   *
   *    In logical plan demostration, we support the cases like below:
   *
   *    e.g. EXISTS / NOT EXISTS
   *    SELECT value FROM src a WHERE (NOT) EXISTS (SELECT 1 FROM src1 b WHERE a.key < b.key)
   *        ==>
   *    SELECT a.value FROM src a LEFT (ANTI) SEMI JOIN src1 b WHERE a.key < b.key
   *
   *    e.g. IN / NOT IN
   *    SELECT value FROM src a WHERE key (NOT) IN (SELECT key FROM src1 b WHERE a.value < b.value)
   *       ==>
   *    SELECT value FROM src a LEFT (ANTI) SEMI JOIN src1 b ON a.key = b.key AND a.value < b.value
   *
   *    e.g. IN / NOT IN with other conjunctions
   *    SELECT value FROM src a
   *    WHERE key (NOT) IN (
   *      SELECT key FROM src1 b WHERE a.value < b.value
   *    ) AND a.key > 10
   *       ==>
   *    SELECT value
   *      (FROM src a WHERE a.key > 10)
   *    LEFT (ANTI) SEMI JOIN src1 b ON a.key = b.key AND a.value < b.value
   *
   * 3. There are also some limitations:
   *   a. IN/NOT IN subqueries may only select a single column.
   *    e.g.(bad example)
   *    SELECT value FROM src a WHERE EXISTS (SELECT key, value FROM src1 WHERE key > 10)
   *   b. EXISTS/NOT EXISTS must have one or more correlated predicates.
   *    e.g.(bad example)
   *    SELECT value FROM src a WHERE EXISTS (SELECT 1 FROM src1 b WHERE b.key > 10)
   *   c. References to the parent query is only supported in the WHERE clause of the subquery.
   *    e.g.(bad example)
   *    SELECT value FROM src a WHERE key IN (SELECT a.key + b.key FROM src1 b)
   *   d. Only a single subquery can support in IN/EXISTS predicate.
   *    e.g.(bad example)
   *    SELECT value FROM src WHERE key IN (SELECT xx1 FROM xxx1) AND key in (SELECT xx2 FROM xxx2)
   *   e. Disjunction is not supported in the top level.
   *    e.g.(bad example)
   *    SELECT value FROM src WHERE key > 10 OR key IN (SELECT xx1 FROM xxx1)
   *   f. Implicit reference expression substitution to the parent query is not supported.
   *    e.g.(bad example)
   *    SELECT min(key) FROM src a HAVING EXISTS (SELECT 1 FROM src1 b WHERE b.key = min(a.key))
   *
   * 4. TODOs
   *   a. More pretty message to user why we failed in parsing.
   *   b. Support multiple IN / EXISTS clause in the predicates.
   *   c. Implicit reference expression substitution to the parent query
   *   d. ..
   */
  object RewriteFilterSubQuery extends Rule[LogicalPlan] with PredicateHelper {
    // This is to extract the SubQuery expression and other conjunction expressions.
    def unapply(condition: Expression): Option[(Expression, Seq[Expression])] = {
      if (condition.resolved == false) {
        return None
      }

      val conjunctions = splitConjunctivePredicates(condition).map(_ transformDown {
          // Remove the Cast expression for SubQueryExpression.
          case Cast(f: SubQueryExpression, BooleanType) => f
        }
      )

      val (subqueries, others) = conjunctions.partition(c => c.isInstanceOf[SubQueryExpression])
      if (subqueries.isEmpty) {
        None
      } else if (subqueries.length > 1) {
        // We don't support the cases with multiple subquery in the predicates now like:
        // SELECT value FROM src
        // WHERE
        //   key IN (SELECT key xxx) AND
        //   key IN (SELECT key xxx)
        // TODO support this case in the future since it's part of the `standard SQL`
        throw new AnalysisException(
          s"Only 1 SubQuery expression is supported in predicates, but we got $subqueries")
      } else {
        val subQueryExpr = subqueries(0).asInstanceOf[SubQueryExpression]
        // try to resolve the subquery

        val subquery = Analyzer.this.execute(subQueryExpr.subquery) match {
          case Distinct(child) =>
            // Distinct is useless for semi join, ignore it.
            // e.g. SELECT value FROM src WHERE key IN (SELECT DISTINCT key FROM src b)
            // which is equvilent to
            // SELECT value FROM src WHERE key IN (SELECT key FROM src b)
            // The reason we discard the DISTINCT keyword is we don't want to make
            // additional rule for DISTINCT operator in the `def apply(..)`
            child
          case other => other
        }
        Some((subQueryExpr.withNewSubQuery(subquery), others))
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case f if f.childrenResolved == false => f

      case f @ Filter(RewriteFilterSubQuery(subquery, others), left) =>
        subquery match {
          case Exists(Project(_, Filter(condition, right)), positive) =>
            checkAnalysis(right)
            if (condition.resolved) {
              // Apparently, it should be not resolved here, since EXIST should be correlated.
              throw new AnalysisException(
                s"Exists/Not Exists operator SubQuery must be correlated, but we got $condition")
            }
            val newLeft = others.reduceOption(And).map(Filter(_, left)).getOrElse(left)
            Join(newLeft, right,
              if (positive) LeftSemi else LeftAnti,
              Some(ResolveReferences.tryResolveAttributes(condition, right)))

          case Exists(right, positive) =>
            throw new AnalysisException(s"Exists/Not Exists operator SubQuery must be Correlated," +
              s"but we got $right")

          case InSubquery(key, Project(projectList, Filter(condition, right)), positive) =>
            // we don't support nested correlation yet, so the `right` must be resolved.
            checkAnalysis(right)
            if (projectList.length != 1) {
              throw new AnalysisException(
                s"Expect only 1 projection in In SubQuery Expression, but we got $projectList")
            } else {
              // This is a workaround to solve the ambiguous references issue like:
              // SELECT 'value FROM src WHERE 'key IN (SELECT 'key FROM src b WHERE 'key > 100)
              //
              // Literally, we will transform the SQL into:
              //
              // SELECT 'value FROM src
              // LEFT SEMI JOIN src b
              //   ON 'key = 'key and 'key > 100 -- this is reference ambiguous for 'key!
              //
              // The ResolveReferences.tryResolveAttributes will partially resolve the project
              // list and filter condition of the subquery, and then what we got looks like:
              //
              // SELECT 'value FROM src
              // LEFI SEMI JOIN src b
              //   ON 'key = key#123 and key#123 > 100
              //
              // And then we will leave the remaining unresolved attributes for the other rules
              // in Analyzer.
              val rightKey = ResolveReferences.tryResolveAttributes(projectList(0), right)

              if (!rightKey.resolved) {
                throw new AnalysisException(s"Cannot resolve the projection for SubQuery $rightKey")
              }

              // This is for the SQL with conjunction like:
              //
              // SELECT value FROM src a
              // WHERE key > 5 AND key IN (SELECT key FROM src1 b key >7) AND key < 10
              //
              // ==>
              // SELECT value FROM (src a
              //   WHERE key > 5 AND key < 10)
              // LEFT SEMI JOIN src1 b ON a.key = b.key AND b.key > 7
              //
              // Ideally, we should transform the original plan into
              // SELECT value FROM src a
              // LEFT SEMI JOIN src1 b
              // ON a.key = b.key AND b.key > 7 AND a.key > 5 AND a.key < 10
              //
              // However, the former one only requires few code change to support
              // the multiple subquery for IN clause, and less overhead for Optimizer
              val newLeft = others.reduceOption(And).map(Filter(_, left)).getOrElse(left)
              val newCondition = Some(
                And(
                  ResolveReferences.tryResolveAttributes(condition, right),
                  EqualTo(rightKey, key)))

              Join(newLeft, right, if (positive) LeftSemi else LeftAnti, newCondition)
            }

          case InSubquery(key, Project(projectList, right), positive) =>
            // we don't support nested correlation yet, so the `right` must be resolved.
            checkAnalysis(right)
            if (projectList.length != 1) {
              throw new AnalysisException(
                s"Expect only 1 projection in In SubQuery Expression, but we got $projectList")
            } else {
              if (!projectList(0).resolved) {
                // We don't support reference in the outer column in the subquery projection list.
                // e.g. SELECT value FROM src a WHERE key in (SELECT b.key + a.key FROM src b)
                // That means, the project list of the subquery MUST BE resolved already, otherwise
                // throws exception.
                throw new AnalysisException(s"Cannot resolve the projection ${projectList(0)}")
              }
              val newLeft = others.reduceOption(And).map(Filter(_, left)).getOrElse(left)
              Join(newLeft, right,
                if (positive) LeftSemi else LeftAnti,
                Some(EqualTo(projectList(0), key)))
            }

          case InSubquery(key, right @ Aggregate(grouping, aggregations, child), positive) =>
            if (aggregations.length != 1) {
              throw new AnalysisException(
                s"Expect only 1 projection in In SubQuery Expression, but we got $aggregations")
            } else {
              // we don't support nested correlation yet, so the `child` must be resolved.
              checkAnalysis(child)
              val rightKey = ResolveReferences.tryResolveAttributes(aggregations(0), child) match {
                case e if !e.resolved =>
                  throw new AnalysisException(
                    s"Cannot resolve the aggregation $e")
                case e: NamedExpression => e
                case other =>
                  // place a space before `in_subquery_key`, hopefully end user
                  // will not take that as the field name or alias.
                  Alias(other, " in_subquery_key")()
              }

              val newLeft = others.reduceOption(And).map(Filter(_, left)).getOrElse(left)
              val newRight = Aggregate(grouping, rightKey :: Nil, child)
              val newCondition = Some(EqualTo(rightKey.toAttribute, key))

              Join(newLeft, newRight, if (positive) LeftSemi else LeftAnti, newCondition)
            }

          case InSubquery(key,
            f @ Filter(condition, right @ Aggregate(grouping, aggregations, child)), positive) =>
            if (aggregations.length != 1) {
              throw new AnalysisException(
                s"Expect only 1 projection in In Subquery Expression, but we got $aggregations")
            } else {
              // we don't support nested correlation yet, so the `child` must be resolved.
              checkAnalysis(child)
              val rightKey = ResolveReferences.tryResolveAttributes(aggregations(0), child) match {
                case e if !e.resolved =>
                  throw new AnalysisException(
                    s"Cannot resolve the aggregation $e")
                case e: NamedExpression => e
                case other => Alias(other, " in_subquery_key")()
              }

              val newLeft =
                Filter(others.foldLeft(
                  ResolveReferences.tryResolveAttributes(condition(0), child))(And(_, _)),
                  left)
              val newRight = Aggregate(grouping, rightKey :: Nil, child)
              val newCondition = Some(EqualTo(rightKey.toAttribute, key))
              Join(newLeft, newRight, if (positive) LeftSemi else LeftAnti, newCondition)
            }
        }
    }
  }

  /**
   * Replaces [[UnresolvedAttribute]]s with concrete [[AttributeReference]]s from
   * a logical plan node's children.
   */
  object ResolveReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p: LogicalPlan if !p.childrenResolved => p

      // If the projection list contains Stars, expand it.
      case p @ Project(projectList, child) if containsStar(projectList) =>
        Project(
          projectList.flatMap {
            case s: Star => s.expand(child.output, resolver)
            case UnresolvedAlias(f @ UnresolvedFunction(_, args, _)) if containsStar(args) =>
              val expandedArgs = args.flatMap {
                case s: Star => s.expand(child.output, resolver)
                case o => o :: Nil
              }
              UnresolvedAlias(child = f.copy(children = expandedArgs)) :: Nil
            case UnresolvedAlias(c @ CreateArray(args)) if containsStar(args) =>
              val expandedArgs = args.flatMap {
                case s: Star => s.expand(child.output, resolver)
                case o => o :: Nil
              }
              UnresolvedAlias(c.copy(children = expandedArgs)) :: Nil
            case UnresolvedAlias(c @ CreateStruct(args)) if containsStar(args) =>
              val expandedArgs = args.flatMap {
                case s: Star => s.expand(child.output, resolver)
                case o => o :: Nil
              }
              UnresolvedAlias(c.copy(children = expandedArgs)) :: Nil
            case o => o :: Nil
          },
          child)
      case t: ScriptTransformation if containsStar(t.input) =>
        t.copy(
          input = t.input.flatMap {
            case s: Star => s.expand(t.child.output, resolver)
            case o => o :: Nil
          }
        )

      // If the aggregate function argument contains Stars, expand it.
      case a: Aggregate if containsStar(a.aggregateExpressions) =>
        a.copy(
          aggregateExpressions = a.aggregateExpressions.flatMap {
            case s: Star => s.expand(a.child.output, resolver)
            case o => o :: Nil
          }
        )

      // Special handling for cases when self-join introduce duplicate expression ids.
      case j @ Join(left, right, _, _) if !j.selfJoinResolved =>
        val conflictingAttributes = left.outputSet.intersect(right.outputSet)
        logDebug(s"Conflicting attributes ${conflictingAttributes.mkString(",")} in $j")

        right.collect {
          // Handle base relations that might appear more than once.
          case oldVersion: MultiInstanceRelation
              if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
            val newVersion = oldVersion.newInstance()
            (oldVersion, newVersion)

          // Handle projects that create conflicting aliases.
          case oldVersion @ Project(projectList, _)
              if findAliases(projectList).intersect(conflictingAttributes).nonEmpty =>
            (oldVersion, oldVersion.copy(projectList = newAliases(projectList)))

          case oldVersion @ Aggregate(_, aggregateExpressions, _)
              if findAliases(aggregateExpressions).intersect(conflictingAttributes).nonEmpty =>
            (oldVersion, oldVersion.copy(aggregateExpressions = newAliases(aggregateExpressions)))

          case oldVersion: Generate
              if oldVersion.generatedSet.intersect(conflictingAttributes).nonEmpty =>
            val newOutput = oldVersion.generatorOutput.map(_.newInstance())
            (oldVersion, oldVersion.copy(generatorOutput = newOutput))

          case oldVersion @ Window(_, windowExpressions, _, _, child)
              if AttributeSet(windowExpressions.map(_.toAttribute)).intersect(conflictingAttributes)
                .nonEmpty =>
            (oldVersion, oldVersion.copy(windowExpressions = newAliases(windowExpressions)))
        }
        // Only handle first case, others will be fixed on the next pass.
        .headOption match {
          case None =>
            /*
             * No result implies that there is a logical plan node that produces new references
             * that this rule cannot handle. When that is the case, there must be another rule
             * that resolves these conflicts. Otherwise, the analysis will fail.
             */
            j
          case Some((oldRelation, newRelation)) =>
            val attributeRewrites = AttributeMap(oldRelation.output.zip(newRelation.output))
            val newRight = right transformUp {
              case r if r == oldRelation => newRelation
            } transformUp {
              case other => other transformExpressions {
                case a: Attribute => attributeRewrites.get(a).getOrElse(a)
              }
            }
            j.copy(right = newRight)
        }

      // When resolve `SortOrder`s in Sort based on child, don't report errors as
      // we still have chance to resolve it based on grandchild
      case s @ Sort(ordering, global, child) if child.resolved && !s.resolved =>
        val newOrdering = resolveSortOrders(ordering, child, throws = false)
        Sort(newOrdering, global, child)

      // A special case for Generate, because the output of Generate should not be resolved by
      // ResolveReferences. Attributes in the output will be resolved by ResolveGenerate.
      case g @ Generate(generator, join, outer, qualifier, output, child)
        if child.resolved && !generator.resolved =>
        val newG = generator transformUp {
          case u @ UnresolvedAttribute(nameParts) =>
            withPosition(u) { child.resolve(nameParts, resolver).getOrElse(u) }
          case UnresolvedExtractValue(child, fieldExpr) =>
            ExtractValue(child, fieldExpr, resolver)
        }
        if (newG.fastEquals(generator)) {
          g
        } else {
          Generate(newG.asInstanceOf[Generator], join, outer, qualifier, output, child)
        }

      case q: LogicalPlan =>
        logTrace(s"Attempting to resolve ${q.simpleString}")
        q transformExpressionsUp  {
          case u @ UnresolvedAlias(expr: NamedExpression) if expr.resolved => expr
          case u @ UnresolvedAttribute(nameParts) =>
            // Leave unchanged if resolution fails.  Hopefully will be resolved next round.
            val result =
              withPosition(u) { q.resolveChildren(nameParts, resolver).getOrElse(u) }
            logDebug(s"Resolving $u to $result")
            result
          case UnresolvedExtractValue(child, fieldExpr) if child.resolved =>
            ExtractValue(child, fieldExpr, resolver)
        }
    }

    // Try to resolve the attributes from the given logical plan
    // TODO share the code with above rules? How?
    def tryResolveAttributes(expr: Expression, q: LogicalPlan): Expression = {
      checkAnalysis(q)
      val projection = Project(q.output, q)

      logTrace(s"Attempting to resolve ${expr.simpleString}")
      expr transformUp  {
        case u @ UnresolvedAlias(expr) => expr
        case u @ UnresolvedAttribute(nameParts) =>
          // Leave unchanged if resolution fails.  Hopefully will be resolved next round.
          val result =
            withPosition(u) { projection.resolveChildren(nameParts, resolver).getOrElse(u) }
          logDebug(s"Resolving $u to $result")
          result
        case UnresolvedExtractValue(child, fieldExpr) if child.resolved =>
          ExtractValue(child, fieldExpr, resolver)
      }
    }

    def newAliases(expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
      expressions.map {
        case a: Alias => Alias(a.child, a.name)()
        case other => other
      }
    }

    def findAliases(projectList: Seq[NamedExpression]): AttributeSet = {
      AttributeSet(projectList.collect { case a: Alias => a.toAttribute })
    }

    /**
     * Returns true if `exprs` contains a [[Star]].
     */
    def containsStar(exprs: Seq[Expression]): Boolean =
      exprs.exists(_.collect { case _: Star => true }.nonEmpty)
  }

  private def resolveSortOrders(ordering: Seq[SortOrder], plan: LogicalPlan, throws: Boolean) = {
    ordering.map { order =>
      // Resolve SortOrder in one round.
      // If throws == false or the desired attribute doesn't exist
      // (like try to resolve `a.b` but `a` doesn't exist), fail and return the origin one.
      // Else, throw exception.
      try {
        val newOrder = order transformUp {
          case u @ UnresolvedAttribute(nameParts) =>
            plan.resolve(nameParts, resolver).getOrElse(u)
          case UnresolvedExtractValue(child, fieldName) if child.resolved =>
            ExtractValue(child, fieldName, resolver)
        }
        newOrder.asInstanceOf[SortOrder]
      } catch {
        case a: AnalysisException if !throws => order
      }
    }
  }

  /**
   * In many dialects of SQL it is valid to sort by attributes that are not present in the SELECT
   * clause.  This rule detects such queries and adds the required attributes to the original
   * projection, so that they will be available during sorting. Another projection is added to
   * remove these attributes after sorting.
   */
  object ResolveSortReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case s @ Sort(ordering, global, p @ Project(projectList, child))
          if !s.resolved && p.resolved =>
        val (newOrdering, missing) = resolveAndFindMissing(ordering, p, child)

        // If this rule was not a no-op, return the transformed plan, otherwise return the original.
        if (missing.nonEmpty) {
          // Add missing attributes and then project them away after the sort.
          Project(p.output,
            Sort(newOrdering, global,
              Project(projectList ++ missing, child)))
        } else {
          logDebug(s"Failed to find $missing in ${p.output.mkString(", ")}")
          s // Nothing we can do here. Return original plan.
        }
    }

    /**
     * Given a child and a grandchild that are present beneath a sort operator, try to resolve
     * the sort ordering and returns it with a list of attributes that are missing from the
     * child but are present in the grandchild.
     */
    def resolveAndFindMissing(
        ordering: Seq[SortOrder],
        child: LogicalPlan,
        grandchild: LogicalPlan): (Seq[SortOrder], Seq[Attribute]) = {
      val newOrdering = resolveSortOrders(ordering, grandchild, throws = true)
      // Construct a set that contains all of the attributes that we need to evaluate the
      // ordering.
      val requiredAttributes = AttributeSet(newOrdering).filter(_.resolved)
      // Figure out which ones are missing from the projection, so that we can add them and
      // remove them after the sort.
      val missingInProject = requiredAttributes -- child.output
      // It is important to return the new SortOrders here, instead of waiting for the standard
      // resolving process as adding attributes to the project below can actually introduce
      // ambiguity that was not present before.
      (newOrdering, missingInProject.toSeq)
    }
  }

  /**
   * Replaces [[UnresolvedFunction]]s with concrete [[Expression]]s.
   */
  object ResolveFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case q: LogicalPlan =>
        q transformExpressions {
          case u if !u.childrenResolved => u // Skip until children are resolved.
          case u @ UnresolvedFunction(name, children, isDistinct) =>
            withPosition(u) {
              registry.lookupFunction(name, children) match {
                // We get an aggregate function built based on AggregateFunction2 interface.
                // So, we wrap it in AggregateExpression2.
                case agg2: AggregateFunction2 => AggregateExpression2(agg2, Complete, isDistinct)
                // Currently, our old aggregate function interface supports SUM(DISTINCT ...)
                // and COUTN(DISTINCT ...).
                case sumDistinct: SumDistinct => sumDistinct
                case countDistinct: CountDistinct => countDistinct
                // DISTINCT is not meaningful with Max and Min.
                case max: Max if isDistinct => max
                case min: Min if isDistinct => min
                // For other aggregate functions, DISTINCT keyword is not supported for now.
                // Once we converted to the new code path, we will allow using DISTINCT keyword.
                case other: AggregateExpression1 if isDistinct =>
                  failAnalysis(s"$name does not support DISTINCT keyword.")
                // If it does not have DISTINCT keyword, we will return it as is.
                case other => other
              }
            }
        }
    }
  }

  /**
   * Turns projections that contain aggregate expressions into aggregations.
   */
  object GlobalAggregates extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case Project(projectList, child) if containsAggregates(projectList) =>
        Aggregate(Nil, projectList, child)
    }

    def containsAggregates(exprs: Seq[Expression]): Boolean = {
      exprs.foreach(_.foreach {
        case agg: AggregateExpression => return true
        case _ =>
      })
      false
    }
  }

  /**
   * This rule finds aggregate expressions that are not in an aggregate operator.  For example,
   * those in a HAVING clause or ORDER BY clause.  These expressions are pushed down to the
   * underlying aggregate operator and then projected away after the original operator.
   */
  object ResolveAggregateFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case filter @ Filter(havingCondition,
             aggregate @ Aggregate(grouping, originalAggExprs, child))
          if aggregate.resolved =>

        // Try resolving the condition of the filter as though it is in the aggregate clause
        val aggregatedCondition =
          Aggregate(grouping, Alias(havingCondition, "havingCondition")() :: Nil, child)
        val resolvedOperator = execute(aggregatedCondition)
        def resolvedAggregateFilter =
          resolvedOperator
            .asInstanceOf[Aggregate]
            .aggregateExpressions.head

        // If resolution was successful and we see the filter has an aggregate in it, add it to
        // the original aggregate operator.
        if (resolvedOperator.resolved && containsAggregate(resolvedAggregateFilter)) {
          val aggExprsWithHaving = resolvedAggregateFilter +: originalAggExprs

          Project(aggregate.output,
            Filter(resolvedAggregateFilter.toAttribute,
              aggregate.copy(aggregateExpressions = aggExprsWithHaving)))
        } else {
          filter
        }

      case sort @ Sort(sortOrder, global, aggregate: Aggregate)
        if aggregate.resolved =>

        // Try resolving the ordering as though it is in the aggregate clause.
        try {
          val aliasedOrdering = sortOrder.map(o => Alias(o.child, "aggOrder")())
          val aggregatedOrdering = aggregate.copy(aggregateExpressions = aliasedOrdering)
          val resolvedAggregate: Aggregate = execute(aggregatedOrdering).asInstanceOf[Aggregate]
          val resolvedAliasedOrdering: Seq[Alias] =
            resolvedAggregate.aggregateExpressions.asInstanceOf[Seq[Alias]]

          // If we pass the analysis check, then the ordering expressions should only reference to
          // aggregate expressions or grouping expressions, and it's safe to push them down to
          // Aggregate.
          checkAnalysis(resolvedAggregate)

          val originalAggExprs = aggregate.aggregateExpressions.map(
            CleanupAliases.trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])

          // If the ordering expression is same with original aggregate expression, we don't need
          // to push down this ordering expression and can reference the original aggregate
          // expression instead.
          val needsPushDown = ArrayBuffer.empty[NamedExpression]
          val evaluatedOrderings = resolvedAliasedOrdering.zip(sortOrder).map {
            case (evaluated, order) =>
              val index = originalAggExprs.indexWhere {
                case Alias(child, _) => child semanticEquals evaluated.child
                case other => other semanticEquals evaluated.child
              }

              if (index == -1) {
                needsPushDown += evaluated
                order.copy(child = evaluated.toAttribute)
              } else {
                order.copy(child = originalAggExprs(index).toAttribute)
              }
          }

          // Since we don't rely on sort.resolved as the stop condition for this rule,
          // we need to check this and prevent applying this rule multiple times
          if (sortOrder == evaluatedOrderings) {
            sort
          } else {
            Project(aggregate.output,
              Sort(evaluatedOrderings, global,
                aggregate.copy(aggregateExpressions = originalAggExprs ++ needsPushDown)))
          }
        } catch {
          // Attempting to resolve in the aggregate can result in ambiguity.  When this happens,
          // just return the original plan.
          case ae: AnalysisException => sort
        }
    }

    protected def containsAggregate(condition: Expression): Boolean = {
      condition.find(_.isInstanceOf[AggregateExpression]).isDefined
    }
  }

  /**
   * Rewrites table generating expressions that either need one or more of the following in order
   * to be resolved:
   *  - concrete attribute references for their output.
   *  - to be relocated from a SELECT clause (i.e. from  a [[Project]]) into a [[Generate]]).
   *
   * Names for the output [[Attribute]]s are extracted from [[Alias]] or [[MultiAlias]] expressions
   * that wrap the [[Generator]]. If more than one [[Generator]] is found in a Project, an
   * [[AnalysisException]] is throw.
   */
  object ResolveGenerate extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case g: Generate if ResolveReferences.containsStar(g.generator.children) =>
        failAnalysis("Cannot explode *, explode can only be applied on a specific column.")
      case p: Generate if !p.child.resolved || !p.generator.resolved => p
      case g: Generate if !g.resolved =>
        g.copy(generatorOutput = makeGeneratorOutput(g.generator, g.generatorOutput.map(_.name)))

      case p @ Project(projectList, child) =>
        // Holds the resolved generator, if one exists in the project list.
        var resolvedGenerator: Generate = null

        val newProjectList = projectList.flatMap {
          case AliasedGenerator(generator, names) if generator.childrenResolved =>
            if (resolvedGenerator != null) {
              failAnalysis(
                s"Only one generator allowed per select but ${resolvedGenerator.nodeName} and " +
                s"and ${generator.nodeName} found.")
            }

            resolvedGenerator =
              Generate(
                generator,
                join = projectList.size > 1, // Only join if there are other expressions in SELECT.
                outer = false,
                qualifier = None,
                generatorOutput = makeGeneratorOutput(generator, names),
                child)

            resolvedGenerator.generatorOutput
          case other => other :: Nil
        }

        if (resolvedGenerator != null) {
          Project(newProjectList, resolvedGenerator)
        } else {
          p
        }
    }

    /** Extracts a [[Generator]] expression and any names assigned by aliases to their output. */
    private object AliasedGenerator {
      def unapply(e: Expression): Option[(Generator, Seq[String])] = e match {
        case Alias(g: Generator, name) if g.resolved && g.elementTypes.size > 1 =>
          // If not given the default names, and the TGF with multiple output columns
          failAnalysis(
            s"""Expect multiple names given for ${g.getClass.getName},
               |but only single name '${name}' specified""".stripMargin)
        case Alias(g: Generator, name) if g.resolved => Some((g, name :: Nil))
        case MultiAlias(g: Generator, names) if g.resolved => Some(g, names)
        case _ => None
      }
    }

    /**
     * Construct the output attributes for a [[Generator]], given a list of names.  If the list of
     * names is empty names are assigned by ordinal (i.e., _c0, _c1, ...) to match Hive's defaults.
     */
    private def makeGeneratorOutput(
        generator: Generator,
        names: Seq[String]): Seq[Attribute] = {
      val elementTypes = generator.elementTypes

      if (names.length == elementTypes.length) {
        names.zip(elementTypes).map {
          case (name, (t, nullable)) =>
            AttributeReference(name, t, nullable)()
        }
      } else if (names.isEmpty) {
        elementTypes.zipWithIndex.map {
          // keep the default column names as Hive does _c0, _c1, _cN
          case ((t, nullable), i) => AttributeReference(s"_c$i", t, nullable)()
        }
      } else {
        failAnalysis(
          "The number of aliases supplied in the AS clause does not match the number of columns " +
          s"output by the UDTF expected ${elementTypes.size} aliases but got " +
          s"${names.mkString(",")} ")
      }
    }
  }

  /**
   * Extracts [[WindowExpression]]s from the projectList of a [[Project]] operator and
   * aggregateExpressions of an [[Aggregate]] operator and creates individual [[Window]]
   * operators for every distinct [[WindowSpecDefinition]].
   *
   * This rule handles three cases:
   *  - A [[Project]] having [[WindowExpression]]s in its projectList;
   *  - An [[Aggregate]] having [[WindowExpression]]s in its aggregateExpressions.
   *  - An [[Filter]]->[[Aggregate]] pattern representing GROUP BY with a HAVING
   *    clause and the [[Aggregate]] has [[WindowExpression]]s in its aggregateExpressions.
   * Note: If there is a GROUP BY clause in the query, aggregations and corresponding
   * filters (expressions in the HAVING clause) should be evaluated before any
   * [[WindowExpression]]. If a query has SELECT DISTINCT, the DISTINCT part should be
   * evaluated after all [[WindowExpression]]s.
   *
   * For every case, the transformation works as follows:
   * 1. For a list of [[Expression]]s (a projectList or an aggregateExpressions), partitions
   *    it two lists of [[Expression]]s, one for all [[WindowExpression]]s and another for
   *    all regular expressions.
   * 2. For all [[WindowExpression]]s, groups them based on their [[WindowSpecDefinition]]s.
   * 3. For every distinct [[WindowSpecDefinition]], creates a [[Window]] operator and inserts
   *    it into the plan tree.
   */
  object ExtractWindowExpressions extends Rule[LogicalPlan] {
    private def hasWindowFunction(projectList: Seq[NamedExpression]): Boolean =
      projectList.exists(hasWindowFunction)

    private def hasWindowFunction(expr: NamedExpression): Boolean = {
      expr.find {
        case window: WindowExpression => true
        case _ => false
      }.isDefined
    }

    /**
     * From a Seq of [[NamedExpression]]s, extract expressions containing window expressions and
     * other regular expressions that do not contain any window expression. For example, for
     * `col1, Sum(col2 + col3) OVER (PARTITION BY col4 ORDER BY col5)`, we will extract
     * `col1`, `col2 + col3`, `col4`, and `col5` out and replace their appearances in
     * the window expression as attribute references. So, the first returned value will be
     * `[Sum(_w0) OVER (PARTITION BY _w1 ORDER BY _w2)]` and the second returned value will be
     * [col1, col2 + col3 as _w0, col4 as _w1, col5 as _w2].
     *
     * @return (seq of expressions containing at lease one window expressions,
     *          seq of non-window expressions)
     */
    private def extract(
        expressions: Seq[NamedExpression]): (Seq[NamedExpression], Seq[NamedExpression]) = {
      // First, we partition the input expressions to two part. For the first part,
      // every expression in it contain at least one WindowExpression.
      // Expressions in the second part do not have any WindowExpression.
      val (expressionsWithWindowFunctions, regularExpressions) =
        expressions.partition(hasWindowFunction)

      // Then, we need to extract those regular expressions used in the WindowExpression.
      // For example, when we have col1 - Sum(col2 + col3) OVER (PARTITION BY col4 ORDER BY col5),
      // we need to make sure that col1 to col5 are all projected from the child of the Window
      // operator.
      val extractedExprBuffer = new ArrayBuffer[NamedExpression]()
      def extractExpr(expr: Expression): Expression = expr match {
        case ne: NamedExpression =>
          // If a named expression is not in regularExpressions, add it to
          // extractedExprBuffer and replace it with an AttributeReference.
          val missingExpr =
            AttributeSet(Seq(expr)) -- (regularExpressions ++ extractedExprBuffer)
          if (missingExpr.nonEmpty) {
            extractedExprBuffer += ne
          }
          ne.toAttribute
        case e: Expression if e.foldable =>
          e // No need to create an attribute reference if it will be evaluated as a Literal.
        case e: Expression =>
          // For other expressions, we extract it and replace it with an AttributeReference (with
          // an interal column name, e.g. "_w0").
          val withName = Alias(e, s"_w${extractedExprBuffer.length}")()
          extractedExprBuffer += withName
          withName.toAttribute
      }

      // Now, we extract regular expressions from expressionsWithWindowFunctions
      // by using extractExpr.
      val newExpressionsWithWindowFunctions = expressionsWithWindowFunctions.map {
        _.transform {
          // Extracts children expressions of a WindowFunction (input parameters of
          // a WindowFunction).
          case wf : WindowFunction =>
            val newChildren = wf.children.map(extractExpr(_))
            wf.withNewChildren(newChildren)

          // Extracts expressions from the partition spec and order spec.
          case wsc @ WindowSpecDefinition(partitionSpec, orderSpec, _) =>
            val newPartitionSpec = partitionSpec.map(extractExpr(_))
            val newOrderSpec = orderSpec.map { so =>
              val newChild = extractExpr(so.child)
              so.copy(child = newChild)
            }
            wsc.copy(partitionSpec = newPartitionSpec, orderSpec = newOrderSpec)

          // Extracts AggregateExpression. For example, for SUM(x) - Sum(y) OVER (...),
          // we need to extract SUM(x).
          case agg: AggregateExpression =>
            val withName = Alias(agg, s"_w${extractedExprBuffer.length}")()
            extractedExprBuffer += withName
            withName.toAttribute

          // Extracts other attributes
          case attr: Attribute => extractExpr(attr)

        }.asInstanceOf[NamedExpression]
      }

      (newExpressionsWithWindowFunctions, regularExpressions ++ extractedExprBuffer)
    } // end of extract

    /**
     * Adds operators for Window Expressions. Every Window operator handles a single Window Spec.
     */
    private def addWindow(
        expressionsWithWindowFunctions: Seq[NamedExpression],
        child: LogicalPlan): LogicalPlan = {
      // First, we need to extract all WindowExpressions from expressionsWithWindowFunctions
      // and put those extracted WindowExpressions to extractedWindowExprBuffer.
      // This step is needed because it is possible that an expression contains multiple
      // WindowExpressions with different Window Specs.
      // After extracting WindowExpressions, we need to construct a project list to generate
      // expressionsWithWindowFunctions based on extractedWindowExprBuffer.
      // For example, for "sum(a) over (...) / sum(b) over (...)", we will first extract
      // "sum(a) over (...)" and "sum(b) over (...)" out, and assign "_we0" as the alias to
      // "sum(a) over (...)" and "_we1" as the alias to "sum(b) over (...)".
      // Then, the projectList will be [_we0/_we1].
      val extractedWindowExprBuffer = new ArrayBuffer[NamedExpression]()
      val newExpressionsWithWindowFunctions = expressionsWithWindowFunctions.map {
        // We need to use transformDown because we want to trigger
        // "case alias @ Alias(window: WindowExpression, _)" first.
        _.transformDown {
          case alias @ Alias(window: WindowExpression, _) =>
            // If a WindowExpression has an assigned alias, just use it.
            extractedWindowExprBuffer += alias
            alias.toAttribute
          case window: WindowExpression =>
            // If there is no alias assigned to the WindowExpressions. We create an
            // internal column.
            val withName = Alias(window, s"_we${extractedWindowExprBuffer.length}")()
            extractedWindowExprBuffer += withName
            withName.toAttribute
        }.asInstanceOf[NamedExpression]
      }

      // Second, we group extractedWindowExprBuffer based on their Partition and Order Specs.
      val groupedWindowExpressions = extractedWindowExprBuffer.groupBy { expr =>
        val distinctWindowSpec = expr.collect {
          case window: WindowExpression => window.windowSpec
        }.distinct

        // We do a final check and see if we only have a single Window Spec defined in an
        // expressions.
        if (distinctWindowSpec.length == 0 ) {
          failAnalysis(s"$expr does not have any WindowExpression.")
        } else if (distinctWindowSpec.length > 1) {
          // newExpressionsWithWindowFunctions only have expressions with a single
          // WindowExpression. If we reach here, we have a bug.
          failAnalysis(s"$expr has multiple Window Specifications ($distinctWindowSpec)." +
            s"Please file a bug report with this error message, stack trace, and the query.")
        } else {
          val spec = distinctWindowSpec.head
          (spec.partitionSpec, spec.orderSpec)
        }
      }.toSeq

      // Third, for every Window Spec, we add a Window operator and set currentChild as the
      // child of it.
      var currentChild = child
      var i = 0
      while (i < groupedWindowExpressions.size) {
        val ((partitionSpec, orderSpec), windowExpressions) = groupedWindowExpressions(i)
        // Set currentChild to the newly created Window operator.
        currentChild =
          Window(
            currentChild.output,
            windowExpressions,
            partitionSpec,
            orderSpec,
            currentChild)

        // Move to next Window Spec.
        i += 1
      }

      // Finally, we create a Project to output currentChild's output
      // newExpressionsWithWindowFunctions.
      Project(currentChild.output ++ newExpressionsWithWindowFunctions, currentChild)
    } // end of addWindow

    // We have to use transformDown at here to make sure the rule of
    // "Aggregate with Having clause" will be triggered.
    def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {

      // Aggregate with Having clause. This rule works with an unresolved Aggregate because
      // a resolved Aggregate will not have Window Functions.
      case f @ Filter(condition, a @ Aggregate(groupingExprs, aggregateExprs, child))
        if child.resolved &&
           hasWindowFunction(aggregateExprs) &&
           a.expressions.forall(_.resolved) =>
        val (windowExpressions, aggregateExpressions) = extract(aggregateExprs)
        // Create an Aggregate operator to evaluate aggregation functions.
        val withAggregate = Aggregate(groupingExprs, aggregateExpressions, child)
        // Add a Filter operator for conditions in the Having clause.
        val withFilter = Filter(condition, withAggregate)
        val withWindow = addWindow(windowExpressions, withFilter)

        // Finally, generate output columns according to the original projectList.
        val finalProjectList = aggregateExprs.map (_.toAttribute)
        Project(finalProjectList, withWindow)

      case p: LogicalPlan if !p.childrenResolved => p

      // Aggregate without Having clause.
      case a @ Aggregate(groupingExprs, aggregateExprs, child)
        if hasWindowFunction(aggregateExprs) &&
           a.expressions.forall(_.resolved) =>
        val (windowExpressions, aggregateExpressions) = extract(aggregateExprs)
        // Create an Aggregate operator to evaluate aggregation functions.
        val withAggregate = Aggregate(groupingExprs, aggregateExpressions, child)
        // Add Window operators.
        val withWindow = addWindow(windowExpressions, withAggregate)

        // Finally, generate output columns according to the original projectList.
        val finalProjectList = aggregateExprs.map (_.toAttribute)
        Project(finalProjectList, withWindow)

      // We only extract Window Expressions after all expressions of the Project
      // have been resolved.
      case p @ Project(projectList, child)
        if hasWindowFunction(projectList) && !p.expressions.exists(!_.resolved) =>
        val (windowExpressions, regularExpressions) = extract(projectList)
        // We add a project to get all needed expressions for window expressions from the child
        // of the original Project operator.
        val withProject = Project(regularExpressions, child)
        // Add Window operators.
        val withWindow = addWindow(windowExpressions, withProject)

        // Finally, generate output columns according to the original projectList.
        val finalProjectList = projectList.map (_.toAttribute)
        Project(finalProjectList, withWindow)
    }
  }

  /**
   * Pulls out nondeterministic expressions from LogicalPlan which is not Project or Filter,
   * put them into an inner Project and finally project them away at the outer Project.
   */
  object PullOutNondeterministic extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p if !p.resolved => p // Skip unresolved nodes.
      case p: Project => p
      case f: Filter => f

      // todo: It's hard to write a general rule to pull out nondeterministic expressions
      // from LogicalPlan, currently we only do it for UnaryNode which has same output
      // schema with its child.
      case p: UnaryNode if p.output == p.child.output && p.expressions.exists(!_.deterministic) =>
        val nondeterministicExprs = p.expressions.filterNot(_.deterministic).flatMap { expr =>
          val leafNondeterministic = expr.collect {
            case n: Nondeterministic => n
          }
          leafNondeterministic.map { e =>
            val ne = e match {
              case n: NamedExpression => n
              case _ => Alias(e, "_nondeterministic")()
            }
            new TreeNodeRef(e) -> ne
          }
        }.toMap
        val newPlan = p.transformExpressions { case e =>
          nondeterministicExprs.get(new TreeNodeRef(e)).map(_.toAttribute).getOrElse(e)
        }
        val newChild = Project(p.child.output ++ nondeterministicExprs.values, p.child)
        Project(p.output, newPlan.withNewChildren(newChild :: Nil))
    }
  }
}

/**
 * Removes [[Subquery]] operators from the plan. Subqueries are only required to provide
 * scoping information for attributes and can be removed once analysis is complete.
 */
object EliminateSubQueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
  }
}

/**
 * Cleans up unnecessary Aliases inside the plan. Basically we only need Alias as a top level
 * expression in Project(project list) or Aggregate(aggregate expressions) or
 * Window(window expressions).
 */
object CleanupAliases extends Rule[LogicalPlan] {
  private def trimAliases(e: Expression): Expression = {
    var stop = false
    e.transformDown {
      // CreateStruct is a special case, we need to retain its top level Aliases as they decide the
      // name of StructField. We also need to stop transform down this expression, or the Aliases
      // under CreateStruct will be mistakenly trimmed.
      case c: CreateStruct if !stop =>
        stop = true
        c.copy(children = c.children.map(trimNonTopLevelAliases))
      case c: CreateStructUnsafe if !stop =>
        stop = true
        c.copy(children = c.children.map(trimNonTopLevelAliases))
      case Alias(child, _) if !stop => child
    }
  }

  def trimNonTopLevelAliases(e: Expression): Expression = e match {
    case a: Alias =>
      Alias(trimAliases(a.child), a.name)(a.exprId, a.qualifiers, a.explicitMetadata)
    case other => trimAliases(other)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case Project(projectList, child) =>
      val cleanedProjectList =
        projectList.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      Project(cleanedProjectList, child)

    case Aggregate(grouping, aggs, child) =>
      val cleanedAggs = aggs.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      Aggregate(grouping.map(trimAliases), cleanedAggs, child)

    case w @ Window(projectList, windowExprs, partitionSpec, orderSpec, child) =>
      val cleanedWindowExprs =
        windowExprs.map(e => trimNonTopLevelAliases(e).asInstanceOf[NamedExpression])
      Window(projectList, cleanedWindowExprs, partitionSpec.map(trimAliases),
        orderSpec.map(trimAliases(_).asInstanceOf[SortOrder]), child)

    case other =>
      var stop = false
      other transformExpressionsDown {
        case c: CreateStruct if !stop =>
          stop = true
          c.copy(children = c.children.map(trimNonTopLevelAliases))
        case c: CreateStructUnsafe if !stop =>
          stop = true
          c.copy(children = c.children.map(trimNonTopLevelAliases))
        case Alias(child, _) if !stop => child
      }
  }
}
