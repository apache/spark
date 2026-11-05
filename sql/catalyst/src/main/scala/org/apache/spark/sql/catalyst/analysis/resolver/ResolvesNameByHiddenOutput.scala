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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.HashSet

import scala.collection.mutable

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeReference,
  Expression,
  ExprId,
  LambdaFunction,
  NamedExpression,
  PipeOperator
}
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  Distinct,
  LogicalPlan,
  Project,
  SubqueryAlias,
  UnaryNode
}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.internal.SQLConf

/**
 * [[ResolvesNameByHiddenOutput]] is used by resolvers for operators that are able to resolve
 * attributes in its expression tree from hidden output or that can reference expressions not
 * present in child's output. Update child operator's output list and place a [[Project]] node on
 * top of original operator node with the original output of an operator's child.
 *
 * For example, in a following query:
 *
 * {{{
 * SELECT
 *  t1.key
 * FROM
 *  t1 FULL OUTER JOIN t2 USING (key)
 * WHERE
 *  t1.key NOT LIKE 'bb.%';
 * }}}
 *
 * Plan without adding missing attributes would be:
 *
 * {{{
 * +- Project [key#1]
 *    +- Filter NOT key#1 LIKE bb.%
 *       +- Project [coalesce(key#1, key#2) AS key#3, __key#1__, __key#2__]
 *          +- Join FullOuter, (key#1 = key#2)
 *             :- SubqueryAlias t1
 *             :  +- Relation t1[key#1]
 *             +- SubqueryAlias t2
 *                +- Relation t2[key#2]
 * }}}
 *
 * NOTE: __#key1__ and __key#2__ at the end of inner [[Project]] are metadata columns from the
 * full outer join. Even though they are present in the [[Project]] in single-pass, fixed-point
 * adds these columns after resolving missing input, so duplication of some columns is possible.
 * In order to stay fully compatible between single-pass and fixed-point, we add both missing
 * attributes and these metadata columns. We mimic fixed-point behavior by putting metadata
 * columns in [[NameScope.hiddenOutput]] instead of [[NameScope.output]].
 *
 * In the plan above, [[Filter]] requires key#1 in its condition, but key#1 is __not__ available
 * in the below [[Project]]'s output, even though key#1 is available in [[Join]]'s hidden output.
 * Because of that, we need to place key#1 in the project list, after original project list
 * expressions, but before metadata columns (to remain compatible with fixed-point). In order to
 * preserve initial output of [[Filter]], we place a [[Project]] node on top of this [[Filter]],
 * whose project list is the original output of the [[Project]] __below__ [[Filter]] (in this
 * case - key#3 and metadata columns key#1 and key#2).
 *
 * Therefore, the plan becomes:
 *
 * {{{
 * +- Project [key#1]
 *    +- Project [key#3, key#1, key#2]
 *       +- Filter NOT key#1 LIKE bb.%
 *          +- Project [coalesce(key#1, key#2) AS key#3, key#1, key#1, key#2]
 *             +- Join FullOuter, (key#1 = key#2)
 *                :- SubqueryAlias t1
 *                :  +- Relation t1[key#1]
 *                +- SubqueryAlias t2
 *                   +- Relation t2[key#2]
 * }}}
 *
 * Query below exhibits similar behavior when [[Sort]] operator resolves an attribute using hidden
 * output:
 *
 * {{{ SELECT col1 FROM VALUES (1, 2) ORDER BY col2; }}}
 *
 * Unresolved plan would be:
 *
 * {{{
 * Sort [col2 ASC NULLS FIRST], true
 *   +- Project [col1]
 *     +- LocalRelation [col1, col2]
 * }}}
 *
 * As it can be seen, attribute `col2` used in [[Sort]] can't be resolved using the [[Project]]
 * output (which is [`col1`]), so it has to be resolved using the hidden output (which is
 * propagated from [[LocalRelation]] and is [`col1`, `col2`]). As it's been shown in the previous
 * example, `col2` has to be added to [[Project]] list and a [[Project]] with original output of
 * the [[Project]] below [[Sort]] is added as a top node. Because of that, analyzed plan is:
 *
 * {{{
 * Project [col1]
 *   +- Sort [col2 ASC NULLS FIRST], true
 *     +- Project [col1, col2]
 *       +- LocalRelation [col1, col2]
 * }}}
 *
 * Another example is when [[Sort]] order expression is an [[AggregateExpression]] which is not
 * present in the [[Aggregate.aggregateExpressions]]:
 *
 * {{{
 * SELECT col1 FROM VALUES (1) GROUP BY col1 ORDER BY sum(col1);
 * }}}
 *
 * In this example `sum(col1)` should be added to child's output and a [[Project]] node should be
 * added on top of the [[Sort]] node to preserve the original output of the [[Aggregate]] node:
 *
 * Project [col1]
 *   +- Sort [sum(col1)#... ASC NULLS FIRST], true
 *     +- Aggregate [col1], [col1, sum(col1) AS sum(col1)#...]
 *       +- LocalRelation [col1]
 *
 * In case of Dataframe programs we can have multiple [[Sort]] operators nested inside each other.
 * For example:
 *
 * {{{
 *   df.select("col1").orderBy("col2").orderBy("col1").orderBy("col2")
 * }}}
 *
 * Unresolved plan would be:
 *
 * {{{
 * Sort [col2 ASC NULLS FIRST], true
 *   +- Sort [col1 ASC NULLS FIRST], true
 *     +- Project [col1]
 *       +- Sort [col2 ASC NULLS FIRST], true
 *         +- Project [col1, col2]
 *           +- Project [col1, col2, col3]
 *             +- LocalRelation [col1, col2, col3]
 * }}}
 *
 * As it can be seen, `col2` ([[Sort]] order expression) needs to be resolved using the hidden
 * output. Because of that it must be added to all the [[Project]]s and [[Aggregate]]s
 * below the [[Sort]] operator.
 * Resolved plan would be:
 *
 * {{{
 * Project [col1]
 *   +- Sort [col2 ASC NULLS FIRST], true
 *     +- Sort [col1 ASC NULLS FIRST], true
 *       +- Project [col1, col2]
 *         +- Sort [col2 ASC NULLS FIRST], true
 *           +- Project [col1, col2]
 *             +- Project [col1, col2, col3]
 *               +- LocalRelation [col1, col2, col3]
 * }}}
 *
 * In the plan you can see that `col2` is added to the lower [[Project.projectList]].
 *
 * Missing expressions can appear in the DISTRIBUTE BY clause as well ([[RepartitionByExpression]]
 * operator).
 * In the following query:
 *
 * {{{
 *   SELECT col1 FROM values(1, 2) WHERE col2 = 1 DISTRIBUTE BY col2;
 * }}}
 *
 * Parsed plan looks like:
 *
 * {{{
 *   'RepartitionByExpression ['col2]
 *   +- 'Project ['col1]
 *      +- 'Filter ('col2 = 1)
 *         +- LocalRelation [col1#0, col2#1]
 * }}}
 *
 * Here `col2` from the `DISTRIBUTE BY` would be resolved using the hidden output and thus
 * the analyzed plan looks like:
 *
 * {{{
 *   Project [col1#0]
 *   +- RepartitionByExpression [col2#1]
 *      +- Project [col1#0, col2#1]
 *         +- Filter (col2#1 = 1)
 *            +- LocalRelation [col1#0, col2#1]
 * }}}
 *
 * As it can be seen, `col2` is added to the [[Project.projectList]] below
 * [[RepartitionByExpression]].
 */
trait ResolvesNameByHiddenOutput extends SQLConfHelper {

  /**
   * Insert the missing expressions in the output list of the operator. Recursively call
   * `expandOperatorsOutputList` to expand the output lists of [[Project]]s
   * and [[Aggregate]]s below the current one.
   * In order to stay compatible with fixed-point, missing expressions are inserted after the
   * original output list, but before any qualified access only columns that have been added as
   * part of resolution from hidden output.
   *
   * Only [[AttributeReference]]s are propagated recursively in `expandOperatorsOutputList`.
   * [[Alias]]es are meant to be inserted in the topmost operator. For example, [[SortResolver]]
   * may push down aliased aggregate and grouping expression trees to the immediate [[Aggregate]]
   * below, but it does not make sense to push them down further:
   *
   * {{{
   * -- The MAX(v2.col1) aggregate has to be pushed down to [[Aggregate]], but not to [[Project]]
   * -- below it.
   * SELECT COUNT(col1) FROM v1 NATURAL JOIN v2 GROUP BY col1 ORDER BY MAX(v2.col1);
   * }}}
   *
   * ->
   *
   * {{{
   * Project [count(col1)#23L]
   * +- Sort [max(col1)#25 ASC NULLS FIRST], true
   *   +- Aggregate [col1#21], [count(col1#21) AS count(col1)#23L, max(col1#20) AS max(col1)#25]
   *     +- Project [col1#21, col1#20]
   *       ...
   * }}}
   */
  def insertMissingExpressions(
      operator: LogicalPlan,
      missingExpressions: Seq[NamedExpression]): LogicalPlan =
    operator match {
      case expandableOperator: UnaryNode
          if shouldExtendOperatorOutput(expandableOperator, missingExpressions) =>
        expandableOperator match {
          case project: Project =>
            expandOperatorsOutputList(
              operator = project,
              operatorOutput = project.projectList,
              missingExpressions = missingExpressions
            )
          case aggregate: Aggregate =>
            expandOperatorsOutputList(
              operator = aggregate,
              operatorOutput = aggregate.aggregateExpressions,
              missingExpressions = missingExpressions
            )
          case other =>
            other.withNewChildren(
              Seq(insertMissingExpressions(expandableOperator.child, missingExpressions))
            )
        }
      case pipeOperator: PipeOperator =>
        failIfMissingExpressionsBelowPipeOperator(pipeOperator, missingExpressions)

        pipeOperator
      case other => other
    }

  /**
   * The fixed-point Analyzer resolves attributes referenced in lambda function bodies one
   * iteration later than the rest of the expression tree. If attributes missing from the child
   * output are referenced both inside and outside of lambda function bodies, it inserts them in two
   * separate iterations, each time placing an additional [[Project]] on top of the operator, which
   * single-pass doesn't replicate. Fail with [[ExplicitlyUnsupportedResolverFeature]] in that case.
   * For example:
   * {{{
   *   df.select("x").filter("exists(i, x -> x % d == 0)")
   * }}}
   */
  protected def failIfMissingAttributesInsideAndOutsideLambdaFunctions(
      expressions: Seq[Expression],
      missingAttributes: Seq[Attribute]): Unit = {
    if (missingAttributes.nonEmpty) {
      val idsInsideLambdaFunctions = new HashSet[ExprId]
      val idsOutsideLambdaFunctions = new HashSet[ExprId]

      def collectReferences(expression: Expression, isInsideLambdaFunction: Boolean): Unit =
        expression match {
          case lambdaFunction: LambdaFunction =>
            collectReferences(lambdaFunction.function, isInsideLambdaFunction = true)
          case attribute: AttributeReference =>
            if (isInsideLambdaFunction) {
              idsInsideLambdaFunctions.add(attribute.exprId)
            } else {
              idsOutsideLambdaFunctions.add(attribute.exprId)
            }
          case other =>
            other.children.foreach(collectReferences(_, isInsideLambdaFunction))
        }

      expressions.foreach(collectReferences(_, isInsideLambdaFunction = false))

      val hasMissingAttributesOutsideLambdaFunctions =
        missingAttributes.exists(attribute => idsOutsideLambdaFunctions.contains(attribute.exprId))
      val hasMissingAttributesOnlyInsideLambdaFunctions = missingAttributes.exists { attribute =>
        idsInsideLambdaFunctions.contains(attribute.exprId) &&
        !idsOutsideLambdaFunctions.contains(attribute.exprId)
      }

      if (hasMissingAttributesOutsideLambdaFunctions &&
        hasMissingAttributesOnlyInsideLambdaFunctions) {
        throw new ExplicitlyUnsupportedResolverFeature(
          ResolutionGap.MissingAttributesInsideAndOutsideLambdaFunctions
        )
      }
    }
  }

  /**
   * Missing expressions are not inserted below a [[PipeOperator]]. However, the fixed-point
   * Analyzer can still propagate qualified access only columns (e.g. the hidden `USING` join keys)
   * referenced above it into the [[Project]]s below it. For example:
   * {{{
   * VALUES (0), (1) lhs(a)
   * |> INNER JOIN VALUES (1), (2) rhs(a) USING (a)
   * |> EXTEND rhs.a AS z
   * |> DROP z
   * |> ORDER BY rhs.a;
   * }}}
   * Single-pass doesn't support this yet, so fail with [[ExplicitlyUnsupportedResolverFeature]] if
   * some of the missing expressions are not in the [[PipeOperator]] output.
   */
  private def failIfMissingExpressionsBelowPipeOperator(
      pipeOperator: PipeOperator,
      missingExpressions: Seq[NamedExpression]): Unit = {
    val pipeOperatorOutputIds = new HashSet[ExprId](pipeOperator.output.size)
    pipeOperator.output.foreach(attribute => pipeOperatorOutputIds.add(attribute.exprId))
    if (missingExpressions.exists(
        expression => !pipeOperatorOutputIds.contains(expression.exprId)
      )) {
      throw new ExplicitlyUnsupportedResolverFeature(
        ResolutionGap.MissingExpressionsBelowPipeOperator
      )
    }
  }

  private def expandOperatorsOutputList(
      operator: UnaryNode,
      operatorOutput: Seq[NamedExpression],
      missingExpressions: Seq[NamedExpression]): LogicalPlan = {
    val filteredMissingExpressions = filterMissingExpressions(
      operatorOutput = operatorOutput,
      missingExpressions = missingExpressions
    )

    if (filteredMissingExpressions.nonEmpty) {
      val (metadataCols, nonMetadataCols) =
        operatorOutput.partition(_.toAttribute.qualifiedAccessOnly)

      val expandedOperator = operator match {
        case aggregate: Aggregate =>
          val newAggregateList = nonMetadataCols ++ filteredMissingExpressions ++ metadataCols
          aggregate.copy(aggregateExpressions = newAggregateList)
        case project: Project =>
          val expandedChild = insertMissingExpressions(
            operator = operator.child,
            missingExpressions = filteredMissingExpressions
          )
          val newProjectList =
            nonMetadataCols ++ filteredMissingExpressions.map(_.toAttribute) ++ metadataCols

          project.copy(projectList = newProjectList, child = expandedChild)
      }

      checkMissingInput(expandedOperator)

      expandedOperator
    } else {
      operator
    }
  }

  /**
   * Rejects an expanded `operator` whose child does not produce an appended hidden-output
   * expression, e.g. an ORDER BY on a column an operator re-outputs under a fresh id.
   */
  private def checkMissingInput(operator: LogicalPlan): Unit = {
    val missingInput = operator.missingInput
    if (missingInput.nonEmpty) {
      Resolver.throwMissingAttributesError(operator, missingInput)
    }
  }

  protected def filterMissingExpressions(
      operatorOutput: Seq[NamedExpression],
      missingExpressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    val operatorOutputExpressionIdSet = new HashSet[ExprId]

    operatorOutput.foreach { element =>
      operatorOutputExpressionIdSet.add(element.exprId)
    }

    val filteredMissingExpressions = new mutable.ArrayBuffer[NamedExpression]()

    missingExpressions.foreach { element =>
      if (!operatorOutputExpressionIdSet.contains(element.exprId)) {
        filteredMissingExpressions += element
      }
    }

    filteredMissingExpressions.toSeq
  }

  private def shouldExtendOperatorOutput(
      unaryNode: UnaryNode,
      missingExpressions: Seq[NamedExpression]): Boolean = {
    val isOperatorExtendable = unaryNode match {
      case _: PipeOperator | _: Distinct | _: SubqueryAlias => false
      case _ => true
    }
    isOperatorExtendable && missingExpressions.nonEmpty
  }

  /**
   * Builds an operator output after hidden output attributes have been inserted into its child.
   *
   * For example:
   *
   * {{{
   * SELECT r.col1 AS a, a + 1
   * FROM t5 JOIN t5 AS r USING (col1, col2)
   * WHERE r.col2 IS NULL
   * }}}
   *
   * `r.col2` is hidden by the `USING` join output, but the project must still carry it through
   * the lower LCA projects so the child remains valid after [[insertMissingExpressions]] expands
   * it. Starting LCA resolution from `scopes.current.output` alone would omit such newly inserted
   * hidden attributes. This method appends the missing hidden attributes to the regular output and
   * keeps qualified-access-only attributes at the end, preserving the output shape expected by
   * hidden output pruning.
   */
  protected def outputWithMissingAttributes(
      output: Seq[Attribute],
      missingAttributes: Seq[Attribute]): Seq[Attribute] = {
    val filteredMissingAttributes = filterMissingExpressions(
      operatorOutput = output,
      missingExpressions = missingAttributes
    ).map(_.toAttribute.markAsAllowAnyAccess())
    val (qualifiedAccessOnlyAttributes, regularAttributes) =
      output.partition(_.qualifiedAccessOnly)

    regularAttributes ++ filteredMissingAttributes ++ qualifiedAccessOnlyAttributes
  }

  /**
   * If [[missingExpressions]] is not empty, output of an operator has been changed by
   * [[insertMissingExpressions]]. Therefore, we need to restore the original output, by placing a
   * [[Project]] on top of an original node, with original's node output. In that case, we set
   * [[OperatorResolutionContext.baseOperator]] to the original operator so we can validate it
   * later in the [[Resolver]]. Additionally, we append all qualified access only columns from
   * hidden output that were inserted as missing attributes, because they may be needed in upper
   * operators (if not, they will be pruned away in [[PruneMetadataColumns]]). Other hidden
   * attributes are thrown away, because we cannot reference them from the new [[Project]] (they
   * are not outputted from below). [[NameScope.qualifiedAccessOnlyOutputIds]] is overwritten with
   * the ids of the appended qualified access only columns.
   *
   * If [[SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_PREVENT_USING_ALIASES_FROM_NON_DIRECT_CHILDREN]] is
   * set to true, we need to clear the base [[Aggregate]] and its aliases in the current
   * [[QueryBlockScope]]. This is needed in order to prevent later replacement of Sort/Having
   * expressions using semantically equal aliased expressions from non-direct children. For
   * example, in the following query:
   *
   * {{{ SELECT col1 AS a FROM VALUES(1,2) GROUP BY col1, col2 HAVING col2 > 1 ORDER BY col1; }}}
   *
   * With flag set to false, analyzed plan will be:
   *
   * Sort [a#3 ASC NULLS FIRST], true
   * +- Project [a#3]
   *    +- Filter (col2#2 > 1)
   *       +- Aggregate [col1#1, col2#2], [col1#1 AS a#3, col2#2, col1#1]
   *          +- LocalRelation [col1#1, col2#2]
   *
   * Instead of using missing attribute `col1#1` we can use its alias `a#3` in the [[Sort]] and
   * avoid adding an extra projection. This is because all of [[Sort]], [[Project]], [[Filter]] and
   * [[Aggregate]] belong to the same query block since [[Project]] was artificially inserted.
   *
   * However, fixed-point can't handle this case properly and produces the following plan:
   *
   * Project [a#3]
   * +- Sort [col1#1 ASC NULLS FIRST], true
   *    +- Project [a#3, col1#1]
   *       +- Filter (col2#2 > 1)
   *          +- Aggregate [col1#1, col2#2], [col1#1 AS a#3, col2#2, col1#1]
   *             +- LocalRelation [col1#1, col2#2]
   *
   * Therefore, we need to match this behavior of fixed-point in single-pass in order to avoid
   * logical plan mismatches.
   */
  def retainOriginalOutput(
      operator: LogicalPlan,
      missingExpressions: Seq[NamedExpression],
      scopes: NameScopeStack,
      operatorResolutionContextStack: OperatorResolutionContextStack): LogicalPlan = {
    if (missingExpressions.isEmpty) {
      operator
    } else {
      operatorResolutionContextStack.current.baseOperator = Some(operator)

      val missingExpressionIds = new HashSet[ExprId](missingExpressions.size)
      missingExpressions.foreach { expression =>
        missingExpressionIds.add(expression.exprId)
      }

      val hiddenOutputToPreserve = scopes.current.hiddenOutput.filter { hiddenAttribute =>
        hiddenAttribute.qualifiedAccessOnly && missingExpressionIds.contains(
          hiddenAttribute.exprId
        )
      }

      val project = Project(
        projectList = scopes.current.output ++ hiddenOutputToPreserve,
        child = operator
      )

      val qualifiedAccessOnlyOutputIds = new HashSet[ExprId](hiddenOutputToPreserve.size)
      hiddenOutputToPreserve.foreach { attribute =>
        qualifiedAccessOnlyOutputIds.add(attribute.exprId)
      }
      scopes.overwriteCurrent(qualifiedAccessOnlyOutputIds = Some(qualifiedAccessOnlyOutputIds))

      if (conf.getConf(
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_PREVENT_USING_ALIASES_FROM_NON_DIRECT_CHILDREN
        )) {
        scopes.current.queryBlockScope.clearAggregateState()
      }

      project
    }
  }
}
