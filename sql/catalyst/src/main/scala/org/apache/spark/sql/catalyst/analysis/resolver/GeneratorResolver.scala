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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.analysis.MultiAlias
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Generator, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Generate, LogicalPlan, Project}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Resolver for [[Generator]] expressions. There are 2 distinct contexts they could be in:
 *  - As a part of the [[Generate]] operator. No special resolution is required. See [[resolve]].
 *  - As a part of project list. Could be part of [[Project]] or [[Aggregate]] resolution. In this
 *    case we have to rewrite a query and extract generator into new [[Generate]] node. For example:
 *    {{{
 *      SELECT explode(array(1, 2, 3));
 *
 *      == Parsed Logical Plan ==
 *      'Project [unresolvedalias('explode('array(1, 2, 3)))]
 *      +- OneRowRelation
 *
 *      == Analyzed Logical Plan ==
 *      col: int
 *      Project [col#1]
 *      +- Generate explode(array(1, 2, 3)), false, [col#1]
 *         +- OneRowRelation
 *    }}}
 *    For more info see [[resolveProjectListWithGenerators]] and [[GeneratorResolver.extract]].
 *
 *  [[Generator]]s have 2 main invariants:
 *   - In [[Aggregate.aggregateExpressions]] list there cannot be more than one [[Generate]]. See
 *     [[validateNoMoreThenOneGenerator()]].
 *   - [[Generator]] can never be nested in other expressions other than aliases. In other words, if
 *     an expression tree has a [[Generator]], it must be the root or child of root alias. See
 *     [[validateNoNestedGenerator]] and [[handleResolvedGenerator]].
 */
class GeneratorResolver(expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[Generator, Expression]
    with ResolvesExpressionChildren
    with CoercesExpressionTypes
    with ResolvesNameByHiddenOutput {

  private val scopes: NameScopeStack = expressionResolver.getNameScopes
  private val traversals = expressionResolver.getExpressionTreeTraversals
  private val expressionResolutionContextStack =
    expressionResolver.getExpressionResolutionContextStack
  private val operatorResolutionContextStack =
    expressionResolver.getOperatorResolutionContextStack

  /**
   * Resolve [[Generator]] as simple expression with type coercion.
   *
   * Note that generators are usually presented in parsed query as [[UnresolvedFunction]] and
   * therefore are usually resolved inside [[FunctionResolver]]. This resolve is used mostly for
   * generators produced during resolution of [[UnresolvedTableValuedFunction]] or during subsequent
   * resolutions.
   */
  override def resolve(generator: Generator): Expression = {
    val generatorWithResolvedChildren = withResolvedChildren(
      unresolvedExpression = generator,
      resolveChild = expressionResolver.resolve _
    ).asInstanceOf[Generator]

    handleResolvedGenerator(generatorWithResolvedChildren)

    coerceExpressionTypes(
      expression = generatorWithResolvedChildren,
      expressionTreeTraversal = traversals.current
    )
  }

  /**
   * Handle a resolved [[Generator]] expression:
   *  - Throws an error if the generator is used outside of supported operators
   *  - Throws an error if nested generators are detected
   *  - Marks that we encountered a generator expression
   *
   * @param generator The resolved generator expression
   */
  def handleResolvedGenerator(generator: Generator): Unit = {
    validateGeneratorParentOperator()

    val expressionResolutionContext = expressionResolutionContextStack.peek()
    if (expressionResolutionContext.hasGeneratorExpressions) {
      throw QueryCompilationErrors.nestedGeneratorError(generator)
    }
    expressionResolutionContext.hasGeneratorExpressions = true
  }

  /**
   * Validate that there are no nested generators in an expression. Together with the check in
   * [[handleResolvedGenerator]], this enforces generators to be top-level expressions (except
   * aliases).
   */
  def validateNoNestedGenerator(expression: Expression): Unit = {
    val hasGenerator = expressionResolutionContextStack.peek().hasGeneratorExpressions
    if (hasGenerator && !hasGeneratorAtTopLevel(expression)) {
      throw QueryCompilationErrors.nestedGeneratorError(trimAlias(expression))
    }
  }

  /**
   * Validate that there are not multiple generators in an expression list.
   */
  def validateNoMoreThenOneGenerator(expressions: Seq[NamedExpression]): Unit = {
    val generators = expressions.filter(hasGeneratorAtTopLevel).map(trimAlias)
    if (generators.size > 1) {
      throw QueryCompilationErrors.moreThanOneGeneratorError(generators)
    }
  }

  /**
   * Builds [[Generate]] operators per each [[Generator]] expressions in the project list from left
   * to right. In simple case without hidden attributes, it creates a chain of operators from bottom
   * to top: child -> N * [[Generate]] -> [[Project]].
   *
   * For example:
   * {{{
   *   SELECT id, explode(array(1, 2)), posexplode(array('a', 'b')) FROM range(1)
   * }}}
   * Becomes:
   * {{{
   *   Project [id#0, col#1, pos#2, col#3]
   *   +- Generate posexplode(array(a, b)), false, [pos#2, col#3]
   *      +- Generate explode(array(1, 2)), false, [col#1]
   *         +- Range (0, 1, step=1)
   * }}}
   *
   * In complex case where generators reference hidden attributes (e.g., from USING joins), we use
   * [[ResolvesNameByHiddenOutput]] trait methods to:
   *  - Expand child operators to include missing hidden attributes via [[insertMissingExpressions]]
   *  - Add compensating [[Project]] operators via [[retainOriginalOutput]] to preserve the correct
   *    output at each level
   * The pattern would be: child -> N * ([[Generate]] -> [[Project]]?) -> [[Project]]
   *
   * For example:
   * {{{
   *   SELECT
   *     explode(array(1)) AS x1,
   *     explode(array(t1.c1, t2.c1)) AS x2,
   *     explode(array(t2.c1)) AS x3
   *   FROM (VALUES (1), (2)) AS t1(c1)
   *   FULL OUTER JOIN (VALUES (2), (3)) AS t2(c1)
   *   USING (c1)
   * }}}
   * Becomes:
   * {{{
   *  Project [x1#8, x2#9, x3#10]
   *  +- Project [c1#7, x1#8, x2#9, x3#10]
   *     +- Generate explode(array(c1#6)), false, [x3#10]
   *        +- Project [c1#7, x1#8, x2#9, c1#6]
   *           +- Generate explode(array(c1#5, c1#6)), false, [x2#9]
   *              +- Generate explode(array(1)), false, [x1#8]
   *                 +- Project [coalesce(c1#5, c1#6) AS c1#7, c1#5, c1#6]
   *                    +- Join FullOuter, (c1#5 = c1#6)
   *                       :- SubqueryAlias t1
   *                       :  +- Project [col1#0 AS c1#5]
   *                       :     +- LocalRelation [col1#0]
   *                       +- SubqueryAlias t2
   *                          +- Project [col1#1 AS c1#6]
   *                             +- LocalRelation [col1#1]
   * }}}
   * Let's break down this plan from bottom to top:
   *  - `Project [coalesce(c1#5, c1#6) AS c1#7, c1#5, c1#6]` has c1#7 as the visible attribute and
   *    c1#5, c1#6 as hidden attributes from the join.
   *  - The first `Generate explode(array(1))` does not reference any hidden attributes, so it does
   *    not need to have a compensating project above it.
   *  - The second `Generate explode(array(c1#5, c1#6))` references hidden attributes c1#5 and c1#6,
   *    so we need to compensate it with `Project [c1#7, x1#8, x2#9, c1#6]` where:
   *    - c1#7 & x1#8 are the part of visible output of previous operators
   *    - x2#9 is the output of the current generator
   *    - c1#6 is the hidden attribute needed by the generator above (or one of generators in
   *      general cases)
   *  - The third `Generate explode(array(c1#6))` references hidden attribute c1#6, so we need to
   *    add another compensating `Project [c1#7, x1#8, x2#9, x3#10]` with only normal visible output
   *    of the whole chain.
   *  - In the end, we have the top-level `Project [x1#8, x2#9, x3#10]` that we would have
   *    regardless of hidden attributes.
   *
   * @param expressions The project list containing [[AliasedGenerator]]s and regular expressions.
   * @param childOperator The child operator to build generators on top of.
   * @return The top-level [[Project]] operator with [[Generate]] operators underneath.
   */
  def resolveProjectListWithGenerators(
      expressions: Seq[NamedExpression],
      childOperator: LogicalPlan): Project = {
    val topProjectListBuffer = new ArrayBuffer[NamedExpression]

    val finalChild = expressions.foldLeft(childOperator) { (currentChild, expr) =>
      expr match {
        case aliasedGenerator: AliasedGenerator =>
          val expandedChild =
            insertMissingExpressions(currentChild, aliasedGenerator.missingInput)

          val generate = Generate(
            generator = aliasedGenerator.child,
            unrequiredChildIndex = Nil,
            outer = aliasedGenerator.outer,
            qualifier = None,
            generatorOutput = aliasedGenerator.generatorOutput,
            child = expandedChild
          )
          topProjectListBuffer ++= generate.nullableOutput

          scopes.overwriteCurrent(
            output = Some(scopes.current.output ++ generate.qualifiedGeneratorOutput)
          )

          retainOriginalOutput(
            operator = generate,
            missingExpressions = aliasedGenerator.missingInput,
            scopes = scopes,
            operatorResolutionContextStack = operatorResolutionContextStack
          )
        case other =>
          topProjectListBuffer += other
          currentChild
      }
    }

    Project(topProjectListBuffer.toSeq, finalChild)
  }

  /**
   * Validates that a generator is only used within allowed operators: [[Generate]], [[Project]],
   * or [[Aggregate]]. Throws [[generatorOutsideSelectError]] otherwise.
   */
  private def validateGeneratorParentOperator(): Unit = {
    val parentOperator = traversals.current.parentOperator
    parentOperator match {
      case _: Generate | _: Project | _: Aggregate =>
      case _ =>
        throw QueryCompilationErrors.generatorOutsideSelectError(parentOperator)
    }
  }

  /**
   * Extract the generator expression from a [[NamedExpression]], stripping any alias wrappers.
   */
  private def trimAlias(expression: Expression): Expression = expression match {
    case aliasedGenerator: AliasedGenerator => aliasedGenerator.child
    case Alias(child, _) => child
    case MultiAlias(child, _) => child
    case other => other
  }

  /**
   * Check if an expression has a generator directly at the top level or wrapped in alias.
   */
  private def hasGeneratorAtTopLevel(expression: Expression): Boolean = expression match {
    case _: AliasedGenerator => true
    case Alias(_: Generator, _) => true
    case MultiAlias(_: Generator, _) => true
    case _: Generator => true
    case _ => false
  }
}

object GeneratorResolver {

  /**
   * Validates that generator expressions are not used with lateral column aliases or window
   * expressions. Throws [[ExplicitlyUnsupportedResolverFeature]] otherwise.
   */
  def checkCanResolveGeneratorExpressions(
      hasGeneratorExpressions: Boolean,
      hasLateralColumnAlias: Boolean,
      hasWindowExpressions: Boolean): Unit = {
    if (hasGeneratorExpressions) {
      if (hasLateralColumnAlias) {
        throw new ExplicitlyUnsupportedResolverFeature(
          "Generator expressions with LCAs are not supported"
        )
      }
      if (hasWindowExpressions) {
        throw new ExplicitlyUnsupportedResolverFeature(
          "Generator expressions with window expressions are not supported"
        )
      }
    }
  }

  /**
   * Extracts generator children and wraps them in aliases. This is done to calculate all
   * necessary aggregate expressions in the [[Aggregate]] and later use them by the [[Generate]]
   * operator.
   *
   * For each non-foldable child, creates aliases like `_gen_input_N` that will be added to
   * the aggregate expressions, along with a flag indicating if it contains an aggregate
   * expression. The generator is rewritten to reference these aliases.
   *
   * For example:
   * {{{
   *  SELECT stack(2, id, sum(val))
   *  FROM VALUES (1L, 10), (1L, 200) AS t(id, val)
   *  GROUP BY ALL
   *
   *  == Analyzed Logical Plan ==
   *  col0: bigint
   *  Project [col0#4L]
   *  +- Generate stack(2, _gen_input_1#2L, _gen_input_2#3L), false, [col0#4L]
   *     +- Aggregate [id#0L], [id#0L AS _gen_input_1#2L, sum(val#1) AS _gen_input_2#3L]
   *        +- SubqueryAlias t
   *           +- LocalRelation [id#0L, val#1]
   * }}}
   *
   * Let's break down each child of `stack(2, id, sum(val))`:
   *  - 2 is foldable, so it remains as is.
   *  - id is non-foldable, so we create an alias `_gen_input_1` for it.
   *  - sum(val) is non-foldable, so we create an alias `_gen_input_2`.
   *
   * @param aliasedGenerator The aliased generator to extract from
   * @return (rewrittenAliasedGenerator, extractedAliases) - the rewritten generator
   *         wrapped in AliasedGenerator and aliases for non-foldable children.
   */
  def extract(aliasedGenerator: AliasedGenerator): (AliasedGenerator, Seq[Alias]) = {
    val generator = aliasedGenerator.child

    val extractedAliases = new ArrayBuffer[Alias]
    val newGeneratorChildren: Seq[Expression] =
      generator.children.zipWithIndex.map { case (child, index) =>
        if (child.foldable) {
          child
        } else {
          val alias = Alias(child, s"_gen_input_$index")()
          extractedAliases += alias
          alias.toAttribute
        }
      }

    val rewrittenGenerator = generator
      .withNewChildren(newGeneratorChildren)
      .asInstanceOf[Generator]

    val rewrittenAliasedGenerator = aliasedGenerator.copy(child = rewrittenGenerator)

    (rewrittenAliasedGenerator, extractedAliases.toSeq)
  }
}
