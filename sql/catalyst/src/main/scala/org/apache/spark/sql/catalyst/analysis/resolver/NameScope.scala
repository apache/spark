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

import java.util.{ArrayDeque, ArrayList, HashSet}

import scala.collection.mutable

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{Resolver => NameComparator, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeSeq,
  Expression,
  NamedExpression
}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * The [[NameScope]] is used during the analysis to control the visibility of names: plan names
 * and output attributes. New [[NameScope]] can be created both in the [[Resolver]] and in
 * the [[ExpressionResolver]] using the [[NameScopeStack]] api. The name resolution for identifiers
 * is case-insensitive.
 *
 * In this example:
 *
 * {{{
 * WITH table_1_cte AS (
 *   SELECT
 *     col1,
 *     col2,
 *     col2
 *   FROM
 *     table_1
 * )
 * SELECT
 *   table_1_cte.col1,
 *   table_2.col1
 * FROM
 *   table_1_cte
 * INNER JOIN
 *   table_2
 * ON
 *   table_1_cte.col2 = table_2.col3
 * ;
 * }}}
 *
 * there are two named subplans in the scope: table_1_cte -> [col1, col2, col2] and
 * table_2 -> [col1, col3].
 *
 * State breakout:
 * - `planOutputs`: list of named plan outputs. Order matters here (e.g. to correctly expand `*`).
 *   Can contain duplicate names, since it's possible to select same column twice, or to select
 *   columns with the same name from different relations. [[OptionalIdentifierMap]] is used here,
 *   since some plans don't have an explicit name, so output attributes from those plans will reside
 *   under the `None` key.
 *   In our example it will be {{{ [(table_1_cte, [col1, col2, col2]), (table_2, [col1, col3])] }}}
 *
 * - `planNameToOffset`: mapping from plan output names to their offsets in the `planOutputs` array.
 *   It's used to lookup attributes by plan output names (multipart names are not supported yet).
 *   In our example it will be {{{ [table_1_cte -> 0, table_2 -> 1] }}}
 */
class NameScope extends SQLConfHelper {
  private val planOutputs = new ArrayList[PlanOutput]()
  private val planNameToOffset = new OptionalIdentifierMap[Int]
  private val nameComparator: NameComparator = conf.resolver
  private val existingAliases = new HashSet[String]

  /**
   * Register the named plan output in this [[NameScope]]. The named plan is usually a
   * [[NamedRelation]]. `attributes` sequence can contain duplicate names both for this named plan
   * and for the scope in general, despite the fact that their further resolution _may_ throw an
   * error in case of ambiguous reference. After calling this method, the code can lookup the
   * attributes using `get*` methods of this [[NameScope]].
   *
   * Duplicate plan names are merged into the same [[PlanOutput]]. For example, this query:
   *
   * {{{ SELECT t.* FROM (SELECT * FROM VALUES (1)) as t, (SELECT * FROM VALUES (2)) as t; }}}
   *
   * will have the following output schema:
   *
   * {{{ [col1, col1] }}}
   *
   * Same logic applies for the unnamed plan outputs. This query:
   *
   * {{{ SELECT * FROM (SELECT * FROM VALUES (1)), (SELECT * FROM VALUES (2)); }}}
   *
   * will have the same output schema:
   *
   * {{{ [col1, col1] }}}
   *
   * @param name The name of this named plan.
   * @param attributes The output of this named plan. Can contain duplicate names.
   */
  def update(name: String, attributes: Seq[Attribute]): Unit = {
    update(attributes, Some(name))
  }

  /**
   * Register the unnamed plan output in this [[NameScope]]. Some examples of the unnamed plan are
   * [[Project]] and [[Aggregate]].
   *
   * See the [[update]] method for more details.
   *
   * @param attributes The output of the unnamed plan. Can contain duplicate names.
   */
  def +=(attributes: Seq[Attribute]): Unit = {
    update(attributes)
  }

  /**
   * Get all the attributes from all the plans registered in this [[NameScope]]. The output can
   * contain duplicate names. This is used for star (`*`) resolution.
   */
  def getAllAttributes: Seq[Attribute] = {
    val attributes = new mutable.ArrayBuffer[Attribute]

    planOutputs.forEach(planOutput => {
      attributes.appendAll(planOutput.attributes)
    })

    attributes.toSeq
  }

  /**
   * Expand the [[UnresolvedStar]] using `planOutputs`. The expected use case for this method is
   * star expansion inside [[Project]]. Since [[Project]] has only one child, we assert that the
   * size of `planOutputs` is 1, otherwise the query is malformed.
   *
   * Some examples of queries with a star:
   *
   *  - Star without a target:
   *  {{{ SELECT * FROM VALUES (1,  2,  3) AS t(a, b, c); }}}
   *  - Star with a multipart name target:
   *  {{{ SELECT catalog1.database1.table1.* FROM catalog1.database1.table1; }}}
   *  - Star with a struct target:
   *  {{{ SELECT d.* FROM VALUES (named_struct('a', 1, 'b', 2)) AS t(d); }}}
   *  - Star as an argument to a function:
   *  {{{ SELECT concat_ws('', *) AS result FROM VALUES (1, 2, 3) AS t(a, b, c); }}}
   *
   * It is resolved by correctly resolving the star qualifier.
   * Please check [[UnresolvedStarBase.expandStar]] for more details.
   *
   * @param unresolvedStar [[UnresolvedStar]] to expand.
   * @return The output of a plan expanded from the star.
   */
  def expandStar(unresolvedStar: UnresolvedStar): Seq[NamedExpression] = {
    if (planOutputs.size != 1) {
      throw QueryCompilationErrors.invalidStarUsageError("query", Seq(unresolvedStar))
    }

    planOutputs.get(0).expandStar(unresolvedStar)
  }

  /**
   * Get all matched attributes by a multipart name. It returns [[Attribute]]s when we resolve a
   * simple column or an alias name from a lower operator. However this function can also return
   * [[Alias]]es in case we access a struct field or a map value using some key.
   *
   * Example that contains those major use-cases:
   *
   * {{{
   *  SELECT col1, a, col2.field, col3.struct.field, col4.key
   *  FROM (SELECT *, col5 AS a FROM t);
   * }}}
   *
   * has a Project list that looks like this:
   *
   * {{{
   *   AttributeReference(col1),
   *   AttributeReference(a),
   *   Alias(col2.field, field),
   *   Alias(col3.struct.field, field),
   *   Alias(col4[CAST(key AS INT)], key)
   * }}}
   *
   * Also, see [[AttributeSeq.resolve]] for more details.
   *
   * Since there can be several identical attribute names for several named plans, this function
   * can return multiple values:
   * - 0 values: No matched attributes
   * - 1 value: Unique attribute matched
   * - 1+ values: Ambiguity, several attributes matched
   *
   * One example of a query with an attribute that has a multipart name:
   *
   * {{{ SELECT catalog1.database1.table1.col1 FROM catalog1.database1.table1; }}}
   *
   * @param multipartName Multipart attribute name. Can be of several forms:
   *   - `catalog.database.table.column`
   *   - `database.table.column`
   *   - `table.column`
   *   - `column`
   * @return All the attributes matched by the `multipartName`, encapsulated in a [[NameTarget]].
   */
  def matchMultipartName(multipartName: Seq[String]): NameTarget = {
    val candidates = new mutable.ArrayBuffer[Expression]
    val allAttributes = new mutable.ArrayBuffer[Attribute]
    var aliasName: Option[String] = None

    planOutputs.forEach(planOutput => {
      allAttributes.appendAll(planOutput.attributes)
      val nameTarget = planOutput.matchMultipartName(multipartName)
      if (nameTarget.aliasName.isDefined) {
        aliasName = nameTarget.aliasName
      }
      candidates.appendAll(nameTarget.candidates)
    })

    NameTarget(candidates.toSeq, aliasName, allAttributes.toSeq)
  }

  /**
   * Add an alias, by name, to the list of existing aliases.
   */
  def addAlias(aliasName: String): Unit = existingAliases.add(aliasName.toLowerCase())

  /**
   * Returns whether an alias exists in the current scope.
   */
  def isExistingAlias(aliasName: String): Boolean =
    existingAliases.contains(aliasName.toLowerCase())

  private def update(attributes: Seq[Attribute], name: Option[String] = None): Unit = {
    planNameToOffset.get(name) match {
      case Some(index) =>
        val prevPlanOutput = planOutputs.get(index)
        planOutputs.set(
          index,
          new PlanOutput(prevPlanOutput.attributes ++ attributes, name, nameComparator)
        )
      case None =>
        val index = planOutputs.size
        planOutputs.add(new PlanOutput(attributes, name, nameComparator))
        planNameToOffset += (name -> index)
    }
  }
}

/**
 * The [[NameScopeStack]] is a stack of [[NameScope]]s managed by the [[Resolver]] and the
 * [[ExpressionResolver]]. Usually a top scope is used for name resolution, but in case of
 * correlated subqueries we can lookup names in the parent scopes. Low-level scope creation is
 * managed internally, and only high-level api like [[withNewScope]] is available to the resolvers.
 * Freshly-created [[NameScopeStack]] contains an empty root [[NameScope]].
 */
class NameScopeStack extends SQLConfHelper {
  private val stack = new ArrayDeque[NameScope]
  push()

  /**
   * Get the top scope, which is a default choice for name resolution.
   */
  def top: NameScope = {
    stack.peek()
  }

  /**
   * Completely overwrite the top scope state with a named plan output.
   *
   * See [[NameScope.update]] for more details.
   */
  def overwriteTop(name: String, attributes: Seq[Attribute]): Unit = {
    val newScope = new NameScope
    newScope.update(name, attributes)

    stack.pop()
    stack.push(newScope)
  }

  /**
   * Completely overwrite the top scope state with an unnamed plan output.
   *
   * See [[NameScope.+=]] for more details.
   */
  def overwriteTop(attributes: Seq[Attribute]): Unit = {
    val newScope = new NameScope
    newScope += attributes

    stack.pop()
    stack.push(newScope)
  }

  /**
   * Execute `body` in a context of a fresh scope. It's used during the [[Project]] or the
   * [[Aggregate]] resolution to avoid calling [[push]] and [[pop]] explicitly.
   */
  def withNewScope[R](body: => R): R = {
    push()
    try {
      body
    } finally {
      pop()
    }
  }

  /**
   * Push a new scope to the stack. Introduced by the [[Project]] or the [[Aggregate]].
   */
  private def push(): Unit = {
    stack.push(new NameScope)
  }

  /**
   * Pop a scope from the stack. Called when the resolution process for the pushed scope is done.
   */
  private def pop(): Unit = {
    stack.pop()
  }
}

/**
 * [[PlanOutput]] represents a sequence of attributes from a plan ([[NamedRelation]], [[Project]],
 * [[Aggregate]], etc).
 *
 * It is created from `attributes`, which is an output of a named plan, optional plan `name` and a
 * resolver provided by the [[NameScopeStack]].
 *
 * @param attributes Plan output. Can contain duplicate names.
 * @param name Plan name. Non-empty for named plans like [[NamedRelation]] or [[SubqueryAlias]],
 *   `None` otherwise.
 */
class PlanOutput(
    val attributes: Seq[Attribute],
    val name: Option[String],
    val nameComparator: NameComparator) {

  /**
   * attributesForResolution is an [[AttributeSeq]] that is used for resolution of
   * multipart attribute names. It's created from the `attributes` when [[NameScope]] is updated.
   */
  private val attributesForResolution: AttributeSeq =
    AttributeSeq.fromNormalOutput(attributes)

  /**
   * Find attributes by the multipart name.
   *
   * See [[NameScope.matchMultipartName]] for more details.
   *
   * @param multipartName Multipart attribute name.
   * @return Matched attributes or [[Seq.empty]] otherwise.
   */
  def matchMultipartName(multipartName: Seq[String]): NameTarget = {
    val (candidates, nestedFields) =
      attributesForResolution.getCandidatesForResolution(multipartName, nameComparator)
    val resolvedCandidates = attributesForResolution.resolveCandidates(
      multipartName,
      nameComparator,
      candidates,
      nestedFields
    )
    resolvedCandidates match {
      case Seq(Alias(child, aliasName)) =>
        NameTarget(Seq(child), Some(aliasName))
      case other =>
        NameTarget(other, None)
    }
  }

  /**
   * Method to expand an unresolved star. See [[NameScope.expandStar]] for more details.
   *
   * @param unresolvedStar Star to resolve.
   * @return Attributes expanded from the star.
   */
  def expandStar(unresolvedStar: UnresolvedStar): Seq[NamedExpression] = {
    unresolvedStar.expandStar(
      childOperatorOutput = attributes,
      childOperatorMetadataOutput = Seq.empty,
      resolve =
        (nameParts, nameComparator) => attributesForResolution.resolve(nameParts, nameComparator),
      suggestedAttributes = attributes,
      resolver = nameComparator,
      cleanupNestedAliasesDuringStructExpansion = true
    )
  }
}
