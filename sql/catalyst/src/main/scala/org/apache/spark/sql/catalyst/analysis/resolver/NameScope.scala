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

import java.util.{ArrayDeque, HashSet}

import scala.collection.mutable

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{Resolver => NameComparator, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSeq, ExprId, NamedExpression}
import org.apache.spark.sql.internal.SQLConf

/**
 * The [[NameScope]] is used to control the resolution of names (table, column, alias identifiers).
 * It's a part of the [[Resolver]]'s state, and is used to manage the output of SQL query/DataFrame
 * program operators.
 *
 * The [[NameScope]] output is immutable. If it's necessary to update the output,
 * [[NameScopeStack]] methods are used ([[overwriteTop]] or [[withNewScope]]). The [[NameScope]]
 * is always used through the [[NameScopeStack]].
 *
 * The resolution of identifiers is case-insensitive.
 *
 * Name resolution priority is as follows:
 *
 *  1. Resolution of local references:
 *    - column reference
 *    - struct field or map key reference
 *  2. Resolution of lateral column aliases (if enabled).
 *
 *  For example, in a query like:
 *
 *  {{{ SELECT 1 AS col1, col1 FROM VALUES (2) }}}
 *
 * Because column resolution has a higher priority than LCA resolution, the result will be [1, 2]
 * and not [1, 1].
 *
 * Approximate tree of [[NameScope]] manipulations is shown in the following example:
 *
 * {{{
 * CREATE TABLE IF NOT EXISTS t1 (col1 INT, col2 INT, col3 STRING);
 *
 * SELECT
 *   col1, col2 as alias1
 * FROM
 *   (SELECT * FROM VALUES (1, 2))
 *   UNION
 *   (SELECT t2.col1, t2.col2 FROM (SELECT col1, col2 FROM t1) AS t2)
 * ;
 * }}}
 *
 * ->
 *
 * {{{
 * unionAttributes = withNewScope {
 *   lhsOutput = withNewScope {
 *     expandedStar = withNewScope {
 *       scope.overwriteTop(localRelation.output)
 *       scope.expandStar(star)
 *     }
 *     scope.overwriteTop(expandedStar)
 *     scope.output
 *   }
 *   rhsOutput = withNewScope {
 *     subqueryAttributes = withNewScope {
 *       scope.overwriteTop(t1.output)
 *       scope.overwriteTop(prependQualifier(scope.output, "t2"))
 *       [scope.matchMultiPartName("t2", "col1"), scope.matchMultiPartName("t2", "col2")]
 *     }
 *     scope.overwriteTop(subqueryAttributes)
 *     scope.output
 *   }
 *   scope.overwriteTop(coerce(lhsOutput, rhsOutput))
 *   [scope.matchMultiPartName("col1"), alias(scope.matchMultiPartName("col2"), "alias1")]
 * }
 * scope.overwriteTop(unionAttributes)
 * }}}
 *
 * @param output These are the attributes visible for lookups in the current scope.
 *   These may be:
 *   - Transformed outputs of lower scopes (e.g. type-coerced outputs of [[Union]]'s children).
 *   - Output of a current operator that is being resolved (leaf nodes like [[Relations]]).
 */
class NameScope(val output: Seq[Attribute] = Seq.empty) extends SQLConfHelper {

  /**
   * [[nameComparator]] is a function that is used to compare two identifiers. Its implementation
   * depends on the "spark.sql.caseSensitive" configuration - whether to respect case sensitivity
   * or not.
   */
  private val nameComparator: NameComparator = conf.resolver

  /**
   * [[attributesForResolution]] is an [[AttributeSeq]] that is used for resolution of
   * multipart attribute names. It's created from the `attributes` when [[NameScope]] is updated.
   */
  private val attributesForResolution: AttributeSeq = AttributeSeq.fromNormalOutput(output)

  /**
   * [[attributesByName]] is used to look up attributes by one-part name from the operator's output.
   * This is a lazy val, since in most of the cases [[ExpressionResolver]] doesn't need it and
   * accesses a generic [[attributesForResolution]] in [[resolveMultipartName]].
   */
  private lazy val attributesByName = createAttributesByName(output)

  /**
   * Expression IDs from `output`. See [[hasAttributeWithId]] for more details.
   */
  private lazy val attributeIds = createAttributeIds(output)

  private val isLcaEnabled = conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)
  lazy val lcaRegistry: LateralColumnAliasRegistry = if (isLcaEnabled) {
    new LateralColumnAliasRegistryImpl(output)
  } else {
    new LateralColumnAliasProhibitedRegistry
  }

  /**
   * Expand the [[UnresolvedStar]]. The expected use case for this method is star expansion inside
   * [[Project]].
   *
   * Star without a target:
   *
   * {{{
   * -- Here the star will be expanded to [a, b, c].
   * SELECT * FROM VALUES (1, 2, 3) AS t(a, b, c);
   * }}}
   *
   * Star with a multipart name target:
   *
   * {{{
   * USE CATALOG catalog1;
   * USE DATABASE database1;
   *
   * CREATE TABLE IF NOT EXISTS table1 (col1 INT, col2 INT);
   *
   * -- Here the star will be expanded to [col1, col2].
   * SELECT catalog1.database1.table1.* FROM catalog1.database1.table1;
   * }}}
   *
   * Star with a struct target:
   *
   * {{{
   * -- Here the star will be expanded to [field1, field2].
   * SELECT d.* FROM VALUES (named_struct('field1', 1, 'field2', 2)) AS t(d);
   * }}}
   *
   * Star as an argument to a function:
   *
   * {{{
   * -- Here the star will be expanded to [col1, col2, col3] and those would be passed as
   * -- arguments to `concat_ws`.
   * SELECT concat_ws('', *) AS result FROM VALUES (1, 2, 3);
   * }}}
   *
   * Also, see [[UnresolvedStarBase.expandStar]] for more details.
   */
  def expandStar(unresolvedStar: UnresolvedStar): Seq[NamedExpression] = {
    unresolvedStar.expandStar(
      childOperatorOutput = output,
      childOperatorMetadataOutput = Seq.empty,
      resolve =
        (nameParts, nameComparator) => attributesForResolution.resolve(nameParts, nameComparator),
      suggestedAttributes = output,
      resolver = nameComparator,
      cleanupNestedAliasesDuringStructExpansion = true
    )
  }

  /**
   * Resolve multipart name into a [[NameTarget]]. [[NameTarget]]'s `candidates` may contain
   * simple [[AttributeReference]]s if it's a column or alias, or [[ExtractValue]] expressions if
   * it's a struct field, map value or array value. The `aliasName` will optionally be set to the
   * proposed alias name for the value extracted from a struct, map or array.
   *
   * Example that demonstrates those major use-cases:
   *
   * {{{
   * CREATE TABLE IF NOT EXISTS t (
   *   col1 INT,
   *   col2 STRUCT<field: INT>,
   *   col3 STRUCT<struct: STRUCT<field: INT>>,
   *   col4 MAP<STRING: INT>,
   *   col5 STRING
   * );
   *
   * -- For the SELECT below the top Project list will be resolved using this method like this:
   * -- AttributeReference(col1),
   * -- AttributeReference(a),
   * -- GetStructField(col2, field),
   * -- GetStructField(GetStructField(col3, struct), field),
   * -- GetMapValue(col4, key)
   * SELECT
   *   col1, a, col2.field, col3.struct.field, col4.key
   * FROM
   *   (SELECT *, col5 AS a FROM t);
   * }}}
   *
   * Since there can be several expressions that matched the same multipart name, this method may
   * return a [[NameTarget]] with the following `candidates`:
   * - 0 values: No matched expressions
   * - 1 value: Unique expression matched
   * - 1+ values: Ambiguity, several expressions matched
   *
   * Some examples of ambiguity:
   *
   * {{{
   * CREATE TABLE IF NOT EXISTS t1 (c1 INT, c2 INT);
   * CREATE TABLE IF NOT EXISTS t2 (c2 INT, c3 INT);
   *
   * -- Identically named columns from different tables.
   * -- This will fail with AMBIGUOUS_REFERENCE error.
   * SELECT c2 FROM t1, t2;
   * }}}
   *
   * {{{
   * CREATE TABLE IF NOT EXISTS foo (c1 INT);
   * CREATE TABLE IF NOT EXISTS bar (foo STRUCT<c1: INT>);
   *
   * -- Ambiguity between a column in a table and a field in a struct.
   * -- This will succeed, and column will win over the struct field.
   * SELECT foo.c1 FROM foo, bar;
   * }}}
   *
   * The candidates are deduplicated by expression ID (not by attribute name!):
   *
   * {{{
   * CREATE TABLE IF NOT EXISTS t1 (col1 STRING);
   *
   * -- No ambiguity here, since we are selecting the same column (same expression ID).
   * SELECT col1 FROM (SELECT col1, col1 FROM t);
   * }}}
   *
   * The case of the `multipartName` takes precedence over the original name case, so the candidates
   * will have names that are case-identical to the `multipartName`:
   *
   * {{{
   * CREATE TABLE IF NOT EXISTS t1 (col1 STRING);
   *
   * -- The output schema of this query is [COL1], despite the fact that the column is in
   * -- lower-case.
   * SELECT COL1 FROM t;
   * }}}
   *
   * We are relying on the [[AttributeSeq]] to perform that work, since it requires complex
   * resolution logic involving nested field extraction and multipart name matching.
   *
   * Also, see [[AttributeSeq.resolve]] for more details.
   */
  def resolveMultipartName(
      multipartName: Seq[String],
      canLaterallyReferenceColumn: Boolean = true): NameTarget = {
    val (candidates, nestedFields) =
      attributesForResolution.getCandidatesForResolution(multipartName, nameComparator)

    val (candidatesWithLCAs: Seq[Attribute], referencedAttribute: Option[Attribute]) =
      if (candidates.isEmpty && canLaterallyReferenceColumn) {
        getLcaCandidates(multipartName)
      } else {
        (candidates, None)
      }

    val resolvedCandidates = attributesForResolution.resolveCandidates(
      multipartName,
      nameComparator,
      candidatesWithLCAs,
      nestedFields
    )

    resolvedCandidates match {
      case Seq(Alias(child, aliasName)) =>
        NameTarget(
          candidates = Seq(child),
          aliasName = Some(aliasName),
          lateralAttributeReference = referencedAttribute,
          output = output
        )
      case other =>
        NameTarget(
          candidates = other,
          lateralAttributeReference = referencedAttribute,
          output = output
        )
    }
  }

  /**
   * Find attributes in this [[NameScope]] that match a provided one-part `name`.
   *
   * This method is simpler and more lightweight than [[resolveMultipartName]], because here we
   * just return all the attributes matched by the one-part `name`. This is only suitable
   * for situations where name _resolution_ is not required (e.g. accessing struct fields
   * from the lower operator's output).
   *
   * For example, this method is used to look up attributes to match a specific [[View]] schema.
   * See [[ExpressionResolver.resolveGetViewColumnByNameAndOrdinal]] for more info on view column
   * lookup.
   *
   * We are relying on a simple [[IdentifierMap]] to perform that work, since we just need to match
   * one-part name from the lower operator's output here.
   */
  def findAttributesByName(name: String): Seq[Attribute] = {
    attributesByName.get(name) match {
      case Some(attributes) => attributes.toSeq
      case None => Seq.empty
    }
  }

  /**
   * Check if `output` contains attributes with `expressionId`. This is used to disable missing
   * attribute propagation for DataFrames, because we don't support it yet.
   */
  def hasAttributeWithId(expressionId: ExprId): Boolean = {
    attributeIds.contains(expressionId)
  }

  /**
   * If a candidate was not found from output attributes, returns the candidate from lateral
   * columns. Here we do [[AttributeSeq.fromNormalOutput]] because a struct field can also be
   * laterally referenced and we need to properly resolve [[GetStructField]] node.
   */
  private def getLcaCandidates(multipartName: Seq[String]): (Seq[Attribute], Option[Attribute]) = {
    val referencedAttribute = lcaRegistry.getAttribute(multipartName.head)
    if (referencedAttribute.isDefined) {
      val attributesForResolution = AttributeSeq.fromNormalOutput(Seq(referencedAttribute.get))
      val (newCandidates, _) =
        attributesForResolution.getCandidatesForResolution(multipartName, nameComparator)
      (newCandidates, Some(referencedAttribute.get))
    } else {
      (Seq.empty, None)
    }
  }

  private def createAttributesByName(
      attributes: Seq[Attribute]): IdentifierMap[mutable.ArrayBuffer[Attribute]] = {
    val result = new IdentifierMap[mutable.ArrayBuffer[Attribute]]
    for (attribute <- attributes) {
      result.get(attribute.name) match {
        case Some(attributesForThisName) =>
          attributesForThisName += attribute
        case None =>
          val attributesForThisName = new mutable.ArrayBuffer[Attribute]
          attributesForThisName += attribute

          result += (attribute.name, attributesForThisName)
      }
    }

    result
  }

  private def createAttributeIds(attributes: Seq[Attribute]): HashSet[ExprId] = {
    val result = new HashSet[ExprId]
    for (attribute <- attributes) {
      result.add(attribute.exprId)
    }

    result
  }
}

/**
 * The [[NameScopeStack]] is a stack of [[NameScope]]s managed by the [[Resolver]]. Usually a top
 * scope is used for name resolution, but in case of correlated subqueries we can lookup names in
 * the parent scopes. Low-level scope creation is managed internally, and only high-level api like
 * [[withNewScope]] is available to the resolvers. Freshly-created [[NameScopeStack]] contains an
 * empty root [[NameScope]], which in the context of [[Resolver]] corresponds to the query output.
 */
class NameScopeStack extends SQLConfHelper {
  private val stack = new ArrayDeque[NameScope]
  stack.push(new NameScope)

  /**
   * Get the top scope, which is a default choice for name resolution.
   */
  def top: NameScope = {
    stack.peek()
  }

  /**
   * Completely overwrite the top scope state with operator `output`.
   *
   * This method is called by the [[Resolver]] when we've calculated the output of an operator that
   * is being resolved. The new output is calculated based on the outputs of operator's children.
   *
   * Example for [[SubqueryAlias]], here we rewrite the top [[NameScope]]'s attributes to prepend
   * subquery qualifier to their names:
   *
   * {{{
   * val qualifier = sa.identifier.qualifier :+ sa.alias
   * scope.overwriteTop(scope.output.map(attribute => attribute.withQualifier(qualifier)))
   * }}}
   *
   * Trivially, we would call this method for every operator in the query plan,
   * however some operators just propagate the output of their children without any changes, so
   * we can omit this call for them (e.g. [[Filter]]).
   *
   * This method should be preferred over [[withNewScope]].
   */
  def overwriteTop(output: Seq[Attribute]): Unit = {
    val newScope = new NameScope(output)

    stack.pop()
    stack.push(newScope)
  }

  /**
   * Execute `body` in a context of a fresh scope.
   *
   * This method is called by the [[Resolver]] before recursing into the operator's child
   * resolution _only_ in cases where a fresh scope is required.
   *
   * For example, [[Project]] or [[Aggregate]] introduce their own scopes semantically, so that a
   * lower resolution can lookup correlated names:
   *
   * {{{
   * CREATE TABLE IF NOT EXISTS t1 (col1 INT, col2 STRING);
   * CREATE TABLE IF NOT EXISTS t2 (col1 INT, col2 STRING);
   *
   * -- Here we need a scope for the upper [[Project]], and a separate scope for the correlated
   * -- subquery, because its [[Filter]] need to lookup `t1.col1` from the upper scope.
   * -- Those scopes have to be independent to avoid polluting each other's attributes.
   * SELECT col1, (SELECT col2 FROM t2 WHERE t2.col1 == t1.col1 LIMIT 1) FROM t1;
   * }}}
   *
   * Also, we need separate scopes for the operators with multiple children, so that the next
   * child's resolution wouldn't try to work with the data from it's sibling's scope, to avoid
   * all kinds of undefined behavior:
   *
   * {{{
   * val resolvedLeftChild = withNewScope {
   *    resolve(unresolvedExcept.left)
   * }
   *
   * // Right child should not see the left child's resolution data to avoid subtle bugs, so we
   * // create a fresh scope here.
   *
   * val resolvedRightChild = withNewScope {
   *    resolve(unresolvedExcept.right)
   * }
   * }}}
   */
  def withNewScope[R](body: => R): R = {
    stack.push(new NameScope)
    try {
      body
    } finally {
      stack.pop()
    }
  }
}
