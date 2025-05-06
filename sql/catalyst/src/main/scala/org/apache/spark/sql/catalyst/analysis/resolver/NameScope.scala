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

import java.util.{ArrayDeque, HashMap, HashSet, LinkedHashMap}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{
  LiteralFunctionResolution,
  Resolver => NameComparator,
  UnresolvedStar
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeSeq,
  Expression,
  ExprId,
  ExtractValue,
  NamedExpression,
  OuterReference
}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Metadata

/**
 * The [[NameScope]] is used to control the resolution of names (table, column, alias identifiers).
 * It's a part of the [[Resolver]]'s state, and is used to manage the output of SQL query/DataFrame
 * program operators.
 *
 * The [[NameScope]] output is immutable. If it's necessary to update the output,
 * [[NameScopeStack]] methods are used ([[overwriteCurrent]] or [[withNewScope]]). The
 * [[NameScope]] is always used through the [[NameScopeStack]].
 *
 * The resolution of identifiers is case-insensitive.
 *
 * Name resolution priority is as follows:
 *
 *  1. Resolution of local references:
 *    - column reference
 *    - parameterless function reference
 *    - struct field or map key reference
 *  2. Resolution of lateral column aliases (if enabled).
 *  3. In the context of [[Aggregate]]: resolution of names in groping expressions list referencing
 *     aliases in aggregate expressions.
 *
 *  Following examples showcase the priority of name resolution:
 *
 *  {{{ SELECT 1 AS col1, col1 FROM VALUES (2) }}}
 *
 * Because column resolution has a higher priority than LCA resolution, the result will be [1, 2]
 * and not [1, 1].
 *
 * {{{
 * CREATE TABLE t AS SELECT col1 as current_date FROM VALUES (2);
 *
 * SELECT
 *  1 AS current_timestamp,
 *  current_timestamp,
 *  current_date
 * FROM
 *  foo;
 * }}}
 *
 * Result of the previous SELECT will be: [1, 2025-02-13T07:55:26.206+00:00, 2]. As can be seen,
 * because of resolution precedence, current_date is resolved as a table column, but
 * current_timestamp is resolved as a function without parenthesis instead of a lateral column
 * reference.
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
 *       scopes.overwriteCurrent(localRelation.output)
 *       scope.expandStar(star)
 *     }
 *     scopes.overwriteCurrent(expandedStar)
 *     scope.output
 *   }
 *   rhsOutput = withNewScope {
 *     subqueryAttributes = withNewScope {
 *       scopes.overwriteCurrent(t1.output)
 *       scopes.overwriteCurrent(prependQualifier(scope.output, "t2"))
 *       [scope.matchMultiPartName("t2", "col1"), scope.matchMultiPartName("t2", "col2")]
 *     }
 *     scopes.overwriteCurrent(subqueryAttributes)
 *     scope.output
 *   }
 *   scopes.overwriteCurrent(coerce(lhsOutput, rhsOutput))
 *   [scope.matchMultiPartName("col1"), alias(scope.matchMultiPartName("col2"), "alias1")]
 * }
 * scopes.overwriteCurrent(unionAttributes)
 * }}}
 *
 * @param output These are the attributes visible for lookups in the current scope.
 *   These may be:
 *   - Transformed outputs of lower scopes (e.g. type-coerced outputs of [[Union]]'s children).
 *   - Output of a current operator that is being resolved (leaf nodes like [[Relations]]).
 * @param hiddenOutput Attributes that are not directly visible in the scope, but available for
 *   lookup in case the resolved attribute is not found in `output`.
 * @param isSubqueryRoot Indicates that the current scope is a root of a subquery. This is used by
 *   [[NameScopeStack.resolveMultipartName]] to detect the nearest outer scope.
 */
class NameScope(
    val output: Seq[Attribute] = Seq.empty,
    val hiddenOutput: Seq[Attribute] = Seq.empty,
    val isSubqueryRoot: Boolean = false)
    extends SQLConfHelper {

  /**
   * This is an internal class used to store resolved multipart name, with correct precedence as
   * specified by [[NameScope]] class doc.
   */
  private case class ResolvedMultipartName(
      candidates: Seq[Expression],
      referencedAttribute: Option[Attribute],
      aliasMetadata: Option[Metadata] = None)

  /**
   * [[nameComparator]] is a function that is used to compare two identifiers. Its implementation
   * depends on the "spark.sql.caseSensitive" configuration - whether to respect case sensitivity
   * or not.
   */
  private val nameComparator: NameComparator = conf.resolver

  /**
   * [[attributesForResolution]] is an [[AttributeSeq]] that is used for resolution of multipart
   * attribute names, by output. It's created from the `output` when
   * [[NameScope]] is updated.
   */
  private val attributesForResolution: AttributeSeq =
    AttributeSeq.fromNormalOutput(output)

  /**
   * [[hiddenAttributesForResolution]] is an [[AttributeSeq]] that is used for resolution of
   * multipart attribute names, by hidden output. It's created from the `hiddenOutput` when
   * [[NameScope]] is updated. [[AGGREGATED_ACCESS_ONLY]] attributes are excluded from
   * resolution by default, since they can only be referenced in specific cases (see
   * [[resolveMultipartName]] for more details).
   */
  private lazy val hiddenAttributesForResolution: AttributeSeq =
    AttributeSeq.fromNormalOutput(hiddenOutput.filter(!_.aggregatedAccessOnly))

  /**
   * [[hiddenAttributesForResolutionWithAggregatedOnlyAccess]] is an [[AttributeSeq]] that is used
   * for resolution of multipart attribute names, by hidden output including attributes with
   * [[AGGREGATED_ACCESS_ONLY]]. These attributes can only be accessed if we are resolving a tree
   * under [[AggregateExpression]] (see [[resolveMultipartName]] for more details).
   */
  private lazy val hiddenAttributesForResolutionWithAggregatedOnlyAccess: AttributeSeq =
    AttributeSeq.fromNormalOutput(hiddenOutput)

  /**
   * [[metadataAttributesForResolution]] is an [[AttributeSeq]] that is used for resolution of
   * multipart attribute names, by qualified access only columns from hidden output. It's created
   * from the `hiddenOutput` when [[NameScope]] is updated.
   */
  private lazy val metadataAttributesForResolution: AttributeSeq =
    AttributeSeq.fromNormalOutput(hiddenOutput.filter(_.qualifiedAccessOnly))

  /**
   * [[attributesByName]] is used to look up attributes by one-part name from the operator's output.
   * This is a lazy val, since in most of the cases [[ExpressionResolver]] doesn't need it and
   * accesses a generic [[attributesForResolution]] in [[resolveMultipartName]].
   */
  private lazy val attributesByName = createAttributesByName(output)

  /**
   * Returns a map of [[ExprId]] to [[Attribute]] from `output`. See [[getAttributeById]] for
   * more details.
   */
  private lazy val attributesById: HashMap[ExprId, Attribute] = createAttributeIds(output)

  /**
   * Returns a map of [[ExprId]] to [[Attribute]] from `hiddenOutput`.
   */
  private lazy val hiddenAttributesById: HashMap[ExprId, Attribute] =
    createAttributeIds(hiddenOutput)

  lazy val lcaRegistry: LateralColumnAliasRegistry =
    if (conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)) {
      new LateralColumnAliasRegistryImpl(output)
    } else {
      new LateralColumnAliasProhibitedRegistry
    }

  /**
   * All aliased aggregate expressions from an [[Aggregate]] that is currently being resolved.
   * Used in [[resolveMultipartName]] to resolve names in grouping expressions list referencing
   * aggregate expressions.
   */
  private lazy val topAggregateExpressionsByAliasName: IdentifierMap[Alias] =
    new IdentifierMap[Alias]

  /**
   * Returns new [[NameScope]] which preserves all the immutable [[NameScope]] properties but
   * overwrites `output` and `hiddenOutput` if provided. Mutable state like `lcaRegistry` is not
   * preserved.
   */
  def overwriteOutput(
      output: Option[Seq[Attribute]] = None,
      hiddenOutput: Option[Seq[Attribute]] = None): NameScope = {
    new NameScope(
      output = output.getOrElse(this.output),
      hiddenOutput = hiddenOutput.getOrElse(this.hiddenOutput),
      isSubqueryRoot = isSubqueryRoot
    )
  }

  /**
   * Given referenced attributes, returns all attributes that are referenced and missing from
   * current output, but can be found in hidden output.
   */
  def resolveMissingAttributesByHiddenOutput(
      referencedAttributes: HashMap[ExprId, Attribute]): Seq[Attribute] = {
    val distinctMissingAttributes = new LinkedHashMap[ExprId, Attribute]
    hiddenOutput.foreach(
      attribute =>
        if (referencedAttributes.containsKey(attribute.exprId) &&
          !attributesById.containsKey(attribute.exprId) &&
          !distinctMissingAttributes.containsKey(attribute.exprId)) {
          distinctMissingAttributes.put(attribute.exprId, attribute)
        }
    )
    distinctMissingAttributes.asScala.values.toSeq
  }

  /**
   * Add a top level alias to the map so it can be used when resolving a grouping expression.
   */
  def addTopAggregateExpression(aliasedAggregateExpression: Alias): Unit = {
    topAggregateExpressionsByAliasName.put(
      aliasedAggregateExpression.name,
      aliasedAggregateExpression
    )
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
      childOperatorMetadataOutput = hiddenOutput,
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
   * Name resolution can be done using the hidden output for certain operators (e.g [[Sort]],
   * [[Filter]]). This is indicated by `canResolveNameByHiddenOutput` which is passed from
   * [[ExpressionResolver.resolveAttribute]] based on the parent operator.
   * Example:
   *
   * {{{
   * -- Project's output = [`col1`]; Project's hidden output = [`col1`, `col2`]
   * SELECT col1 FROM VALUES(1, 2) ORDER BY col2;
   * }}}
   *
   * The names in [[Aggregate.groupingExpressions]] can reference
   * [[Aggregate.aggregateExpressions]] aliases. `canReferenceAggregateExpressionAliases` will be
   * true when we are resolving the grouping expressions.
   * Example:
   *
   * {{ SELECT col1 + col2 AS a FROM VALUES (1, 2) GROUP BY a; }}}
   *
   * In case we are resolving names in expression trees from HAVING or ORDER BY on top of
   * [[Aggregate]], we are able to resolve hidden attributes only if those are present in
   * grouping expressions, or if the reference itself is under an [[AggregateExpression]].
   * In the latter case `canReferenceAggregatedAccessOnlyAttributes` will be true, and
   * `hiddenAttributesForResolutionWithAggregatedOnlyAccess` will be used instead of
   * `hiddenAttributesForResolution`. Consider the following example:
   *
   * {{{
   * -- This succeeds, because `col2` is in the grouping expressions.
   * SELECT COUNT(col1) FROM t1 GROUP BY col1, col2 ORDER BY col2;
   *
   * -- This fails, because `col2` is not in the grouping expressions.
   * SELECT COUNT(col1) FROM t1 GROUP BY col1 ORDER BY col2;
   *
   * -- This succeeds, despite the fact that `col2` is not in the grouping expressions.
   * -- Such references are allowed under an aggregate expression (MAX).
   * SELECT COUNT(col1) FROM t1 GROUP BY col1 ORDER BY MAX(col2);
   * }}}
   *
   * We are relying on the [[AttributeSeq]] to perform that work, since it requires complex
   * resolution logic involving nested field extraction and multipart name matching.
   *
   * Also, see [[AttributeSeq.resolve]] for more details.
   */
  def resolveMultipartName(
      multipartName: Seq[String],
      canLaterallyReferenceColumn: Boolean = true,
      canReferenceAggregateExpressionAliases: Boolean = false,
      canResolveNameByHiddenOutput: Boolean = false,
      canReferenceAggregatedAccessOnlyAttributes: Boolean = false): NameTarget = {
    val currentHiddenAttributesForResolution = if (canReferenceAggregatedAccessOnlyAttributes) {
      hiddenAttributesForResolutionWithAggregatedOnlyAccess
    } else {
      hiddenAttributesForResolution
    }

    val resolvedMultipartName: ResolvedMultipartName =
      tryResolveMultipartNameByOutput(
        multipartName,
        nameComparator,
        attributesForResolution,
        canResolveByProposedAttributes = true
      ).orElse(
          tryResolveMultipartNameByOutput(
            multipartName,
            nameComparator,
            metadataAttributesForResolution,
            canResolveByProposedAttributes = true
          )
        )
        .orElse(
          tryResolveMultipartNameByOutput(
            multipartName,
            nameComparator,
            currentHiddenAttributesForResolution,
            canResolveByProposedAttributes = canResolveNameByHiddenOutput
          )
        )
        .orElse(tryResolveMultipartNameAsLiteralFunction(multipartName))
        .orElse(
          tryResolveMultipartNameAsLateralColumnReference(
            multipartName,
            canLaterallyReferenceColumn
          )
        )
        .orElse(
          tryResolveAttributeAsGroupByAlias(multipartName, canReferenceAggregateExpressionAliases)
        )
        .getOrElse(ResolvedMultipartName(candidates = Seq.empty, referencedAttribute = None))

    resolvedMultipartName.candidates match {
      case Seq(Alias(child, aliasName)) =>
        NameTarget(
          candidates = Seq(child),
          aliasName = Some(aliasName),
          aliasMetadata = resolvedMultipartName.aliasMetadata,
          lateralAttributeReference = resolvedMultipartName.referencedAttribute,
          output = output
        )
      case other =>
        NameTarget(
          candidates = other,
          lateralAttributeReference = resolvedMultipartName.referencedAttribute,
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
   * Returns attribute with `expressionId` if `output` contains it. This is used to preserve
   * nullability for resolved [[AttributeReference]].
   */
  def getAttributeById(expressionId: ExprId): Option[Attribute] =
    Option(attributesById.get(expressionId))

  /**
   * Returns attribute with `expressionId` if `hiddenOutput` contains it.
   */
  def getHiddenAttributeById(expressionId: ExprId): Option[Attribute] =
    Option(hiddenAttributesById.get(expressionId))

  private def tryResolveMultipartNameByOutput(
      multipartName: Seq[String],
      nameComparator: NameComparator,
      attributesForResolution: AttributeSeq,
      canResolveByProposedAttributes: Boolean): Option[ResolvedMultipartName] = {
    if (canResolveByProposedAttributes) {
      val (candidates, nestedFields) =
        attributesForResolution.getCandidatesForResolution(multipartName, nameComparator)
      val resolvedCandidates = attributesForResolution.resolveCandidates(
        multipartName,
        nameComparator,
        candidates,
        nestedFields
      )
      if (resolvedCandidates.nonEmpty) {
        Some(ResolvedMultipartName(candidates = resolvedCandidates, referencedAttribute = None))
      } else {
        None
      }
    } else {
      None
    }
  }

  private def tryResolveMultipartNameAsLiteralFunction(
      multipartName: Seq[String]): Option[ResolvedMultipartName] = {
    val literalFunction = LiteralFunctionResolution.resolve(multipartName).toSeq
    if (literalFunction.nonEmpty) {
      Some(ResolvedMultipartName(candidates = literalFunction, referencedAttribute = None))
    } else {
      None
    }
  }

  private def tryResolveMultipartNameAsLateralColumnReference(
      multipartName: Seq[String],
      canLaterallyReferenceColumn: Boolean): Option[ResolvedMultipartName] = {
    val (candidatesForLca, nestedFields, referencedAttribute) =
      if (canLaterallyReferenceColumn) {
        getLcaCandidates(multipartName)
      } else {
        (Seq.empty, Seq.empty, None)
      }

    val resolvedCandidatesForLca = attributesForResolution.resolveCandidates(
      multipartName,
      nameComparator,
      candidatesForLca,
      nestedFields
    )
    if (resolvedCandidatesForLca.nonEmpty) {
      Some(
        ResolvedMultipartName(
          candidates = resolvedCandidatesForLca,
          referencedAttribute = referencedAttribute
        )
      )
    } else {
      None
    }
  }

  private def tryResolveAttributeAsGroupByAlias(
      multipartName: Seq[String],
      canReferenceAggregateExpressionAliases: Boolean): Option[ResolvedMultipartName] = {
    if (canReferenceAggregateExpressionAliases) {
      topAggregateExpressionsByAliasName.get(multipartName.head) match {
        case None =>
          None
        case Some(alias) =>
          Some(
            ResolvedMultipartName(
              candidates = Seq(alias.child),
              referencedAttribute = None,
              aliasMetadata = Some(alias.metadata)
            )
          )
      }
    } else {
      None
    }
  }

  /**
   * If a candidate was not found from output attributes, returns the candidate from lateral
   * columns. Here we do [[AttributeSeq.fromNormalOutput]] because a struct field can also be
   * laterally referenced and we need to properly resolve [[GetStructField]] node.
   */
  private def getLcaCandidates(
      multipartName: Seq[String]): (Seq[Attribute], Seq[String], Option[Attribute]) = {
    val referencedAttribute = lcaRegistry.getAttribute(multipartName.head)
    if (referencedAttribute.isDefined) {
      val attributesForResolution = AttributeSeq.fromNormalOutput(Seq(referencedAttribute.get))
      val (newCandidates, nestedFields) =
        attributesForResolution.getCandidatesForResolution(multipartName, nameComparator)
      (newCandidates, nestedFields, Some(referencedAttribute.get))
    } else {
      (Seq.empty, Seq.empty, None)
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

  private def createAttributeIds(attributes: Seq[Attribute]): HashMap[ExprId, Attribute] = {
    val result = new HashMap[ExprId, Attribute](attributes.size)
    for (attribute <- attributes) {
      result.put(attribute.exprId, attribute)
    }

    result
  }
}

/**
 * The [[NameScopeStack]] is a stack of [[NameScope]]s managed by the [[Resolver]]. Usually the
 * current scope is used for name resolution, but in case of correlated subqueries we can lookup
 * names in the parent scopes. Low-level scope creation is managed internally, and only high-level
 * api like [[withNewScope]] is available to the resolvers. Freshly-created [[NameScopeStack]]
 * contains an empty root [[NameScope]], which in the context of [[Resolver]] corresponds to the
 * query output.
 */
class NameScopeStack extends SQLConfHelper {
  private val stack = new ArrayDeque[NameScope]
  stack.push(new NameScope)

  /**
   * Get the current scope, which is a default choice for name resolution.
   */
  def current: NameScope = {
    stack.peek()
  }

  /**
   * Completely overwrite the current scope state with operator `output` and `hiddenOutput`. If
   * `hiddenOutput` is not provided, preserve the previous `hiddenOutput`. Additionally, update
   * nullabilities of attributes in hidden output from new output, so that if attribute was
   * nullable in either old hidden output or new output, it must stay nullable in new hidden
   * output as well.
   *
   * This method is called by the [[Resolver]] when we've calculated the output of an operator that
   * is being resolved. The new output is calculated based on the outputs of operator's children.
   *
   * Example for [[SubqueryAlias]], here we rewrite the current [[NameScope]]'s attributes to
   * prepend subquery qualifier to their names:
   *
   * {{{
   * val qualifier = sa.identifier.qualifier :+ sa.alias
   * scopes.overwriteCurrent(scope.output.map(attribute => attribute.withQualifier(qualifier)))
   * }}}
   *
   * Trivially, we would call this method for every operator in the query plan,
   * however some operators just propagate the output of their children without any changes, so
   * we can omit this call for them (e.g. [[Filter]]).
   *
   * This method should be preferred over [[withNewScope]].
   */
  def overwriteCurrent(
      output: Option[Seq[Attribute]] = None,
      hiddenOutput: Option[Seq[Attribute]] = None): Unit = {
    val hiddenOutputWithUpdatedNullabilities = updateHiddenOutputProperties(
      output = output.getOrElse(stack.peek().output),
      hiddenOutput = hiddenOutput.getOrElse(stack.peek().hiddenOutput)
    )
    val newScope = stack.pop.overwriteOutput(output, Some(hiddenOutputWithUpdatedNullabilities))

    stack.push(newScope)
  }

  /**
   * Overwrites output of the current [[NameScope]] entry and:
   *  1. extends hidden output with the provided output (only attributes that are not in the hidden
   *  output are added). This is done because resolution of arguments can be done through certain
   *  operators by hidden output. This use case is specific to Dataframe programs. Example:
   *
   *  {{{
   *  val df = (1 to 100).map { i => (i, i % 10, i % 2 == 0) }.toDF("a", "b", "c")
   *  df.select($"a", $"b").filter($"c")
   *  }}}
   *
   *  Unresolved tree would be:
   *
   *  Filter 'c
   *    +- 'Project ['a, 'b]
   *      +- Project [_1 AS a, _2 AS b, _3 AS c]
   *        +- LocalRelation [_1, _2, _3]
   *
   *  As it can be seen in the example above, `c` from the [[Filter]] condition should be resolved
   *  using the `hiddenOutput` (because its child output doesn't contain `c`). That's why in hidden
   *  output we have to have both hidden output from the previous scope and the provided output.
   *  This is done for [[Project]] and [[Aggregate]] operators.
   *
   *  2. updates properties of attributes in hidden output. THis includes nullabilities and access
   *  modes. See [[updateHiddenOutputProperties]] for more details.
   */
  def overwriteOutputAndExtendHiddenOutput(
      output: Seq[Attribute],
      groupingAttributeIds: Option[HashSet[ExprId]] = None): Unit = {
    val prevScope = stack.pop

    val hiddenOutputWithUpdatedProperties = updateHiddenOutputProperties(
      output = output,
      hiddenOutput = prevScope.hiddenOutput,
      groupingAttributeIds = groupingAttributeIds
    )

    val hiddenOutput = hiddenOutputWithUpdatedProperties ++ output.filter { attribute =>
        prevScope.getHiddenAttributeById(attribute.exprId).isEmpty
      }

    val newScope = prevScope.overwriteOutput(
      output = Some(output),
      hiddenOutput = Some(hiddenOutput)
    )

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
   *
   * After finishing execution of the body within the `withNewScope`, pops the stack. It also
   * propagates `hiddenOutput` upwards because of name resolution by overwriting the current
   * [[NameScope.hiddenOutput]] with the popped one. This is not done in case `withNewScope` was
   * called in the context of subquery resolution (which is indicated by `isSubqueryRoot` flag),
   * because we don't want to overwrite the existing `hiddenOutput` of the main plan.
   *
   * @param isSubqueryRoot Indicates that the current scope is a root of a subquery. This is used by
   *   [[NameScopeStack.resolveMultipartName]] to detect the nearest outer scope.
   */
  def withNewScope[R](isSubqueryRoot: Boolean = false)(body: => R): R = {
    stack.push(new NameScope(isSubqueryRoot = isSubqueryRoot))
    try {
      body
    } finally {
      val childScope = stack.pop()
      if (stack.size() > 0 && !childScope.isSubqueryRoot) {
        val currentScope = stack.pop()
        stack.push(currentScope.overwriteOutput(hiddenOutput = Some(childScope.hiddenOutput)))
      }
    }
  }

  /**
   * Resolve multipart name into a [[NameTarget]] from current or outer scopes. Currently we only
   * support one level of correlation, so we look up `multipartName` in current scope, and if the
   * name was not found, we look it up in the nearest outer scope:
   *
   * {{{
   * -- 'a' is a simple lookup from the current scope.
   * SELECT a FROM (SELECT col1 AS a FROM VALUES (1));
   * }}}
   *
   * {{{
   * -- `a` in `(SELECT a + 1)` will be wrapped in [[OuterReference]].
   * SELECT a, (SELECT a + 1) AS b FROM (SELECT col1 AS a FROM VALUES (1));
   * }}}
   *
   * The ambiguity between local and outer references is resolved in favour of current:
   * {{{
   * -- There's no correlation here, subquery references its column from the current scope.
   * -- This returns [1, 2].
   * SELECT col1, (SELECT col1 FROM VALUES (2)) AS b FROM VALUES (1)
   * }}}
   *
   * Correlations beyond one level are not supported:
   * {{{
   * -- 3 levels, fails with `UNRESOLVED_COLUMN`.
   * SELECT (
   *   SELECT (
   *     SELECT t1.col1 FROM VALUES (3) AS t3
   *   ) FROM VALUES (2) AS t2
   * ) FROM VALUES (1) AS t1;
   * }}}
   *
   * Correlated references are accessible from lower subquery operators:
   * {{{
   * -- Returns [1, 1]
   *  SELECT
   *   col1, (SELECT * FROM (SELECT t1.col1 FROM VALUES (2) AS t2))
   * FROM
   *   VALUES (1) AS t1;
   * }}}
   *
   * We cannot reference LCA or aggregate expression by alias in the outer scope:
   * {{{
   * -- These examples fail with `UNRESOLVED_COLUMN`.
   * -- LCA in outer scope.
   * SELECT col1 AS a, (SELECT a + 1) AS b FROM VALUES (1);
   * -- Aliased aggerate expression in outer scope.
   * SELECT col1 AS a FROM VALUES (1) GROUP BY a, (SELECT a + 1);
   * }}}
   *
   * Only [[Attribute]]s are wrapped in [[OuterReference]]:
   * {{{
   * -- The subquery's [[Project]] list will contain outer(col1#0).f1.f2.
   * SELECT
   *   col1, (SELECT col1.f1.f2 + 1) AS b
   * FROM
   *   VALUES (named_struct('f1', named_struct('f2', 1)));
   * }}}
   */
  def resolveMultipartName(
      multipartName: Seq[String],
      canLaterallyReferenceColumn: Boolean = true,
      canReferenceAggregateExpressionAliases: Boolean = false,
      canResolveNameByHiddenOutput: Boolean = false,
      canReferenceAggregatedAccessOnlyAttributes: Boolean = false): NameTarget = {
    val nameTargetFromCurrentScope = current.resolveMultipartName(
      multipartName,
      canLaterallyReferenceColumn = canLaterallyReferenceColumn,
      canReferenceAggregateExpressionAliases = canReferenceAggregateExpressionAliases,
      canResolveNameByHiddenOutput = canResolveNameByHiddenOutput,
      canReferenceAggregatedAccessOnlyAttributes = canReferenceAggregatedAccessOnlyAttributes
    )

    if (nameTargetFromCurrentScope.candidates.nonEmpty) {
      nameTargetFromCurrentScope
    } else {
      outer match {
        case Some(outer) =>
          val nameTarget = outer.resolveMultipartName(
            multipartName,
            canLaterallyReferenceColumn = false,
            canReferenceAggregateExpressionAliases = false,
            canReferenceAggregatedAccessOnlyAttributes = canReferenceAggregatedAccessOnlyAttributes
          )

          if (nameTarget.candidates.nonEmpty) {
            nameTarget.copy(
              isOuterReference = true,
              candidates = nameTarget.candidates.map(wrapCandidateInOuterReference)
            )
          } else {
            nameTargetFromCurrentScope
          }

        case None =>
          nameTargetFromCurrentScope
      }
    }
  }

  /**
   * Find the nearest outer scope and return it if we are in a subquery.
   */
  private def outer: Option[NameScope] = {
    var outerScope: Option[NameScope] = None

    val iter = stack.iterator
    while (iter.hasNext && !outerScope.isDefined) {
      val scope = iter.next

      if (scope.isSubqueryRoot && iter.hasNext) {
        outerScope = Some(iter.next)
      }
    }

    outerScope
  }

  /**
   * Wrap candidate in [[OuterReference]]. If the root is not an [[Attribute]], but an
   * [[ExtractValue]] (struct/map/array field reference) we find the actual [[Attribute]] and wrap
   * it in [[OuterReference]].
   */
  private def wrapCandidateInOuterReference(candidate: Expression): Expression = candidate match {
    case candidate: Attribute =>
      OuterReference(candidate)
    case extractValue: ExtractValue =>
      extractValue.transformUp {
        case attribute: Attribute => OuterReference(attribute)
        case other => other
      }
    case _ =>
      candidate
  }

  /**
   * Update attribute properties when overwriting the current outputs.
   *
   * 1. When the scope gets the new output, we need to refresh nullabilities in its `hiddenOutput`.
   * If an attribute is nullable in either old hidden output or new output, it must remain nullable
   * in new hidden output as well.
   *
   * 2. If we are updating the hidden output on top of an [[Aggregate]], HAVING and ORDER BY clauses
   * may later reference either attributes from grouping expressions, or any other attributes
   * under the condition that they are referenced under [[AggregateExpression]]. We mark those
   * attributes as [[AGGREGATED_ACCESS_ONLY]] to reference them in [[resolveMultipartName]] only
   * if `canReferenceAggregatedAccessOnlyAttributes` is set to `true`.
   * Attributes from grouping expressions lose their access metadata (e.g.
   * [[QUALIFIED_ACCESS_ONLY]]) - grouping expression attributes can be simply referenced given
   * that the relevant expression tree is canonically equal to the grouping expression tree.
   */
  private def updateHiddenOutputProperties(
      output: Seq[Attribute],
      hiddenOutput: Seq[Attribute],
      groupingAttributeIds: Option[HashSet[ExprId]] = None) = {
    val outputLookup = new HashMap[ExprId, Attribute](output.size)
    output.foreach(attribute => outputLookup.put(attribute.exprId, attribute))

    hiddenOutput.map { attribute =>
      val attributeWithUpdatedNullability = if (outputLookup.containsKey(attribute.exprId)) {
        attribute.withNullability(attribute.nullable || outputLookup.get(attribute.exprId).nullable)
      } else {
        attribute
      }

      groupingAttributeIds match {
        case Some(groupingAttributeIds) =>
          if (groupingAttributeIds.contains(attribute.exprId)) {
            attributeWithUpdatedNullability.markAsAllowAnyAccess()
          } else {
            attributeWithUpdatedNullability.markAsAggregatedAccessOnly()
          }
        case None =>
          attributeWithUpdatedNullability
      }
    }
  }
}
