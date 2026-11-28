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

import scala.annotation.tailrec
import scala.util.{Left => EitherLeft, Right => EitherRight}

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeSeq,
  Expression,
  ExtractValue,
  Literal,
  NonNullLiteral,
  SubqueryExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{
  LogicalPlan,
  SupervisingCommand,
  WithWindowDefinition
}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin, TreeNodeTag}
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.types.{DataType, NullType, StringType}

/**
 * Optional references: a `?` after an identifier of a column, field, or variable reference, or
 * after a named parameter marker, makes that identifier optional. When name resolution finds no
 * match for an optional identifier, the reference resolves to a static NULL of type VOID instead
 * of raising a not-found error. Ambiguity and type errors are still raised.
 *
 * {{{
 *   -- `t` has a column `id` but no column `name`:
 *   SELECT id?, name? FROM t;
 *   -- Project [id#1, null AS name#2]
 * }}}
 */
object OptionalReferences {

  /**
   * Indices of the name parts of an unresolved multipart name (an `UnresolvedAttribute` or an
   * `UnresolvedNamedLambdaVariable`) that are followed by `?`.
   */
  val OPTIONAL_NAME_PART_INDICES: TreeNodeTag[Set[Int]] =
    TreeNodeTag[Set[Int]]("optionalNamePartIndices")

  /**
   * Marks an [[UnresolvedExtractValue]] field, a named parameter marker, or an IDENTIFIER clause
   * followed by `?`.
   */
  val OPTIONAL_REFERENCE: TreeNodeTag[Unit] = TreeNodeTag[Unit]("optionalReference")

  /**
   * Marks the NULL that an unresolvable optional reference resolves to, see [[nullReference]], and
   * the alias that names it, see [[implicitAlias]].
   *
   * The two tags that the parser sets, [[OPTIONAL_NAME_PART_INDICES]] and [[OPTIONAL_REFERENCE]],
   * describe the statement: they are facts about the reference as written, which stay true in every
   * analysis of the plan. The generic transforms of a tree copy the tags of a node to the node that
   * replaces it, but only if that node has no tags of its own, see `TreeNode.copyTagsFrom`. A rule
   * that rebuilds a reference in another way forwards them itself: the optional name parts of a
   * multipart name with [[withOptionalNamePartsOf]], and the marker of a field extraction, a named
   * parameter marker, or an IDENTIFIER clause with `copyTagsFrom` into the new node. This tag
   * records a decision of one analysis, that a reference matched nothing, on the NULL and the alias
   * that this analysis creates, like [[RESOLVED_NULL_REFERENCE]] on the plan that it returns. Only
   * the analyzer sets them, and only on these nodes: the plan that the analyzer receives can be
   * analyzed again later, e.g. from a plan cache, against a relation that has the column by then.
   * The other decision of an analysis, that no resolution step of an operator can resolve a field
   * extraction, is a node of the plan that the analysis builds, see [[LastResortExtractValue]].
   */
  val NULL_REFERENCE: TreeNodeTag[Unit] = TreeNodeTag[Unit]("optionalReferenceNull")

  /**
   * Marks the root of the plan that an analysis returned, with the origin of the first optional
   * reference of the analyzed plan that the analysis resolved to a [[nullReference]], see
   * [[markResolvedNullReference]]. The rest of the analysis can replace that NULL, e.g. by folding
   * an extraction from it, the IF whose condition it is, or the row of an inline table that holds
   * it, so the mark is what keeps the decision for `CheckOptionalReferences`, which reads it
   * in the analyzed operations of a MERGE source. The optional references of the body of a view
   * or SQL function that the plan reads do not count: that body is analyzed in an analysis
   * context of its own.
   *
   * The generic transforms of a later analysis copy the mark to the node that replaces a marked
   * one only if that node has no tags of its own, see `TreeNode.copyTagsFrom`; a node with a
   * Dataset id or a Spark Connect plan id has some. So code outside of the analyzer that replaces
   * or rebuilds an analyzed node, e.g. with a case class copy, must keep the mark of the node that
   * it replaces with [[withResolvedNullReferenceOf]], which also sets it on a node with other tags,
   * as the union flattening and the join rewrites of `Dataset` do.
   */
  val RESOLVED_NULL_REFERENCE: TreeNodeTag[Origin] =
    TreeNodeTag[Origin]("optionalReferenceResolvedNull")

  /** Returns the indices of the optional name parts of `expression`. */
  def optionalNamePartIndices(expression: Expression): Set[Int] =
    expression.getTagValue(OPTIONAL_NAME_PART_INDICES).getOrElse(Set.empty)

  /** Marks the name parts at `indices` of the multipart name `expression` as optional. */
  def markNamePartsOptional(expression: Expression, indices: Set[Int]): Unit = {
    if (indices.nonEmpty) {
      expression.setTagValue(
        OPTIONAL_NAME_PART_INDICES,
        optionalNamePartIndices(expression) ++ indices
      )
    }
  }

  /** Marks `expression` as a whole as optional. */
  def markOptional(expression: Expression): Unit =
    expression.setTagValue(OPTIONAL_REFERENCE, ())

  /** Returns true if `expression` as a whole is marked optional. */
  def isOptional(expression: Expression): Boolean =
    expression.getTagValue(OPTIONAL_REFERENCE).isDefined

  /** Returns true if `expression` or any of its name parts is marked optional. */
  def hasOptionalMarker(expression: Expression): Boolean =
    isOptional(expression) || optionalNamePartIndices(expression).nonEmpty

  /**
   * Gives `expression`, a multipart name that a rule rebuilt from `source`, the optional name
   * parts of `source`, which a case class copy or a constructor does not keep.
   */
  def withOptionalNamePartsOf[T <: Expression](expression: T, source: Expression): T = {
    markNamePartsOptional(expression, optionalNamePartIndices(source))
    expression
  }

  /**
   * Returns the first unresolved expression in `expression` that is an optional reference or has
   * an optional name part, including in the plans of its subquery expressions, or None.
   */
  def unresolvedOptionalReference(expression: Expression): Option[Expression] = {
    expression.find(e => !e.resolved && hasOptionalMarker(e)).orElse {
      expression
        .collect { case subquery: SubqueryExpression => subquery.plan }
        .iterator
        .flatMap(plan => unresolvedOptionalReference(plan))
        .nextOption()
    }
  }

  /**
   * Returns true if the parsed `plan` has an optional reference anywhere, see
   * [[unresolvedOptionalReference]]. Before the analysis, every optional reference of the
   * statement is unresolved, so the answer does not depend on the data that the statement is
   * analyzed against.
   */
  def hasOptionalReferences(plan: LogicalPlan): Boolean =
    unresolvedOptionalReference(plan).isDefined

  /**
   * Returns the first unresolved optional reference of the operators of `plan`, including in the
   * plans of their subquery expressions and in the plans and expressions that an operator keeps
   * outside of its children and expressions, or None: the common table expressions of
   * `UnresolvedWith`, the query of a command such as EXPLAIN or DESCRIBE QUERY, and the window
   * specifications of a WINDOW clause.
   */
  def unresolvedOptionalReference(plan: LogicalPlan): Option[Expression] = {
    val expressions = plan match {
      case withWindows: WithWindowDefinition =>
        plan.expressions ++ withWindows.windowDefinitions.values
      case _ =>
        plan.expressions
    }
    val supervisedPlans = plan match {
      case command: SupervisingCommand => Seq(supervisedPlan(command))
      case _ => Nil
    }
    expressions.iterator
      .flatMap(expression => unresolvedOptionalReference(expression))
      .nextOption()
      .orElse {
        // The subquery plans of the operator are inner children as well, and the search of its
        // expressions has covered them, so that every plan is searched once.
        val subqueryPlans = plan.expressions.flatMap(_.collect {
          case subquery: SubqueryExpression => subquery.plan
        })
        plan.innerChildren.iterator
          .collect {
            case innerPlan: LogicalPlan if !subqueryPlans.exists(_ eq innerPlan) => innerPlan
          }
          .flatMap(innerPlan => unresolvedOptionalReference(innerPlan))
          .nextOption()
      }
      .orElse {
        (plan.children ++ supervisedPlans).iterator
          .flatMap(child => unresolvedOptionalReference(child))
          .nextOption()
      }
  }

  /** Returns the plan that `command` supervises, which is not a child of the command. */
  private def supervisedPlan(command: SupervisingCommand): LogicalPlan = {
    var supervisedPlan: LogicalPlan = null
    command.withTransformedSupervisedPlan { plan =>
      supervisedPlan = plan
      plan
    }
    supervisedPlan
  }

  /**
   * The static NULL that an unresolvable optional reference resolves to: of type VOID, or of type
   * VARIANT for a path extracted from such a NULL (`missing?:a`).
   */
  def nullReference(dataType: DataType = NullType): Literal = {
    val nullReference = Literal(null, dataType)
    nullReference.setTagValue(NULL_REFERENCE, ())
    AnalysisContext.get.nullReferenceOrigins += nullReference.origin
    nullReference
  }

  /**
   * Marks `analyzed`, the plan that an analysis of `plan` returned, with
   * [[RESOLVED_NULL_REFERENCE]] if the analysis resolved an optional reference of `plan` to a
   * [[nullReference]] and built `analyzed`. A node of `plan` is never marked: `plan` can be
   * analyzed again later, e.g. from a plan cache, and its analyzed nodes belong to other plans as
   * well, such as the plan of the Dataset that a drop returns when it drops nothing. The NULL is
   * not a part of such a node.
   */
  def markResolvedNullReference(plan: LogicalPlan, analyzed: LogicalPlan): Unit = {
    AnalysisContext.get.nullReferenceOrigins.headOption.foreach { origin =>
      if (!plan.exists(_ eq analyzed)) {
        analyzed.setTagValue(RESOLVED_NULL_REFERENCE, origin)
      }
    }
  }

  /**
   * Gives `plan`, which a rewrite outside of the analyzer built to replace the analyzed plans
   * `replaced`, the first [[RESOLVED_NULL_REFERENCE]] mark of them, unless `plan` has one. Other
   * tags stay where they are, e.g. the ids of the Datasets that the self-join detection reads.
   */
  def withResolvedNullReferenceOf[T <: LogicalPlan](plan: T, replaced: LogicalPlan*): T = {
    if (resolvedNullReferenceOrigin(plan).isEmpty) {
      replaced.iterator.flatMap(resolvedNullReferenceOrigin).nextOption().foreach { origin =>
        plan.setTagValue(RESOLVED_NULL_REFERENCE, origin)
      }
    }
    plan
  }

  /** Returns the origin of the [[RESOLVED_NULL_REFERENCE]] mark of `plan`, or None. */
  def resolvedNullReferenceOrigin(plan: LogicalPlan): Option[Origin] =
    plan.getTagValue(RESOLVED_NULL_REFERENCE)

  /**
   * Returns true if `expression` is a [[nullReference]], i.e. the result of an optional reference
   * that did not resolve to a column, field, or variable, or the [[implicitAlias]] that names
   * one. [[isNullReferenceAlias]] tells the alias apart.
   */
  def isNullReference(expression: Expression): Boolean =
    expression.getTagValue(NULL_REFERENCE).isDefined

  /**
   * Returns true if `expression`, or the child of the aliases that `expression` is, is a
   * [[nullReference]]. An extraction from a [[nullReference]] is aliased with the text of the
   * extraction, as in `arr?[0]`, see [[extractedFromNullReference]].
   */
  def holdsNullReference(expression: Expression): Boolean =
    heldNullReference(expression).isDefined

  private def heldNullReference(expression: Expression): Option[Expression] = expression match {
    case _ if isNullReference(expression) && !isNullReferenceAlias(expression) => Some(expression)
    case alias: Alias => heldNullReference(alias.child)
    case _ => None
  }

  /**
   * Returns `extracted`, the NULL that an extraction from `base` returned because `base` is NULL,
   * as a [[nullReference]] with the position of the reference if `base` holds one: the extraction
   * from a missing reference is missing too, so that an extraction from it is NULL in turn, as the
   * VARIANT path in `arr?[0]:a`.
   */
  def extractedFromNullReference(base: Expression, extracted: Expression): Expression = {
    heldNullReference(base) match {
      case Some(reference) if !isNullReference(extracted) =>
        val nullReference = CurrentOrigin.withOrigin(reference.origin) {
          Literal(null, extracted.dataType)
        }
        nullReference.setTagValue(NULL_REFERENCE, ())
        nullReference
      case _ =>
        extracted
    }
  }

  /**
   * Returns the alias that gives `child`, the result of a top-level reference, the name `name` of
   * the reference, see [[markImplicitAlias]].
   */
  def implicitAlias(child: Expression, name: String): Alias =
    markImplicitAlias(Alias(child, name)())

  /**
   * Marks `alias`, which gives the result of a top-level reference the name of the reference, if
   * that result is a [[nullReference]]. Such an alias is only there for the name of the output
   * column, which ORDER BY and HAVING can reference. Unlike other aliases of a SELECT list,
   * including `missing? AS m`, it is neither a lateral column alias nor an alias that GROUP BY
   * can reference, so that a reference without `?` is not found, see [[isNullReferenceAlias]].
   *
   * {{{
   *   SELECT missing?, missing? IS NULL, missing + 1 FROM t;
   *   -- NULL, true, and UNRESOLVED_COLUMN for the `missing` without `?`
   * }}}
   */
  def markImplicitAlias(alias: Alias): Alias = {
    if (isNullReference(alias.child)) {
      alias.setTagValue(NULL_REFERENCE, ())
    }
    alias
  }

  /** Returns true if `expression` is the [[implicitAlias]] of a [[nullReference]]. */
  def isNullReferenceAlias(expression: Expression): Boolean = expression match {
    case alias: Alias => alias.getTagValue(NULL_REFERENCE).isDefined
    case _ => false
  }

  /**
   * Extracts `nestedFields` from `base` in the same way as the `ExtractValue` chain of a multipart
   * name, where the first nested field is the name part at `firstNestedFieldIndex`. A struct field
   * that does not exist turns the whole chain into [[nullReference]] if its name part is optional,
   * without extracting the remaining fields. Any other extraction error is thrown.
   *
   * @param voidBaseIsNull Whether the fields of a NULL of type VOID are NULL, as they are for a
   *   column, or an error, as they are for a variable.
   */
  def extractNestedFields(
      base: Expression,
      nestedFields: Seq[String],
      firstNestedFieldIndex: Int,
      optionalNamePartIndices: Set[Int],
      resolver: Resolver,
      voidBaseIsNull: Boolean): Expression = {
    extractNestedFieldsOrError(
      expression = base,
      nestedFields = nestedFields,
      nestedFieldIndex = firstNestedFieldIndex,
      optionalNamePartIndices = optionalNamePartIndices,
      resolver = resolver,
      voidBaseIsNull = voidBaseIsNull
    ) match {
      case EitherLeft(extracted) => extracted
      case EitherRight(throwable) => throw throwable
    }
  }

  /**
   * Returns true if the optional multipart name `nameParts` resolves to [[nullReference]] after
   * it failed to resolve against `attributeGroups`, i.e. if its first name part that continues no
   * reference is optional. See [[firstUnmatchedNamePartIndex]].
   */
  def resolvesToNull(
      nameParts: Seq[String],
      optionalNamePartIndices: Set[Int],
      attributeGroups: Seq[Seq[Attribute]],
      resolver: Resolver,
      isVariableQualifier: Seq[String] => Boolean): Boolean = {
    optionalNamePartIndices.nonEmpty &&
    firstUnmatchedNamePartIndex(nameParts, attributeGroups, resolver, isVariableQualifier).exists(
      optionalNamePartIndices.contains
    )
  }

  /**
   * Returns the index of the first name part of `nameParts` that continues no reference, i.e. the
   * length of the longest prefix of `nameParts` that is a reference or the qualifier of one. The
   * name resolution of attributes, [[AttributeSeq.getCandidatesForResolution]], and of variables,
   * `isVariableQualifier`, decide what a prefix is, so that `?` turns only a name that they
   * resolve to nothing into NULL. A prefix is:
   *  - a reference if it resolves to an attribute and the nested fields that follow it;
   *  - a qualifier if a name part follows it, and if it qualifies a variable, see
   *    `VariableResolution.isVariableQualifier`, or an attribute: if the prefix and the name of
   *    the attribute resolve to the attribute. A qualifier is not a reference by itself, so
   *    `default.id?` and `system.v?` fail like without `?`: `default` and `system` alone are
   *    not qualifiers.
   *
   * `attributeGroups` are the groups of attributes that the name resolution tries one after the
   * other, such as the SELECT list and then the input of the SELECT list for ORDER BY, so a name
   * is ambiguous only within a group. Returns None if the whole name is a reference, which means
   * that the name was found but was not usable, e.g. because it references a column that is not
   * grouped, or if the longest prefix of a group that resolves to something resolves to more than
   * one attribute of the group, whose ambiguity error stays.
   *
   * {{{
   *   -- `t` is the table `spark_catalog.default.t` with the columns `id` and
   *   -- `addr STRUCT<city STRING>`:
   *   t.name          -- 1: `t` qualifies `id`, `name` continues nothing
   *   default.t.name  -- 2
   *   ghost.id        -- 0
   *   default.id      -- 0: `default` qualifies nothing without `t`
   *   addr.zip        -- 1
   *   t               -- 0: `t` is not a column
   *   -- `l` and `r` both have a struct column `s`:
   *   s.y             -- None: `s` is ambiguous
   * }}}
   */
  def firstUnmatchedNamePartIndex(
      nameParts: Seq[String],
      attributeGroups: Seq[Seq[Attribute]],
      resolver: Resolver,
      isVariableQualifier: Seq[String] => Boolean): Option[Int] = {
    val prefixLengths = nameParts.length to 1 by -1
    val variableQualifierLength = prefixLengths
      .find(length => length < nameParts.length && isVariableQualifier(nameParts.take(length)))
      .getOrElse(0)
    val referenceLengths = attributeGroups.map(referencePrefixLength(nameParts, _, resolver))
    if (referenceLengths.contains(None)) {
      None
    } else {
      Some((variableQualifierLength +: referenceLengths.flatten).max)
    }
  }

  /**
   * Returns the length of the longest prefix of `nameParts` that is a reference to `attributes`
   * or a qualifier of one of them, see [[firstUnmatchedNamePartIndex]], or None if the whole name
   * is a reference or if that prefix is ambiguous.
   */
  private def referencePrefixLength(
      nameParts: Seq[String],
      attributes: Seq[Attribute],
      resolver: Resolver): Option[Int] = {
    val attributeSeq = new AttributeSeq(attributes)
    lazy val attributeNames = attributes.map(_.name).distinct
    def qualifiesAttribute(prefix: Seq[String]): Boolean = attributeNames.exists { name =>
      val (candidates, nestedFields) =
        attributeSeq.getCandidatesForResolution(prefix :+ name, resolver)
      candidates.nonEmpty && nestedFields.isEmpty
    }
    // The longest prefix that resolves to something decides.
    val decisions = (nameParts.length to 1 by -1).iterator.flatMap { length =>
      val prefix = nameParts.take(length)
      val (candidates, nestedFields) = attributeSeq.getCandidatesForResolution(prefix, resolver)
      candidates.distinct match {
        case Seq(_, _, _*) =>
          Some(None)
        case Seq(attribute)
            if matchedFieldCount(attribute, nestedFields, resolver) == nestedFields.length =>
          Some(Option.when(length < nameParts.length)(length))
        case _ if length < nameParts.length && qualifiesAttribute(prefix) =>
          Some(Some(length))
        case _ =>
          None
      }
    }
    decisions.nextOption().getOrElse(Some(0))
  }

  /**
   * Returns the name with which to alias the static NULL of the optional field extraction
   * `unresolvedExtractValue` if `child` is resolved and does not have the extracted struct field,
   * or None otherwise. The name is the one that the extraction has when the field exists.
   */
  def missingOptionalFieldAliasName(
      unresolvedExtractValue: UnresolvedExtractValue,
      child: Expression,
      extraction: Expression,
      resolver: Resolver): Option[String] = {
    extraction match {
      case NonNullLiteral(fieldName, _: StringType)
          if isOptional(unresolvedExtractValue) && child.resolved =>
        ExtractValue.extractValue(child, extraction, resolver) match {
          case EitherRight(throwable) if isFieldNotFound(throwable) =>
            Some(s"${toPrettySQL(child)}.$fieldName")
          case _ =>
            None
        }
      case _ =>
        None
    }
  }

  /**
   * Extracts `nestedFields` from `expression`, or returns the error of the extraction that
   * failed. See [[extractNestedFields]].
   */
  @tailrec
  private def extractNestedFieldsOrError(
      expression: Expression,
      nestedFields: Seq[String],
      nestedFieldIndex: Int,
      optionalNamePartIndices: Set[Int],
      resolver: Resolver,
      voidBaseIsNull: Boolean): Either[Expression, Throwable] = {
    if (nestedFields.isEmpty) {
      EitherLeft(expression)
    } else if (voidBaseIsNull && expression.dataType == NullType) {
      // The name is found, so this NULL is not a `nullReference`.
      EitherLeft(Literal(null, NullType))
    } else {
      ExtractValue.extractValue(expression, Literal(nestedFields.head), resolver) match {
        case EitherLeft(extracted) =>
          extractNestedFieldsOrError(
            expression = extracted,
            nestedFields = nestedFields.tail,
            nestedFieldIndex = nestedFieldIndex + 1,
            optionalNamePartIndices = optionalNamePartIndices,
            resolver = resolver,
            voidBaseIsNull = voidBaseIsNull
          )
        case EitherRight(throwable)
            if isFieldNotFound(throwable) &&
            optionalNamePartIndices.contains(nestedFieldIndex) =>
          EitherLeft(nullReference())
        case error =>
          error
      }
    }
  }

  private def isFieldNotFound(throwable: Throwable): Boolean = throwable match {
    case sparkThrowable: SparkThrowable => sparkThrowable.getCondition == "FIELD_NOT_FOUND"
    case _ => false
  }

  /**
   * Returns the number of leading `fieldNames` that can be extracted from `expression` one after
   * the other without a field-not-found error. No other extraction error is a not-found error,
   * so all the remaining names count as matched once an extraction fails for another reason.
   */
  @tailrec
  private def matchedFieldCount(
      expression: Expression,
      fieldNames: Seq[String],
      resolver: Resolver,
      matchedCount: Int = 0): Int = {
    if (fieldNames.isEmpty) {
      matchedCount
    } else if (expression.dataType == NullType) {
      matchedCount + fieldNames.length
    } else {
      ExtractValue.extractValue(expression, Literal(fieldNames.head), resolver) match {
        case EitherLeft(extracted) =>
          matchedFieldCount(extracted, fieldNames.tail, resolver, matchedCount + 1)
        case EitherRight(throwable) if isFieldNotFound(throwable) =>
          matchedCount
        case EitherRight(_) =>
          matchedCount + fieldNames.length
      }
    }
  }
}
