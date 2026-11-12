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
  Expression,
  ExtractValue,
  Literal,
  NonNullLiteral
}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.{toPrettySQL, MetadataColumnHelper}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.types.{NullType, StringType}

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

  /** Marks an optional [[UnresolvedExtractValue]] as a last resort, see [[markLastResort]]. */
  val LAST_RESORT: TreeNodeTag[Unit] = TreeNodeTag[Unit]("optionalReferenceLastResort")

  /**
   * Marks the NULL that an unresolvable optional reference resolves to, see [[nullReference]], and
   * the alias that names it, see [[implicitAlias]].
   */
  val NULL_REFERENCE: TreeNodeTag[Unit] = TreeNodeTag[Unit]("optionalReferenceNull")

  /** Qualifiers of session variables, which are valid prefixes of a variable reference. */
  private val SESSION_VARIABLE_QUALIFIERS = Seq(
    Seq(CatalogManager.SYSTEM_CATALOG_NAME, CatalogManager.SESSION_NAMESPACE),
    Seq(CatalogManager.SESSION_NAMESPACE)
  )

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
   * Marks the optional field extraction `extractValue` as one that no name resolution step of its
   * operator could resolve, so that its missing field is NULL in the next step that resolves its
   * child. Until then, a step whose errors are not final leaves it unresolved, like the same
   * extraction without `?`, because a later step can still resolve it. For example, ORDER BY
   * resolves names against the SELECT list before the columns of the FROM clause:
   *
   * {{{
   *   -- `t.arr` is an ARRAY<STRUCT<a INT, b INT>>, so ORDER BY sorts by its field `b`.
   *   SELECT array(named_struct('a', 1)) AS arr FROM t ORDER BY arr[0].b?;
   * }}}
   */
  def markLastResort(extractValue: UnresolvedExtractValue): Unit =
    extractValue.setTagValue(LAST_RESORT, ())

  /** Returns true if `extractValue` is marked as a last resort, see [[markLastResort]]. */
  def isLastResort(extractValue: UnresolvedExtractValue): Boolean =
    extractValue.getTagValue(LAST_RESORT).isDefined

  /** The static NULL of type VOID that an unresolvable optional reference resolves to. */
  def nullReference(): Literal = {
    val nullReference = Literal(null, NullType)
    nullReference.setTagValue(NULL_REFERENCE, ())
    nullReference
  }

  /**
   * Returns true if `expression` is a [[nullReference]], i.e. the result of an optional reference
   * that did not resolve to a column, field, or variable.
   */
  def isNullReference(expression: Expression): Boolean =
    expression.getTagValue(NULL_REFERENCE).isDefined

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
   * it failed to resolve against `attributes`, i.e. if its first name part that matches no
   * reference path to `attributes` is optional. See [[firstUnmatchedNamePartIndex]].
   */
  def resolvesToNull(
      nameParts: Seq[String],
      optionalNamePartIndices: Set[Int],
      attributes: Seq[Attribute],
      resolver: Resolver): Boolean = {
    optionalNamePartIndices.nonEmpty &&
    firstUnmatchedNamePartIndex(nameParts, attributes, resolver).exists(
      optionalNamePartIndices.contains
    )
  }

  /**
   * Returns the index of the first name part of `nameParts` that does not continue any reference
   * path to `attributes`, where a path is a suffix of the attribute qualifier, the attribute name,
   * and then the names of nested fields. Session variable qualifiers are valid paths too. A
   * qualifier is not a reference by itself, so the last name part never matches a qualifier.
   * Returns None if the whole name matches a path, which means that the name was found but was not
   * usable, e.g. because it references a column that is not grouped.
   *
   * {{{
   *   -- `t` has the columns `id` and `addr STRUCT<city STRING>`:
   *   t.name    -- 1: `t` matches the qualifier of `id`, `name` matches nothing
   *   ghost.id  -- 0
   *   addr.zip  -- 1
   *   t         -- 0: `t` is not a column
   * }}}
   */
  def firstUnmatchedNamePartIndex(
      nameParts: Seq[String],
      attributes: Seq[Attribute],
      resolver: Resolver): Option[Int] = {
    val matchedNamePartCounts =
      attributes.iterator.map(matchedNamePartCount(nameParts, _, resolver)) ++
      SESSION_VARIABLE_QUALIFIERS.iterator.map(matchedQualifierPartCount(nameParts, _, resolver))
    val longestMatch = matchedNamePartCounts.max
    if (longestMatch < nameParts.length) Some(longestMatch) else None
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
   * Returns the number of leading name parts of `nameParts` that match the longest reference path
   * to `attribute`. A qualified-access-only attribute requires at least one qualifier part.
   */
  private def matchedNamePartCount(
      nameParts: Seq[String],
      attribute: Attribute,
      resolver: Resolver): Int = {
    val minQualifierLength = if (attribute.qualifiedAccessOnly) 1 else 0
    (minQualifierLength to attribute.qualifier.length).iterator
      .map { qualifierLength =>
        val qualifier = attribute.qualifier.takeRight(qualifierLength)
        val matchedQualifierParts = matchedQualifierPartCount(nameParts, qualifier, resolver)
        if (matchedQualifierParts < qualifier.length) {
          matchedQualifierParts
        } else if (!resolver(attribute.name, nameParts(qualifier.length))) {
          qualifier.length
        } else {
          qualifier.length + 1 +
          matchedFieldCount(attribute, nameParts.drop(qualifier.length + 1), resolver)
        }
      }
      .foldLeft(0)(math.max)
  }

  /**
   * Returns the number of leading name parts of `nameParts` that match the leading parts of
   * `qualifier`. The last name part is not compared, because a reference needs at least one name
   * part after its qualifier.
   */
  private def matchedQualifierPartCount(
      nameParts: Seq[String],
      qualifier: Seq[String],
      resolver: Resolver): Int = {
    qualifier
      .zip(nameParts.init)
      .takeWhile {
        case (qualifierPart, namePart) => resolver(qualifierPart, namePart)
      }
      .length
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
