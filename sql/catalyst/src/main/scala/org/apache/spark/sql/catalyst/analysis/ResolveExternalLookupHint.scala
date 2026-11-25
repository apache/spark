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

import java.util.Locale

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions.{Expression, StringLiteral}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupHint,
  ExternalLookupHintCacheOptions, LogicalPlan, SubqueryAlias, UnaryNode, UnresolvedHint,
  UnresolvedWith, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_HINT
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.util.Utils

/**
 * Resolves the `EXTERNAL_LOOKUP` hint to [[ExternalLookupHint]] markers. Its parameters are
 * relation names and cache options:
 *  - A relation name is an identifier, or a string that does not contain `=`. As with join
 *    strategy hints, a hint without relation names marks the plan it is attached to, and a hint
 *    with relation names marks each named relation.
 *  - A cache option is a string of the form `key=value`, such as `'cache_ttl=5s'`. It overrides a
 *    `spark.sql.join.externalLookup.cache.*` configuration for the joins that the hint marks. The
 *    marker keeps only the options that the hint sets.
 */
object ResolveExternalLookupHint extends Rule[LogicalPlan] with QueryErrorsBase {
  private val HINT_NAME = "EXTERNAL_LOOKUP"
  private val CACHE_ENABLED_OPTION = "cache_enabled"
  private val CACHE_MAX_BYTES_OPTION = "cache_max_bytes"
  private val CACHE_TTL_OPTION = "cache_ttl"
  private val SUPPORTED_OPTIONS =
    Set(CACHE_ENABLED_OPTION, CACHE_MAX_BYTES_OPTION, CACHE_TTL_OPTION)
  private val POSITIVE_SIZE = "a positive size, such as 64k, 16m, or 1g"
  private val NON_NEGATIVE_DURATION =
    "a non-negative duration, such as 500ms, 5s, or 10m; a number without a unit is in milliseconds"

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperatorsUpWithPruning(_.containsPattern(UNRESOLVED_HINT), ruleId) {
      case UnresolvedHint(name, parameters, child)
          if name.toUpperCase(Locale.ROOT) == HINT_NAME =>
        resolveHint(parameters, child)
    }

  private def resolveHint(parameters: Seq[Expression], child: LogicalPlan): LogicalPlan = {
    val relationNames = mutable.ArrayBuffer.empty[Seq[String]]
    var cacheOptions = ExternalLookupHintCacheOptions()
    val seenOptions = mutable.HashSet.empty[String]
    // The Dataset API, PySpark and Spark Connect pass a relation name as a string, as they do for
    // join strategy hints. A relation whose name contains `=` is named with an identifier.
    def addString(value: String): Unit =
      if (value.contains('=')) {
        cacheOptions = parseOption(value, cacheOptions, seenOptions)
      } else {
        relationNames += UnresolvedAttribute.parseAttributeName(value)
      }
    parameters.foreach {
      case StringLiteral(value) => addString(value)
      case attr: UnresolvedAttribute =>
        relationNames += attr.nameParts
      case other =>
        throw QueryCompilationErrors.invalidExternalLookupHintError(
          "UNSUPPORTED_PARAMETER", Map("parameter" -> toSQLExpr(other)))
    }
    if (relationNames.isEmpty) {
      ExternalLookupHint(child, cacheOptions)
    } else {
      // As with join strategy hints, a name marks every relation that it matches. `matched` only
      // tells which names matched nothing.
      val matched = mutable.HashSet.empty[Seq[String]]
      def matches(identifier: Seq[String]): Boolean = {
        val names = relationNames.filter { name =>
          ResolveHints.matchedIdentifier(name, identifier, conf.resolver)
        }
        matched ++= names
        names.nonEmpty
      }
      def named(plan: LogicalPlan): Boolean = plan match {
        case relation: UnresolvedRelation => matches(relation.multipartIdentifier)
        case alias: SubqueryAlias => matches(alias.identifier.qualifier :+ alias.identifier.name)
        case _ => false
      }
      // Whether the operators with one child below `plan` lead to a relation that the hint names.
      @tailrec
      def leadsToNamed(plan: LogicalPlan): Boolean = plan match {
        case _: UnresolvedRelation | _: SubqueryAlias => named(plan)
        case _: View | _: UnresolvedWith => false
        case unary: UnaryNode => leadsToNamed(unary.child)
        case _ => false
      }
      // Mirrors the target scoping of ResolveHints.ResolveJoinStrategyHints: a name matches an
      // unaliased (possibly multipart) table or an alias, and does not reach into a view,
      // a WITH clause, or an aliased subquery. A marker that an inner hint put on the way to a
      // named relation stays below this hint's marker, so that the outer hint's options win, as
      // with join strategy hints.
      def applyHint(plan: LogicalPlan): LogicalPlan = plan match {
        case marker: ExternalLookupHint if leadsToNamed(marker.child) =>
          ExternalLookupHint(marker, cacheOptions)
        case _: UnresolvedRelation | _: SubqueryAlias if named(plan) =>
          ExternalLookupHint(plan, cacheOptions)
        case _: View | _: UnresolvedWith | _: SubqueryAlias => plan
        case _ => plan.mapChildren(applyHint)
      }
      val rewritten = applyHint(child)
      // As with join strategy hints, a name may be repeated.
      val missing = relationNames.distinct.filterNot(matched.contains)
      if (missing.nonEmpty) {
        throw QueryCompilationErrors.invalidExternalLookupHintError(
          "RELATION_NOT_FOUND", Map("relations" -> missing.map(toSQLId).mkString(", ")))
      }
      rewritten
    }
  }

  private def parseOption(
      option: String,
      cacheOptions: ExternalLookupHintCacheOptions,
      seenOptions: mutable.Set[String]): ExternalLookupHintCacheOptions = {
    val separator = option.indexOf('=')
    if (separator <= 0 || separator == option.length - 1) {
      throw QueryCompilationErrors.invalidExternalLookupOptionError(option, "INVALID_OPTION_FORMAT")
    }
    val name = option.substring(0, separator).trim.toLowerCase(Locale.ROOT)
    val value = option.substring(separator + 1).trim
    if (!SUPPORTED_OPTIONS.contains(name)) {
      throw QueryCompilationErrors.invalidExternalLookupOptionError(
        name,
        "UNSUPPORTED_OPTION",
        Map("supportedOptions" -> SUPPORTED_OPTIONS.toSeq.sorted.map(toDSOption).mkString(", ")))
    }
    if (!seenOptions.add(name)) {
      throw QueryCompilationErrors.invalidExternalLookupOptionError(name, "DUPLICATE_OPTION")
    }

    name match {
      case CACHE_ENABLED_OPTION =>
        value.toLowerCase(Locale.ROOT) match {
          case "true" => cacheOptions.copy(enabled = Some(true))
          case "false" => cacheOptions.copy(enabled = Some(false))
          case _ => invalidOptionValue(CACHE_ENABLED_OPTION, "true or false")
        }
      case CACHE_MAX_BYTES_OPTION =>
        cacheOptions.copy(maxBytes = Some(parsePositiveSize(CACHE_MAX_BYTES_OPTION, value)))
      case CACHE_TTL_OPTION =>
        cacheOptions.copy(
          ttlMillis = Some(parseNonNegativeDurationMillis(CACHE_TTL_OPTION, value)))
    }
  }

  private def parsePositiveSize(optionName: String, value: String): Long = {
    val parsed = try {
      Utils.byteStringAsBytes(value)
    } catch {
      case NonFatal(_) => invalidOptionValue(optionName, POSITIVE_SIZE)
    }
    if (parsed <= 0) invalidOptionValue(optionName, POSITIVE_SIZE)
    parsed
  }

  private def parseNonNegativeDurationMillis(optionName: String, value: String): Long = {
    val parsed = try {
      Utils.timeStringAsMs(value)
    } catch {
      case NonFatal(_) => invalidOptionValue(optionName, NON_NEGATIVE_DURATION)
    }
    if (parsed < 0) invalidOptionValue(optionName, NON_NEGATIVE_DURATION)
    parsed
  }

  private def invalidOptionValue(optionName: String, expected: String): Nothing = {
    throw QueryCompilationErrors.invalidExternalLookupOptionError(
      optionName, "INVALID_OPTION_VALUE", Map("expected" -> expected))
  }
}
