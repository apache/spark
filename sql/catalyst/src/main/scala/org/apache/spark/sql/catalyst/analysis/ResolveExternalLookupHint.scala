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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, StringLiteral}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupCacheOptions,
  ExternalLookupHint, LogicalPlan, SubqueryAlias, UnresolvedHint, UnresolvedWith, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_HINT
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Resolves the `EXTERNAL_LOOKUP` hint to [[ExternalLookupHint]] markers. As with join strategy
 * hints, a hint without parameters marks the plan it is attached to, and a hint with relation
 * names, given as identifiers or strings, marks each named relation.
 */
object ResolveExternalLookupHint extends Rule[LogicalPlan] with QueryErrorsBase {
  private val HINT_NAME = "EXTERNAL_LOOKUP"
  private val CACHE_ENABLED_OPTION = "cache_enabled"
  private val CACHE_MAX_ENTRIES_OPTION = "cache_max_entries"
  private val CACHE_TTL_OPTION = "cache_ttl"
  private val CACHE_MAX_ROWS_PER_KEY_OPTION = "cache_max_rows_per_key"
  private val SUPPORTED_OPTIONS = Set(
    CACHE_ENABLED_OPTION,
    CACHE_MAX_ENTRIES_OPTION,
    CACHE_TTL_OPTION,
    CACHE_MAX_ROWS_PER_KEY_OPTION)
  private val NON_NEGATIVE_DURATION = "a non-negative duration, such as 500ms, 5s, or 10m"

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperatorsUpWithPruning(_.containsPattern(UNRESOLVED_HINT), ruleId) {
      case UnresolvedHint(name, parameters, child)
          if name.toUpperCase(Locale.ROOT) == HINT_NAME =>
        resolveHint(parameters, child)
    }

  private def resolveHint(parameters: Seq[Expression], child: LogicalPlan): LogicalPlan = {
    val relationNames = mutable.ArrayBuffer.empty[Seq[String]]
    var cacheOptions = defaultCacheOptions
    val seenOptions = mutable.HashSet.empty[String]
    parameters.foreach {
      case StringLiteral(value) =>
        cacheOptions = parseOption(value, cacheOptions, seenOptions)
      case Literal(value: String, _) =>
        cacheOptions = parseOption(value, cacheOptions, seenOptions)
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
      // Mirrors the target scoping of ResolveHints.ResolveJoinStrategyHints: a name matches an
      // unaliased (possibly multipart) table or an alias, and does not reach into a view,
      // a WITH clause, or an aliased subquery.
      def applyHint(plan: LogicalPlan): LogicalPlan = plan match {
        case relation: UnresolvedRelation if matches(relation.multipartIdentifier) =>
          ExternalLookupHint(relation, cacheOptions)
        case alias: SubqueryAlias
            if matches(alias.identifier.qualifier :+ alias.identifier.name) =>
          ExternalLookupHint(alias, cacheOptions)
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

  private[analysis] def defaultCacheOptions: ExternalLookupCacheOptions = {
    val conf = SQLConf.get
    ExternalLookupCacheOptions(
      enabled = conf.getConf(SQLConf.JOIN_EXTERNAL_LOOKUP_CACHE_ENABLED),
      maxEntries = conf.getConf(SQLConf.JOIN_EXTERNAL_LOOKUP_CACHE_MAX_ENTRIES),
      ttlMillis = conf.getConf(SQLConf.JOIN_EXTERNAL_LOOKUP_CACHE_TTL),
      maxRowsPerKey = conf.getConf(SQLConf.JOIN_EXTERNAL_LOOKUP_CACHE_MAX_ROWS_PER_KEY))
  }

  private def parseOption(
      option: String,
      cacheOptions: ExternalLookupCacheOptions,
      seenOptions: mutable.Set[String]): ExternalLookupCacheOptions = {
    val separator = option.indexOf('=')
    if (separator <= 0 || separator == option.length - 1) {
      throw QueryCompilationErrors.invalidExternalLookupOptionError(option, "INVALID_FORMAT")
    }
    val name = option.substring(0, separator).trim.toLowerCase(Locale.ROOT)
    val value = option.substring(separator + 1).trim
    if (!SUPPORTED_OPTIONS.contains(name)) {
      throw QueryCompilationErrors.invalidExternalLookupOptionError(
        name,
        "UNSUPPORTED",
        Map("supportedOptions" -> SUPPORTED_OPTIONS.toSeq.sorted.map(toDSOption).mkString(", ")))
    }
    if (!seenOptions.add(name)) {
      throw QueryCompilationErrors.invalidExternalLookupOptionError(name, "DUPLICATE")
    }

    name match {
      case CACHE_ENABLED_OPTION =>
        value.toLowerCase(Locale.ROOT) match {
          case "true" => cacheOptions.copy(enabled = true)
          case "false" => cacheOptions.copy(enabled = false)
          case _ => invalidOptionValue(CACHE_ENABLED_OPTION, "true or false")
        }
      case CACHE_MAX_ENTRIES_OPTION =>
        cacheOptions.copy(maxEntries = parsePositiveInt(CACHE_MAX_ENTRIES_OPTION, value))
      case CACHE_TTL_OPTION =>
        cacheOptions.copy(ttlMillis = parseNonNegativeDurationMillis(CACHE_TTL_OPTION, value))
      case CACHE_MAX_ROWS_PER_KEY_OPTION =>
        cacheOptions.copy(maxRowsPerKey = parsePositiveInt(CACHE_MAX_ROWS_PER_KEY_OPTION, value))
    }
  }

  private def parsePositiveInt(optionName: String, value: String): Int = {
    val parsed = try {
      value.toInt
    } catch {
      case NonFatal(_) => invalidOptionValue(optionName, "a positive integer")
    }
    if (parsed <= 0) invalidOptionValue(optionName, "a positive integer")
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
      optionName, "INVALID_VALUE", Map("expected" -> expected))
  }
}
