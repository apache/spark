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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{DescribeColumn, DescribeFunction, DescribeNamespace, DescribeRelation, DescribeTablePartition, LogicalPlan}
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, toPrettySQL}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.CatalogV2Util.isSessionCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{DescribeColumnCommand, DescribeDatabaseCommand, DescribeFunctionCommand, DescribeTableCommand}
import org.apache.spark.sql.internal.connector.V1Function

/**
 * Rewrites DESCRIBE plans to V1 commands when their targets support them, e.g.
 * `DESCRIBE TABLE t` becomes [[DescribeTableCommand]] for a V1 table. An unhandled plan is
 * returned unchanged.
 */
private[sql] object DescribeCommandHelper {

  /**
   * Rewrites a [[DescribeRelation]] when a V1 command describes its target.
   * Temporary and session-catalog views use the V1 command.
   */
  def rewriteDescribeRelation(describeRelation: DescribeRelation): LogicalPlan =
    describeRelation match {
    case DescribeRelation(
        resolvedChild @ ResolvedV1TableOrViewIdentifier(ident),
        isExtended,
        output) =>
      DescribeTableCommand(resolvedChild, ident, Map.empty, isExtended, output)
    case _ => describeRelation
  }

  /**
   * Rewrites a partition description, rejecting a persistent view without a V1 command.
   *
   * For `DESCRIBE TABLE <view> PARTITION (...)`, the V1 rewrite only matches session-catalog
   * views through `ResolvedV1TableOrViewIdentifier`. Non-session V2 views are rejected with
   * the same `FORBIDDEN_OPERATION` error that V1 raises at runtime in
   * `DescribeTableCommand.describeDetailedPartitionInfo`. Otherwise, `UnresolvedPartitionSpec`
   * remains unresolved on the V2 view path, and CheckAnalysis reports a generic
   * "Found the unresolved operator" INTERNAL_ERROR.
   */
  def rewriteDescribeTablePartition(
      describeTablePartition: DescribeTablePartition): LogicalPlan = describeTablePartition match {
    case DescribeTablePartition(
        resolvedChild @ ResolvedV1TableOrViewIdentifier(ident),
        UnresolvedPartitionSpec(spec, _),
        isExtended,
        output) =>
      DescribeTableCommand(resolvedChild, ident, spec, isExtended, output)

    case DescribeTablePartition(rpv: ResolvedPersistentView, _, _, _) =>
      val quoted = (rpv.catalog.name() +: rpv.identifier.asMultipartIdentifier)
        .map(quoteIfNeeded).mkString(".")
      throw QueryCompilationErrors.descPartitionNotAllowedOnView(quoted)
    case _ => describeTablePartition
  }

  /**
   * Rewrites a [[DescribeColumn]] when a V1 command describes its target.
   *
   * For views, `ResolvedPersistentView` exposes the view's schema as its `output`, so
   * `ResolveReferences` typically resolves the column to an `Attribute`. The legacy
   * `UnresolvedAttribute` form is also accepted (e.g. the parser referenced a non-existent
   * column whose resolution was skipped) to support analyzer ordering changes. The unwrap
   * logic is shared with the non-session V2 view path in `DataSourceV2Strategy`.
   */
  def rewriteDescribeColumn(describeColumn: DescribeColumn): LogicalPlan = describeColumn match {
    case DescribeColumn(ResolvedViewIdentifier(ident), column, isExtended, output) =>
      val nameParts = DescribeColumn.extractColumnNameParts(column)
      DescribeColumnCommand(ident, nameParts, isExtended, output)

    case DescribeColumn(ResolvedV1TableIdentifier(ident), column, isExtended, output) =>
      column match {
        case u: UnresolvedAttribute =>
          throw QueryCompilationErrors.columnNotFoundError(u.name)
        case a: Attribute =>
          DescribeColumnCommand(ident, a.qualifier :+ a.name, isExtended, output)
        case Alias(child, _) =>
          throw QueryCompilationErrors.commandNotSupportNestedColumnError(
            "DESC TABLE COLUMN", toPrettySQL(child))
        case _ =>
          throw SparkException.internalError(s"[BUG] unexpected column expression: $column")
      }
    case _ => describeColumn
  }

  /** Rewrites a namespace description, evaluating the V1 gate only after matching its catalog. */
  def rewriteDescribeNamespace(
      describeNamespace: DescribeNamespace,
      useV1Command: => Boolean): LogicalPlan =
    describeNamespace match {
    case DescribeNamespace(ResolvedV1Database(db), extended, output) if useV1Command =>
      DescribeDatabaseCommand(db, extended, output)
    case _ => describeNamespace
  }

  /** Rewrites a function description when its catalog supports the V1 command. */
  def rewriteDescribeFunction(describeFunction: DescribeFunction): LogicalPlan =
    describeFunction match {
    case DescribeFunction(ResolvedNonPersistentFunc(_, v1Func: V1Function), extended) =>
      DescribeFunctionCommand(v1Func.info, extended)

    case DescribeFunction(ResolvedPersistentFunc(catalog, _, func), extended) =>
      if (isSessionCatalog(catalog)) {
        DescribeFunctionCommand(func.asInstanceOf[V1Function].info, extended)
      } else {
        throw QueryCompilationErrors.missingCatalogFunctionsAbilityError(catalog)
      }
    case _ => describeFunction
  }
}
