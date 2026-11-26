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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Validates resolved DDL targets, e.g. a view found for `ALTER TABLE t` is rejected.
 * Valid targets are returned unchanged.
 */
private[sql] object DdlTargetValidation {

  /** Fails unless the object found for an [[UnresolvedTable]] is a table. */
  def validateTable(unresolvedTable: UnresolvedTable, target: LogicalPlan): LogicalPlan =
    unresolvedTable match {
      case u @ UnresolvedTable(identifier, cmd, suggestAlternative) =>
        target match {
          case v: ResolvedPersistentView =>
            val nameParts = v.catalog.name() +: v.identifier.asMultipartIdentifier
            throw QueryCompilationErrors.expectTableNotViewError(
              nameParts, cmd, suggestAlternative, u)
          case _: ResolvedTempView =>
            throw QueryCompilationErrors.expectTableNotViewError(
              identifier, cmd, suggestAlternative, u)
          case table => table
        }
    }

  /** Fails when the command requires a permanent object but its target is a temporary view. */
  def validateTableOrView(
      unresolvedTableOrView: UnresolvedTableOrView,
      target: LogicalPlan): LogicalPlan = unresolvedTableOrView match {
    case u @ UnresolvedTableOrView(identifier, cmd, allowTempView, _) =>
      target match {
        case _: ResolvedTempView if !allowTempView =>
          throw QueryCompilationErrors.expectPermanentViewNotTempViewError(
            identifier, cmd, u)
        case other => other
      }
  }
}
