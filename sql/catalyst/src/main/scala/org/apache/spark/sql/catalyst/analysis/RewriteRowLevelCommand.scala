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

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRowProjection
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.write.{RowLevelOperation, RowLevelOperationInfoImpl, SupportsDelta}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait RewriteRowLevelCommand extends Rule[LogicalPlan] {

  protected def buildRowLevelOperation(
      table: SupportsRowLevelOperations,
      command: Command): RowLevelOperation = {
    val info = RowLevelOperationInfoImpl(command, CaseInsensitiveStringMap.empty())
    val builder = table.newRowLevelOperationBuilder(info)
    builder.build()
  }

  protected def dedupAttrs(attrs: Seq[AttributeReference]): Seq[AttributeReference] = {
    val exprIds = mutable.Set.empty[ExprId]
    attrs.flatMap { attr =>
      if (exprIds.contains(attr.exprId)) {
        None
      } else {
        exprIds += attr.exprId
        Some(attr)
      }
    }
  }

  protected def buildWriteDeltaProjections(
      plan: LogicalPlan,
      rowAttrs: Seq[Attribute],
      rowIdAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute]): WriteDeltaProjections = {

    val rowProjection = if (rowAttrs.nonEmpty) {
      Some(newProjection(plan, rowAttrs, usePlanTypes = true))
    } else {
      None
    }

    // in MERGE, the plan may contain both delete and insert records that may affect
    // the nullability of metadata columns (e.g. metadata columns for new records are always null)
    // since metadata columns are never passed with new records to insert,
    // use the actual metadata column type instead of the one present in the plan

    val rowIdProjection = newProjection(plan, rowIdAttrs, usePlanTypes = false)

    val metadataProjection = if (metadataAttrs.nonEmpty) {
      Some(newProjection(plan, metadataAttrs, usePlanTypes = false))
    } else {
      None
    }

    WriteDeltaProjections(rowProjection, rowIdProjection, metadataProjection)
  }

  // the projection is done by name, ignoring expr IDs
  private def newProjection(
      plan: LogicalPlan,
      attrs: Seq[Attribute],
      usePlanTypes: Boolean): InternalRowProjection = {

    val colOrdinals = attrs.map(attr => plan.output.indexWhere(_.name == attr.name))
    val schema = if (usePlanTypes) {
      val planAttrs = colOrdinals.map(plan.output(_))
      StructType.fromAttributes(planAttrs)
    } else {
      StructType.fromAttributes(attrs)
    }
    InternalRowProjection(schema, colOrdinals)
  }

  protected def resolveRequiredMetadataAttrs(
      relation: DataSourceV2Relation,
      operation: RowLevelOperation): Seq[AttributeReference] = {

    V2ExpressionUtils.resolveRefs[AttributeReference](
      operation.requiredMetadataAttributes,
      relation)
  }

  protected def resolveRowIdAttrs(
      relation: DataSourceV2Relation,
      operation: RowLevelOperation): Seq[AttributeReference] = {

    operation match {
      case supportsDelta: SupportsDelta =>
        val rowIdAttrs = V2ExpressionUtils.resolveRefs[AttributeReference](
          supportsDelta.rowId,
          relation)

        val nullableRowIdAttrs = rowIdAttrs.filter(_.nullable)
        if (nullableRowIdAttrs.nonEmpty) {
          throw new AnalysisException(s"Row ID attrs cannot be nullable: $nullableRowIdAttrs")
        }

        rowIdAttrs

      case other =>
        throw new AnalysisException(s"Operation $other does not support deltas")
    }
  }
}
