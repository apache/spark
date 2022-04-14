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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.write.{RowLevelOperation, RowLevelOperationInfoImpl, RowLevelOperationTable}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait RewriteRowLevelCommand extends Rule[LogicalPlan] {

  protected def buildOperationTable(
      table: SupportsRowLevelOperations,
      command: Command,
      options: CaseInsensitiveStringMap): RowLevelOperationTable = {
    val info = RowLevelOperationInfoImpl(command, options)
    val operation = table.newRowLevelOperationBuilder(info).build()
    RowLevelOperationTable(table, operation)
  }

  protected def buildRelationWithAttrs(
      relation: DataSourceV2Relation,
      table: RowLevelOperationTable,
      metadataAttrs: Seq[AttributeReference]): DataSourceV2Relation = {

    val attrs = dedupAttrs(relation.output ++ metadataAttrs)
    relation.copy(table = table, output = attrs)
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

  protected def resolveRequiredMetadataAttrs(
      relation: DataSourceV2Relation,
      operation: RowLevelOperation): Seq[AttributeReference] = {

    V2ExpressionUtils.resolveRefs[AttributeReference](
      operation.requiredMetadataAttributes,
      relation)
  }
}
