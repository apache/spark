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

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy

/**
 * A rule that aligns assignments with table attributes in row-level operations.
 *
 * Note that this rule must be run after resolving default values but before rewriting row-level
 * commands into executable plans. This rule does not apply to tables that accept any schema.
 * Such tables must inject their own rules to align assignments.
 */
object AlignRowLevelCommandAssignments extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u: UpdateTable if u.resolved && requiresAlignment(u.table) && !u.aligned =>
      val newTable = u.table.transform {
        case r: DataSourceV2Relation =>
          validateStoreAssignmentPolicy()
          r.copy(output = r.output.map(CharVarcharUtils.cleanAttrMetadata))
      }
      val newAssignments = AssignmentUtils.alignAssignments(u.table.output, u.assignments)
      u.copy(table = newTable, assignments = newAssignments)
  }

  private def validateStoreAssignmentPolicy(): Unit = {
    // SPARK-28730: LEGACY store assignment policy is disallowed in data source v2
    if (conf.storeAssignmentPolicy == StoreAssignmentPolicy.LEGACY) {
      throw QueryCompilationErrors.legacyStoreAssignmentPolicyError()
    }
  }

  private def requiresAlignment(table: LogicalPlan): Boolean = {
    EliminateSubqueryAliases(table) match {
      case r: NamedRelation if r.skipSchemaResolution => false
      case DataSourceV2Relation(_: SupportsRowLevelOperations, _, _, _, _) => true
      case _ => false
    }
  }
}
