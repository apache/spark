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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, LogicalPlan, MergeIntoTable, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy

/**
 * A rule that resolves assignments in row-level operations.
 *
 * Note that this rule must be run after resolving default values but before rewriting row-level
 * commands into executable plans. This rule does not apply to tables that accept any schema.
 * Such tables must inject their own rules to resolve assignments.
 */
object ResolveRowLevelCommandAssignments extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(COMMAND), ruleId) {
    case u: UpdateTable if !u.skipSchemaResolution && u.resolved &&
        supportsRowLevelOperations(u.table) && !u.aligned =>
      validateStoreAssignmentPolicy()
      val newTable = u.table.transform {
        case r: DataSourceV2Relation =>
          r.copy(output = r.output.map(CharVarcharUtils.cleanAttrMetadata))
      }
      val newAssignments = AssignmentUtils.alignAssignments(u.table.output, u.assignments)
      u.copy(table = newTable, assignments = newAssignments)

    case u: UpdateTable if !u.skipSchemaResolution && u.resolved && !u.aligned =>
      resolveAssignments(u)

    case m: MergeIntoTable if !m.skipSchemaResolution && m.resolved =>
      resolveAssignments(m)
  }

  private def validateStoreAssignmentPolicy(): Unit = {
    // SPARK-28730: LEGACY store assignment policy is disallowed in data source v2
    if (conf.storeAssignmentPolicy == StoreAssignmentPolicy.LEGACY) {
      throw QueryCompilationErrors.legacyStoreAssignmentPolicyError()
    }
  }

  private def supportsRowLevelOperations(table: LogicalPlan): Boolean = {
    EliminateSubqueryAliases(table) match {
      case DataSourceV2Relation(_: SupportsRowLevelOperations, _, _, _, _) => true
      case _ => false
    }
  }

  private def resolveAssignments(p: LogicalPlan): LogicalPlan = {
    p.transformExpressions {
      case assignment: Assignment =>
        val nullHandled = if (!assignment.key.nullable && assignment.value.nullable) {
          AssertNotNull(assignment.value)
        } else {
          assignment.value
        }
        val casted = if (assignment.key.dataType != nullHandled.dataType) {
          val cast = Cast(nullHandled, assignment.key.dataType, ansiEnabled = true)
          cast.setTagValue(Cast.BY_TABLE_INSERTION, ())
          cast
        } else {
          nullHandled
        }
        val rawKeyType = assignment.key.transform {
          case a: AttributeReference =>
            CharVarcharUtils.getRawType(a.metadata).map(a.withDataType).getOrElse(a)
        }.dataType
        val finalValue = if (CharVarcharUtils.hasCharVarchar(rawKeyType)) {
          CharVarcharUtils.stringLengthCheck(casted, rawKeyType)
        } else {
          casted
        }
        val cleanedKey = assignment.key.transform {
          case a: AttributeReference => CharVarcharUtils.cleanAttrMetadata(a)
        }
        Assignment(cleanedKey, finalValue)
    }
  }
}
