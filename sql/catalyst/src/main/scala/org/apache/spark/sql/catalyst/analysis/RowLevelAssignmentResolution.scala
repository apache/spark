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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Cast, MetadataAttribute}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, LogicalPlan, MergeAction, UpdateAction}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, GeneratedColumn}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.connector.SchemaAlignmentConfig.AnsiStoreAssignmentCastCheck

/** Assignment validation and value conversion for UPDATE and MERGE actions. */
private[sql] object RowLevelAssignmentResolution extends SQLConfHelper {

  def metadataColumnAssignmentErrors(assignments: Seq[Assignment]): Seq[String] = {
    assignments.map(_.key).filter(_.resolved).flatMap(_.references)
      .filter(attr => MetadataAttribute.isValid(attr.metadata))
      .map(_.name)
      .distinct
      .map(name => s"Cannot assign to metadata column '$name'")
  }

  /** Adds null, store-assignment and CHAR/VARCHAR checks to one resolved assignment. */
  def resolveAssignment(assignment: Assignment): Assignment = {
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

  /**
   * Fills omitted generated values for UPDATE/MERGE. Regular INSERT uses WriteOutputResolution.
   * Example: g = a * 2 with SET a = b + 1, b = 100 becomes (old.b + 1) * 2.
   */
  def resolveGeneratedColumnAssignments(
      table: LogicalPlan,
      assignments: Seq[Assignment],
      analyze: LogicalPlan => LogicalPlan): Seq[Assignment] = {
    if (!assignments.exists(_.value.isInstanceOf[GeneratedColumnAssignmentValue])) {
      return assignments
    }

    val relation = table.collectFirst { case r: DataSourceV2Relation => r }.get
    val generationExpressions = GeneratedColumn.expressionReaders(relation)
    // assigned aliases old.b + 1 as a and 100 as b, each with a fresh attribute ID.
    val assigned = assignments.collect {
      case Assignment(attr: Attribute, value)
          if !value.isInstanceOf[GeneratedColumnAssignmentValue] =>
        Alias(value, attr.name)()
    }
    // input exposes the new a and b; valuesByAttribute maps them back to old.b + 1 and 100.
    val input = assigned.map(_.toAttribute)
    val valuesByAttribute = AttributeMap(assigned.map(a => a.toAttribute -> a.child))

    assignments.map {
      case Assignment(attr: Attribute, _: GeneratedColumnAssignmentValue) =>
        // resolved binds a * 2 to input's new a, before substituting assigned values.
        val resolved = generationExpressions(attr.name).resolve(input, analyze) { expr =>
          Cast(expr, attr.dataType, Some(conf.sessionLocalTimeZone), ansiEnabled = true)
        }
        // generated uses valuesByAttribute once: (old.b + 1) * 2, keeping the original b.
        val generated = resolved.transform {
          case a: AttributeReference if valuesByAttribute.contains(a) => valuesByAttribute(a)
        }
        resolveAssignment(Assignment(attr, generated))
      case assignment => assignment
    }
  }

  def alignActions(
      table: LogicalPlan,
      actions: Seq[MergeAction],
      coerceNestedTypes: Boolean,
      ansiStoreAssignmentCastCheck: AnsiStoreAssignmentCastCheck,
      generatedColumns: Set[String],
      analyze: LogicalPlan => LogicalPlan): Seq[MergeAction] = {
    actions.map {
      case u @ UpdateAction(_, assignments, fromStar) =>
        val aligned = AssignmentUtils.alignUpdateAssignments(table.output, assignments,
          fromStar, coerceNestedTypes, ansiStoreAssignmentCastCheck, generatedColumns)
        u.copy(assignments = resolveGeneratedColumnAssignments(table, aligned, analyze))
      case d: DeleteAction =>
        d
      case i @ InsertAction(_, assignments) =>
        val aligned = AssignmentUtils.alignInsertAssignments(table.output, assignments,
          coerceNestedTypes, ansiStoreAssignmentCastCheck, generatedColumns)
        i.copy(assignments = resolveGeneratedColumnAssignments(table, aligned, analyze))
      case other =>
        throw new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_3052",
          messageParameters = Map("other" -> other.toString))
    }
  }
}
