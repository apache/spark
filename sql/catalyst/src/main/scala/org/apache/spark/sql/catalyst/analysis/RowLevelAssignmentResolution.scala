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

  /** Resolves omitted generated values against the aligned assignments of one action. */
  def resolveGeneratedColumns(
      table: LogicalPlan,
      assignments: Seq[Assignment],
      analyze: LogicalPlan => LogicalPlan): Seq[Assignment] = {
    if (!assignments.exists(_.value.isInstanceOf[GeneratedColumnValue])) {
      return assignments
    }

    val relation = table.collectFirst { case r: DataSourceV2Relation => r }.get
    val generationExpressions = GeneratedColumn.expressionReaders(relation)
    val assigned = assignments.collect {
      case Assignment(attr: Attribute, value) if !value.isInstanceOf[GeneratedColumnValue] =>
        Alias(value, attr.name)()
    }
    val input = assigned.map(_.toAttribute)
    val valuesByAttribute = AttributeMap(assigned.map(a => a.toAttribute -> a.child))

    assignments.map {
      case Assignment(attr: Attribute, _: GeneratedColumnValue) =>
        val resolved = generationExpressions(attr.name).resolve(input, analyze) { expr =>
          Cast(expr, attr.dataType, Some(conf.sessionLocalTimeZone), ansiEnabled = true)
        }
        // Substitute once: assignment right-hand sides still refer to the original input row.
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
        u.copy(assignments = resolveGeneratedColumns(table, aligned, analyze))
      case d: DeleteAction =>
        d
      case i @ InsertAction(_, assignments) =>
        val aligned = AssignmentUtils.alignInsertAssignments(table.output, assignments,
          coerceNestedTypes, ansiStoreAssignmentCastCheck, generatedColumns)
        i.copy(assignments = resolveGeneratedColumns(table, aligned, analyze))
      case other =>
        throw new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_3052",
          messageParameters = Map("other" -> other.toString))
    }
  }
}
