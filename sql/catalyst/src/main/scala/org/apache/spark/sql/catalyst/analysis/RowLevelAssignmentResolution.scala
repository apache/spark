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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Cast, Expression, ExprId, MetadataAttribute, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, LogicalPlan, MergeAction, UpdateAction}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, GeneratedColumn}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.connector.SchemaAlignmentConfig.AnsiStoreAssignmentCastCheck
import org.apache.spark.sql.types.DataType

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
   * Fills omitted generated-column assignments for UPDATE and MERGE actions, including INSERT
   * actions within MERGE. Regular INSERT uses WriteOutputResolution instead.
   *
   * For g GENERATED ALWAYS AS (a * 2), UPDATE t SET a = b + 1, b = 100 must assign
   * g = (old.b + 1) * 2. Resolve the generation expression against attributes for the new values,
   * then substitute the aligned assignments while preserving their references to the original row.
   * UPDATE and projection-based insert-only MERGE project shared assignment values in the rewrite.
   * Explicitly supplied generated values pass through for validation against the final row.
   */
  def resolveGeneratedColumnAssignments(
      table: LogicalPlan,
      assignments: Seq[Assignment],
      analyze: LogicalPlan => LogicalPlan,
      shareAssignedValues: Boolean = true): Seq[Assignment] = {
    if (!assignments.exists(_.value.isInstanceOf[GeneratedColumnAssignmentValue])) {
      return assignments
    }

    val relation = table.collectFirst { case r: DataSourceV2Relation => r }.get
    val generationExpressions = GeneratedColumn.expressionReaders(relation)
    val assigned = assignments.collect {
      case Assignment(attr: Attribute, value)
          if !value.isInstanceOf[GeneratedColumnAssignmentValue] =>
        attr -> Alias(value, attr.name)()
    }
    val input = assigned.map(_._2.toAttribute)
    val valuesByAttribute = AttributeMap(assigned.map { case (_, alias) =>
      val value = if (shareAssignedValues) {
        SharedAssignmentValue(alias.child, alias.exprId)
      } else {
        alias.child
      }
      alias.toAttribute -> value
    })

    val generatedAssignments = assignments.map {
      case Assignment(attr: Attribute, _: GeneratedColumnAssignmentValue) =>
        val resolved = generationExpressions(attr.name).resolve(input, analyze) { expr =>
          Cast(expr, attr.dataType, Some(conf.sessionLocalTimeZone), ansiEnabled = true)
        }
        val generated = resolved.transform {
          case a: AttributeReference if valuesByAttribute.contains(a) => valuesByAttribute(a)
        }
        resolveAssignment(Assignment(attr, generated))
      case assignment => assignment
    }

    val sharedIds = generatedAssignments.flatMap(_.value.collect {
      case value: SharedAssignmentValue => value.id
    }).toSet
    val sharedByTarget = AttributeMap(assigned.collect {
      case (attr, alias) if sharedIds.contains(alias.exprId) =>
        attr -> SharedAssignmentValue(alias.child, alias.exprId)
    })
    generatedAssignments.map {
      case Assignment(attr: Attribute, _) if sharedByTarget.contains(attr) =>
        Assignment(attr, sharedByTarget(attr))
      case assignment => assignment
    }
  }

  def alignActions(
      table: LogicalPlan,
      actions: Seq[MergeAction],
      coerceNestedTypes: Boolean,
      ansiStoreAssignmentCastCheck: AnsiStoreAssignmentCastCheck,
      generatedColumns: Set[String],
      analyze: LogicalPlan => LogicalPlan,
      shareAssignedValues: Boolean = false): Seq[MergeAction] = {
    actions.map {
      case u @ UpdateAction(_, assignments, fromStar) =>
        val aligned = AssignmentUtils.alignUpdateAssignments(table.output, assignments,
          fromStar, coerceNestedTypes, ansiStoreAssignmentCastCheck, generatedColumns)
        u.copy(assignments = resolveGeneratedColumnAssignments(
          table, aligned, analyze, shareAssignedValues))
      case d: DeleteAction =>
        d
      case i @ InsertAction(_, assignments) =>
        val aligned = AssignmentUtils.alignInsertAssignments(table.output, assignments,
          coerceNestedTypes, ansiStoreAssignmentCastCheck, generatedColumns)
        i.copy(assignments = resolveGeneratedColumnAssignments(
          table, aligned, analyze, shareAssignedValues))
      case other =>
        throw new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_3052",
          messageParameters = Map("other" -> other.toString))
    }
  }
}

/** An assigned value that a row-level rewrite shares with omitted generated columns. */
private[sql] case class SharedAssignmentValue(child: Expression, id: ExprId)
  extends UnaryExpression with Unevaluable {
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override protected def withNewChildInternal(newChild: Expression): SharedAssignmentValue =
    copy(child = newChild)
}
