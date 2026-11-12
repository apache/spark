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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, MetadataAttribute}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, MergeAction, UpdateAction}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.internal.connector.SchemaAlignmentConfig.AnsiStoreAssignmentCastCheck

/** Assignment validation and value conversion for UPDATE and MERGE actions. */
private[sql] object RowLevelAssignmentResolution {

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

  def alignActions(
      attrs: Seq[Attribute],
      actions: Seq[MergeAction],
      coerceNestedTypes: Boolean,
      ansiStoreAssignmentCastCheck: AnsiStoreAssignmentCastCheck): Seq[MergeAction] = {
    actions.map {
      case u @ UpdateAction(_, assignments, fromStar) =>
        u.copy(assignments = AssignmentUtils.alignUpdateAssignments(attrs, assignments,
          fromStar, coerceNestedTypes, ansiStoreAssignmentCastCheck))
      case d: DeleteAction =>
        d
      case i @ InsertAction(_, assignments) =>
        i.copy(assignments = AssignmentUtils.alignInsertAssignments(attrs, assignments,
          coerceNestedTypes, ansiStoreAssignmentCastCheck))
      case other =>
        throw new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_3052",
          messageParameters = Map("other" -> other.toString))
    }
  }
}
