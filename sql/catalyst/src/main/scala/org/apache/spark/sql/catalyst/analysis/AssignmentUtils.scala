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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.TableOutputResolver.DefaultValueFillMode.{NONE, RECURSE}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, CreateNamedStruct, Expression, GetStructField, If, IsNull, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getDefaultValueExprOrNullLit
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.ArrayImplicits._

object AssignmentUtils extends SQLConfHelper with CastSupport {

  /**
   * Aligns update assignments to match table columns.
   * <p>
   * This method processes and reorders given assignments so that each target column gets
   * an expression it should be set to. If a column does not have a matching assignment,
   * it will be set to its current value. For example, if one passes table attributes c1, c2
   * and an assignment c2 = 1, this method will return c1 = c1, c2 = 1. This allows Spark to
   * construct an updated version of a row.
   * <p>
   * This method also handles updates to nested columns. If there is an assignment to a particular
   * nested field, this method will construct a new struct with one field updated preserving other
   * fields that have not been modified. For example, if one passes table attributes c1, c2
   * where c2 is a struct with fields n1 and n2 and an assignment c2.n2 = 1, this method will
   * return c1 = c1, c2 = struct(c2.n1, 1).
   *
   * @param attrs table attributes
   * @param assignments assignments to align
   * @param fromStar whether the assignments were resolved from an UPDATE SET * clause.
   *                 These updates may assign struct fields individually
   *                 (preserving existing fields).
   * @param coerceNestedTypes whether to coerce nested types to match the target type
   *                         for complex types
   * @return aligned update assignments that match table attributes
   */
  def alignUpdateAssignments(
      attrs: Seq[Attribute],
      assignments: Seq[Assignment],
      fromStar: Boolean,
      coerceNestedTypes: Boolean): Seq[Assignment] = {

    val errors = new mutable.ArrayBuffer[String]()

    val output = attrs.map { attr =>
      applyAssignments(
        col = restoreActualType(attr),
        colExpr = attr,
        assignments,
        addError = err => errors += err,
        colPath = Seq(attr.name),
        coerceNestedTypes,
        fromStar)
    }

    if (errors.nonEmpty) {
      throw QueryCompilationErrors.invalidRowLevelOperationAssignments(assignments, errors.toSeq)
    }

    attrs.zip(output).map { case (attr, expr) => Assignment(attr, expr) }
  }

  /**
   * Aligns insert assignments to match table columns.
   * <p>
   * This method processes and reorders given assignments so that each target column gets
   * an expression it should be set to. There must be exactly one assignment for each top-level
   * attribute and its value must be compatible.
   * <p>
   * Insert assignments cannot refer to nested columns.
   *
   * @param attrs table attributes
   * @param assignments insert assignments to align
   * @param coerceNestedTypes whether to coerce nested types to match the target type
   *                          for complex types
   * @return aligned insert assignments that match table attributes
   */
  def alignInsertAssignments(
      attrs: Seq[Attribute],
      assignments: Seq[Assignment],
      coerceNestedTypes: Boolean = false): Seq[Assignment] = {

    val errors = new mutable.ArrayBuffer[String]()

    val (topLevelAssignments, nestedAssignments) = assignments.partition { assignment =>
      assignment.key.isInstanceOf[Attribute]
    }

    if (nestedAssignments.nonEmpty) {
      val nestedAssignmentsStr = nestedAssignments.map(_.sql).mkString(", ")
      errors += s"INSERT assignment keys cannot be nested fields: $nestedAssignmentsStr"
    }

    val alignedAssignments = attrs.map { attr =>
      val matchingAssignments = topLevelAssignments.collect {
        case assignment if assignment.key.semanticEquals(attr) => assignment
      }
      val resolvedValue = if (matchingAssignments.isEmpty) {
        val defaultExpr = getDefaultValueExprOrNullLit(
          attr, conf.useNullsForMissingDefaultColumnValues)
        if (defaultExpr.isEmpty) {
          errors += s"No assignment for '${attr.name}'"
        }
        defaultExpr.getOrElse(attr)
      } else if (matchingAssignments.length > 1) {
        val conflictingValuesStr = matchingAssignments.map(_.value.sql).mkString(", ")
        errors += s"Multiple assignments for '${attr.name}': $conflictingValuesStr"
        attr
      } else {
        val colPath = Seq(attr.name)
        val actualAttr = restoreActualType(attr)
        val value = matchingAssignments.head.value
        val coerceMode = if (coerceNestedTypes) RECURSE else NONE
        TableOutputResolver.resolveUpdate(
          "", value, actualAttr, conf, err => errors += err, colPath, coerceMode)
      }
      Assignment(attr, resolvedValue)
    }

    if (errors.nonEmpty) {
      throw QueryCompilationErrors.invalidRowLevelOperationAssignments(assignments, errors.toSeq)
    }

    alignedAssignments
  }

  private def restoreActualType(attr: Attribute): Attribute = {
    attr.withDataType(CharVarcharUtils.getRawType(attr.metadata).getOrElse(attr.dataType))
  }

  private def applyAssignments(
      col: Attribute,
      colExpr: Expression,
      assignments: Seq[Assignment],
      addError: String => Unit,
      colPath: Seq[String],
      coerceNestedTypes: Boolean = false,
      updateStar: Boolean = false): Expression = {

    val (exactAssignments, otherAssignments) = assignments.partition { assignment =>
      assignment.key.semanticEquals(colExpr)
    }

    val fieldAssignments = otherAssignments.filter { assignment =>
      assignment.key.exists(_.semanticEquals(colExpr))
    }

    if (exactAssignments.size > 1) {
      val conflictingValuesStr = exactAssignments.map(_.value.sql).mkString(", ")
      addError(s"Multiple assignments for '${colPath.quoted}': $conflictingValuesStr")
      colExpr
    } else if (exactAssignments.nonEmpty && fieldAssignments.nonEmpty) {
      val conflictingAssignments = exactAssignments ++ fieldAssignments
      val conflictingAssignmentsStr = conflictingAssignments.map(_.sql).mkString(", ")
      addError(s"Conflicting assignments for '${colPath.quoted}': $conflictingAssignmentsStr")
      colExpr
    } else if (exactAssignments.isEmpty && fieldAssignments.isEmpty) {
      TableOutputResolver.checkNullability(colExpr, col, conf, colPath)
    } else if (exactAssignments.nonEmpty) {
      if (updateStar && SQLConf.get.coerceMergeNestedTypes) {
        val value = exactAssignments.head.value
        col.dataType match {
          case _: StructType =>
            // Expand assignments to leaf fields (fixNullExpansion is applied inside)
            applyNestedFieldAssignments(col, colExpr, value, addError, colPath,
              coerceNestedTypes)
          case _ =>
            // For non-struct types, resolve directly
            val coerceMode = if (coerceNestedTypes) RECURSE else NONE
            TableOutputResolver.resolveUpdate("", value, col, conf, addError, colPath,
              coerceMode)
        }
      } else {
        val value = exactAssignments.head.value
        val coerceMode = if (coerceNestedTypes) RECURSE else NONE
        TableOutputResolver.resolveUpdate("", value, col, conf, addError,
          colPath, coerceMode)
      }
    } else {
      applyFieldAssignments(col, colExpr, fieldAssignments, addError, colPath, coerceNestedTypes)
    }
  }

  private def applyFieldAssignments(
      col: Attribute,
      colExpr: Expression,
      assignments: Seq[Assignment],
      addError: String => Unit,
      colPath: Seq[String],
      coerceNestedTypes: Boolean): Expression = {

    col.dataType match {
      case structType: StructType =>
        val fieldAttrs = DataTypeUtils.toAttributes(structType)
        val fieldExprs = structType.fields.zipWithIndex.map { case (field, ordinal) =>
          GetStructField(colExpr, ordinal, Some(field.name))
        }
        val updatedFieldExprs = fieldAttrs.zip(fieldExprs).map { case (fieldAttr, fieldExpr) =>
          applyAssignments(fieldAttr, fieldExpr, assignments, addError, colPath :+ fieldAttr.name,
            coerceNestedTypes)
        }
        toNamedStruct(structType, updatedFieldExprs)

      case otherType =>
        addError(
          "Updating nested fields is only supported for StructType but " +
            s"'${colPath.quoted}' is of type $otherType")
        colExpr
    }
  }

  private def applyNestedFieldAssignments(
      col: Attribute,
      colExpr: Expression,
      value: Expression,
      addError: String => Unit,
      colPath: Seq[String],
      coerceNestedTypes: Boolean): Expression = {

    col.dataType match {
      case structType: StructType =>
        val fieldAttrs = DataTypeUtils.toAttributes(structType)

        val updatedFieldExprs = fieldAttrs.zipWithIndex.map { case (fieldAttr, ordinal) =>
          val fieldPath = colPath :+ fieldAttr.name
          val targetFieldExpr = GetStructField(colExpr, ordinal, Some(fieldAttr.name))

          // Try to find a corresponding field in the source value by name
          val sourceFieldValue: Expression = value.dataType match {
            case valueStructType: StructType =>
              valueStructType.fields.find(f => conf.resolver(f.name, fieldAttr.name)) match {
                case Some(matchingField) =>
                  // Found matching field in source, extract it
                  val fieldIndex = valueStructType.fieldIndex(matchingField.name)
                  GetStructField(value, fieldIndex, Some(matchingField.name))
                case None =>
                  // Field doesn't exist in source, use target's current value with null check
                  TableOutputResolver.checkNullability(targetFieldExpr, fieldAttr, conf, fieldPath)
              }
            case _ =>
              // Value is not a struct, cannot extract field
              addError(s"Cannot assign non-struct value to struct field '${fieldPath.quoted}'")
              Literal(null, fieldAttr.dataType)
          }

          // Recurse or resolve based on field type
          fieldAttr.dataType match {
            case _: StructType =>
              // Field is a struct, recurse
              applyNestedFieldAssignments(fieldAttr, targetFieldExpr,
                sourceFieldValue, addError, fieldPath, coerceNestedTypes)
            case _ =>
              // Field is not a struct, resolve with TableOutputResolver
              val coerceMode = if (coerceNestedTypes) RECURSE else NONE
              TableOutputResolver.resolveUpdate("", sourceFieldValue, fieldAttr, conf, addError,
                fieldPath, coerceMode)
          }
        }
        val namedStruct = toNamedStruct(structType, updatedFieldExprs)

        // Prevent unnecessary null struct expansion
        fixNullExpansion(colExpr, value, structType, namedStruct, colPath)

      case otherType =>
        addError(
          "Updating nested fields is only supported for StructType but " +
            s"'${colPath.quoted}' is of type $otherType")
        colExpr
    }
  }

  private def toNamedStruct(structType: StructType, fieldExprs: Seq[Expression]): Expression = {
    val namedStructExprs = structType.fields.zip(fieldExprs).flatMap { case (field, expr) =>
      Seq(Literal(field.name), expr)
    }.toImmutableArraySeq
    CreateNamedStruct(namedStructExprs)
  }

  /**
   * Checks if target struct has extra fields compared to source struct, recursively.
   */
  private def hasExtraTargetFields(targetType: StructType, sourceType: DataType): Boolean = {
    sourceType match {
      case sourceStructType: StructType =>
        targetType.fields.exists { targetField =>
          sourceStructType.fields.find(f => conf.resolver(f.name, targetField.name)) match {
            case Some(sourceField) =>
              // Check nested structs recursively
              (targetField.dataType, sourceField.dataType) match {
                case (targetNested: StructType, sourceNested) =>
                  hasExtraTargetFields(targetNested, sourceNested)
                case _ => false
              }
            case None => true // target has extra field not in source
          }
        }
      case _ =>
        // Should be caught earlier
        throw SparkException.internalError(
          s"Source type must be StructType but found: $sourceType")
    }
  }

  /**
   * As UPDATE SET * assigns struct fields individually (preserving existing fields),
   * this will lead to indiscriminate null expansion, ie, a struct is created where all
   * fields are null.  Wraps a struct assignment with a condition to return null
   * if both conditions are true:
   *
   * - source struct is null
   * - target struct is null OR target struct is same as source struct
   *
   * If the condition is not true, we preserve the original structure.
   * This includes cases where the source was a struct of nulls,
   * or there were any extra target fields (including null ones),
   * both cases retain the assignment to a struct of nulls.
   *
   * @param key the original assignment key (target struct) expression
   * @param value the original assignment value (source struct) expression
   * @param structType the target struct type
   * @param structExpression the result create struct expression result to wrap
   * @param colPath the column path for error reporting
   * @return the wrapped expression with null checks
   */
  private def fixNullExpansion(
      key: Expression,
      value: Expression,
      structType: StructType,
      structExpression: Expression,
      colPath: Seq[String]): Expression = {
    if (key.nullable) {
      val condition = if (hasExtraTargetFields(structType, value.dataType)) {
        // extra target fields: return null iff source struct is null and target struct is null
        And(IsNull(value), IsNull(key))
      } else {
        // schemas match: return null iff source struct is null
        IsNull(value)
      }

      If(condition, Literal(null, structExpression.dataType), structExpression)
    } else {
      structExpression
    }
  }

  /**
   * Checks whether assignments are aligned and compatible with table columns.
   *
   * @param attrs table attributes
   * @param assignments assignments to check
   * @return true if the assignments are aligned
   */
  def aligned(attrs: Seq[Attribute], assignments: Seq[Assignment]): Boolean = {
    if (attrs.size != assignments.size) {
      return false
    }

    attrs.zip(assignments).forall { case (attr, assignment) =>
      val attrType = CharVarcharUtils.getRawType(attr.metadata).getOrElse(attr.dataType)
      val isMatchingAssignment = assignment.key match {
        case key: Attribute if conf.resolver(key.name, attr.name) => true
        case _ => false
      }
      isMatchingAssignment &&
        DataType.equalsIgnoreCompatibleNullability(assignment.value.dataType, attrType) &&
        (attr.nullable || !assignment.value.nullable)
    }
  }
}
