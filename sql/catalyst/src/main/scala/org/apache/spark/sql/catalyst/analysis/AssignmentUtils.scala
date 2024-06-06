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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateNamedStruct, Expression, GetStructField, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getDefaultValueExprOrNullLit
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryCompilationErrors
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
   * @return aligned update assignments that match table attributes
   */
  def alignUpdateAssignments(
      attrs: Seq[Attribute],
      assignments: Seq[Assignment]): Seq[Assignment] = {

    val errors = new mutable.ArrayBuffer[String]()

    val output = attrs.map { attr =>
      applyAssignments(
        col = restoreActualType(attr),
        colExpr = attr,
        assignments,
        addError = err => errors += err,
        colPath = Seq(attr.name))
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
   * @return aligned insert assignments that match table attributes
   */
  def alignInsertAssignments(
      attrs: Seq[Attribute],
      assignments: Seq[Assignment]): Seq[Assignment] = {

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
        TableOutputResolver.resolveUpdate(
          "", value, actualAttr, conf, err => errors += err, colPath)
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
      colPath: Seq[String]): Expression = {

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
      val value = exactAssignments.head.value
      TableOutputResolver.resolveUpdate("", value, col, conf, addError, colPath)
    } else {
      applyFieldAssignments(col, colExpr, fieldAssignments, addError, colPath)
    }
  }

  private def applyFieldAssignments(
      col: Attribute,
      colExpr: Expression,
      assignments: Seq[Assignment],
      addError: String => Unit,
      colPath: Seq[String]): Expression = {

    col.dataType match {
      case structType: StructType =>
        val fieldAttrs = DataTypeUtils.toAttributes(structType)
        val fieldExprs = structType.fields.zipWithIndex.map { case (field, ordinal) =>
          GetStructField(colExpr, ordinal, Some(field.name))
        }
        val updatedFieldExprs = fieldAttrs.zip(fieldExprs).map { case (fieldAttr, fieldExpr) =>
          applyAssignments(fieldAttr, fieldExpr, assignments, addError, colPath :+ fieldAttr.name)
        }
        toNamedStruct(structType, updatedFieldExprs)

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
