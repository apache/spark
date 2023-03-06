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
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, CreateNamedStruct, Expression, GetArrayItem, GetArrayStructFields, GetMapValue, GetStructField, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{DataType, StructType}

object AssignmentUtils extends SQLConfHelper with CastSupport {

  private case class ColumnUpdate(ref: Seq[String], expr: Expression)

  /**
   * Aligns assignments to match table columns.
   * <p>
   * This method processes and reorders given assignments so that each target column gets
   * an expression it should be set to. If a column does not have a matching assignment,
   * it will be set to its current value. For example, if one passes table attributes c1, c2
   * and an assignment c2 = 1, this method will return c1 = c1, c2 = 1. This alignment is
   * required to construct an updated version of a row.
   * <p>
   * This method also handles updates to nested columns. If there is an assignment to a particular
   * nested field, this method will construct a new struct with one field updated preserving other
   * fields that have not been modified. For example, if one passes table attributes c1, c2
   * where c2 is a struct with fields n1 and n2 and an assignment c2.n2 = 1, this method will
   * return c1 = c1, c2 = struct(c2.n1, 1).
   *
   * @param attrs table attributes
   * @param assignments assignments to align
   * @return aligned assignments that match table attributes
   */
  def alignAssignments(
      attrs: Seq[Attribute],
      assignments: Seq[Assignment]): Seq[Assignment] = {

    val errors = new mutable.ArrayBuffer[String]()

    val output = applyUpdates(
      updates = assignments.map(toColumnUpdate),
      cols = attrs.map(restoreActualType),
      colExprs = attrs,
      addError = err => errors += err)

    if (errors.nonEmpty) {
      throw QueryCompilationErrors.invalidRowLevelOperationAssignments(assignments, errors.toSeq)
    }

    attrs.zip(output).map { case (attr, expr) => Assignment(attr, expr) }
  }

  private def toColumnUpdate(assignment: Assignment): ColumnUpdate = {
    ColumnUpdate(toRef(assignment.key), assignment.value)
  }

  private def restoreActualType(attr: Attribute): Attribute = {
    attr.withDataType(CharVarcharUtils.getRawType(attr.metadata).getOrElse(attr.dataType))
  }

  private def applyUpdates(
      updates: Seq[ColumnUpdate],
      cols: Seq[Attribute],
      colExprs: Seq[Expression],
      addError: String => Unit,
      colPath: Seq[String] = Nil): Seq[Expression] = {

    // iterate through columns at the current level and find matching updates
    cols.zip(colExprs).map { case (col, colExpr) =>
      // find matches for this column or any of its children
      val prefixMatchedUpdates = updates.filter(update => conf.resolver(update.ref.head, col.name))
      val newColPath = colPath :+ col.name
      prefixMatchedUpdates match {
        // if there is no exact match and no match for children, return the column expr as is
        case matchedUpdates if matchedUpdates.isEmpty =>
          TableOutputResolver.checkNullability(colExpr, col, conf, colPath)

        // if there is only one update and it is an exact match, resolve the assigned expr
        case Seq(matchedUpdate) if isExactMatch(matchedUpdate, col) =>
          TableOutputResolver.resolveUpdate(matchedUpdate.expr, col, conf, addError, newColPath)

        // if there are matches only for children
        case matchedUpdates if !hasExactMatch(matchedUpdates, col) =>
          col.dataType match {
            case colType: StructType =>
              val fieldExprs = colType.fields.zipWithIndex.map { case (field, ordinal) =>
                GetStructField(colExpr, ordinal, Some(field.name))
              }

              val updatedFieldExprs = applyUpdates(
                matchedUpdates.map(update => update.copy(ref = update.ref.tail)),
                colType.toAttributes,
                fieldExprs,
                addError,
                newColPath)

              toNamedStruct(colType, updatedFieldExprs)

            case otherType =>
              addError(
                "Updating nested fields is only supported for StructType but " +
                s"'${newColPath.quoted}' is of type $otherType")
              col
          }

        // if there are conflicting updates, throw an exception
        // there are two illegal scenarios:
        // - multiple updates to the same column
        // - updates to a top-level struct and its nested fields (like 'a.b' and 'a.b.c')
        case matchedUpdates if hasExactMatch(matchedUpdates, col) =>
          val conflictingColNamesStr = matchedUpdates
            .map(update => colPath ++ update.ref)
            .map(conflictingColPath => s"'${conflictingColPath.quoted}'")
            .distinct
            .mkString(", ")
          addError("Update conflicts for columns: " + conflictingColNamesStr)
          col
      }
    }
  }

  private def toNamedStruct(structType: StructType, fieldExprs: Seq[Expression]): Expression = {
    val namedStructExprs = structType.fields.zip(fieldExprs).flatMap { case (field, expr) =>
      Seq(Literal(field.name), expr)
    }
    CreateNamedStruct(namedStructExprs)
  }

  private def hasExactMatch(updates: Seq[ColumnUpdate], col: NamedExpression): Boolean = {
    updates.exists(isExactMatch(_, col))
  }

  private def isExactMatch(update: ColumnUpdate, col: NamedExpression): Boolean = {
    update.ref match {
      case Seq(namePart) if conf.resolver(namePart, col.name) => true
      case _ => false
    }
  }

  /**
   * Checks whether assignments are aligned and are compatible with table columns.
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
      val key = assignment.key
      val value = assignment.value

      val attrType = CharVarcharUtils.getRawType(attr.metadata).getOrElse(attr.dataType)

      sameRef(toRef(key), toRef(attr)) &&
        DataType.equalsIgnoreCompatibleNullability(value.dataType, attrType) &&
        (attr.nullable || !value.nullable)
    }
  }

  private def sameRef(ref: Seq[String], otherRef: Seq[String]): Boolean = {
    ref.size == otherRef.size && ref.zip(otherRef).forall { case (namePart, otherNamePart) =>
      conf.resolver(namePart, otherNamePart)
    }
  }

  private def toRef(expr: Expression): Seq[String] = expr match {
    case attr: AttributeReference =>
      Seq(attr.name)
    case GetStructField(child, _, Some(name)) =>
      toRef(child) :+ name
    case arrayStructFields: GetArrayStructFields =>
      throw new AnalysisException(s"Cannot update nested fields inside arrays: $arrayStructFields")
    case mapValue: GetMapValue =>
      throw new AnalysisException(s"Cannot update map values: $mapValue")
    case arrItem: GetArrayItem =>
      throw new AnalysisException(s"Cannot update array items: $arrItem")
    case other =>
      throw new AnalysisException(s"Cannot convert to a reference, unsupported expression: $other")
  }
}
