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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{CombineUnions, OptimizeUpdateFields}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.unsafe.types.UTF8String

/**
 * Resolves different children of Union to a common set of columns.
 */
object ResolveUnion extends Rule[LogicalPlan] {
  /**
   * This method sorts columns recursively in a struct expression based on column names.
   */
  private def sortStructFields(expr: Expression): Expression = {
    val existingExprs = expr.dataType.asInstanceOf[StructType].fieldNames.zipWithIndex.map {
      case (name, i) =>
        val fieldExpr = GetStructField(KnownNotNull(expr), i)
        if (fieldExpr.dataType.isInstanceOf[StructType]) {
          (name, sortStructFields(fieldExpr))
        } else {
          (name, fieldExpr)
        }
    }.sortBy(_._1).flatMap(pair => Seq(Literal(pair._1), pair._2))

    val newExpr = CreateNamedStruct(existingExprs)
    if (expr.nullable) {
      If(IsNull(expr), Literal(null, newExpr.dataType), newExpr)
    } else {
      newExpr
    }
  }

  /**
   * Assumes input expressions are field expression of `CreateNamedStruct`. This method
   * sorts the expressions based on field names.
   */
  private def sortFieldExprs(fieldExprs: Seq[Expression]): Seq[Expression] = {
    fieldExprs.grouped(2).map { e =>
      Seq(e.head, e.last)
    }.toSeq.sortBy { pair =>
      assert(pair.head.isInstanceOf[Literal])
      pair.head.eval().asInstanceOf[UTF8String].toString
    }.flatten
  }

  /**
   * This helper method sorts fields in a `UpdateFields` expression by field name.
   */
  private def sortStructFieldsInWithFields(expr: Expression): Expression = expr transformUp {
    case u: UpdateFields if u.resolved =>
      u.evalExpr match {
        case i @ If(IsNull(_), _, CreateNamedStruct(fieldExprs)) =>
          val sorted = sortFieldExprs(fieldExprs)
          val newStruct = CreateNamedStruct(sorted)
          i.copy(trueValue = Literal(null, newStruct.dataType), falseValue = newStruct)
        case CreateNamedStruct(fieldExprs) =>
          val sorted = sortFieldExprs(fieldExprs)
          val newStruct = CreateNamedStruct(sorted)
          newStruct
        case other =>
          throw new IllegalStateException(s"`UpdateFields` has incorrect expression: $other. " +
            "Please file a bug report with this error message, stack trace, and the query.")
      }
  }

  /**
   * Adds missing fields recursively into given `col` expression, based on the target `StructType`.
   * This is called by `compareAndAddFields` when we find two struct columns with same name but
   * different nested fields. This method will find out the missing nested fields from `col` to
   * `target` struct and add these missing nested fields. Currently we don't support finding out
   * missing nested fields of struct nested in array or struct nested in map.
   */
  private def addFields(col: NamedExpression, target: StructType): Expression = {
    assert(col.dataType.isInstanceOf[StructType], "Only support StructType.")

    val resolver = SQLConf.get.resolver
    val missingFieldsOpt =
      StructType.findMissingFields(col.dataType.asInstanceOf[StructType], target, resolver)

    // We need to sort columns in result, because we might add another column in other side.
    // E.g., we want to union two structs "a int, b long" and "a int, c string".
    // If we don't sort, we will have "a int, b long, c string" and
    // "a int, c string, b long", which are not compatible.
    if (missingFieldsOpt.isEmpty) {
      sortStructFields(col)
    } else {
      missingFieldsOpt.map { s =>
        val struct = addFieldsInto(col, s.fields)
        // Combines `WithFields`s to reduce expression tree.
        val reducedStruct = struct.transformUp(OptimizeUpdateFields.optimizeUpdateFields)
        val sorted = sortStructFieldsInWithFields(reducedStruct)
        sorted
      }.get
    }
  }

  /**
   * Adds missing fields recursively into given `col` expression. The missing fields are given
   * in `fields`. For example, given `col` as "z struct<z:int, y:int>, x int", and `fields` is
   * "z struct<w:long>, w string". This method will add a nested `z.w` field and a top-level
   * `w` field to `col` and fill null values for them. Note that because we might also add missing
   * fields at other side of Union, we must make sure corresponding attributes at two sides have
   * same field order in structs, so when we adding missing fields, we will sort the fields based on
   * field names. So the data type of returned expression will be
   * "w string, x int, z struct<w:long, y:int, z:int>".
   */
  private def addFieldsInto(
      col: Expression,
      fields: Seq[StructField]): Expression = {
    fields.foldLeft(col) { case (currCol, field) =>
      field.dataType match {
        case st: StructType =>
          val resolver = SQLConf.get.resolver
          val colField = currCol.dataType.asInstanceOf[StructType]
            .find(f => resolver(f.name, field.name))
          if (colField.isEmpty) {
            // The whole struct is missing. Add a null.
            UpdateFields(currCol, field.name, Literal(null, st))
          } else {
            UpdateFields(currCol, field.name,
              addFieldsInto(ExtractValue(currCol, Literal(field.name), resolver), st.fields))
          }
        case dt =>
          UpdateFields(currCol, field.name, Literal(null, dt))
      }
    }
  }

  /**
   * This method will compare right to left plan's outputs. If there is one struct attribute
   * at right side has same name with left side struct attribute, but two structs are not the
   * same data type, i.e., some missing (nested) fields at right struct attribute, then this
   * method will try to add missing (nested) fields into the right attribute with null values.
   */
  private def compareAndAddFields(
      left: LogicalPlan,
      right: LogicalPlan,
      allowMissingCol: Boolean): (Seq[NamedExpression], Seq[NamedExpression]) = {
    val resolver = SQLConf.get.resolver
    val leftOutputAttrs = left.output
    val rightOutputAttrs = right.output

    val aliased = mutable.ArrayBuffer.empty[Attribute]

    val rightProjectList = leftOutputAttrs.map { lattr =>
      val found = rightOutputAttrs.find { rattr => resolver(lattr.name, rattr.name) }
      if (found.isDefined) {
        val foundAttr = found.get
        val foundDt = foundAttr.dataType
        (foundDt, lattr.dataType) match {
          case (source: StructType, target: StructType)
              if allowMissingCol && !source.sameType(target) =>
            // Having an output with same name, but different struct type.
            // We need to add missing fields. Note that if there are deeply nested structs such as
            // nested struct of array in struct, we don't support to add missing deeply nested field
            // like that. We will sort columns in the struct expression to make sure two sides of
            // union have consistent schema.
            aliased += foundAttr
            Alias(addFields(foundAttr, target), foundAttr.name)()
          case _ =>
            // We don't need/try to add missing fields if:
            // 1. The attributes of left and right side are the same struct type
            // 2. The attributes are not struct types. They might be primitive types, or array, map
            //    types. We don't support adding missing fields of nested structs in array or map
            //    types now.
            // 3. `allowMissingCol` is disabled.
            foundAttr
        }
      } else {
        if (allowMissingCol) {
          Alias(Literal(null, lattr.dataType), lattr.name)()
        } else {
          throw new AnalysisException(
            s"""Cannot resolve column name "${lattr.name}" among """ +
              s"""(${rightOutputAttrs.map(_.name).mkString(", ")})""")
        }
      }
    }

    (rightProjectList, aliased.toSeq)
  }

  private def unionTwoSides(
      left: LogicalPlan,
      right: LogicalPlan,
      allowMissingCol: Boolean): LogicalPlan = {
    val rightOutputAttrs = right.output

    // Builds a project list for `right` based on `left` output names
    val (rightProjectList, aliased) = compareAndAddFields(left, right, allowMissingCol)

    // Delegates failure checks to `CheckAnalysis`
    val notFoundAttrs = rightOutputAttrs.diff(rightProjectList ++ aliased)
    val rightChild = Project(rightProjectList ++ notFoundAttrs, right)

    // Builds a project for `logicalPlan` based on `right` output names, if allowing
    // missing columns.
    val leftChild = if (allowMissingCol) {
      // Add missing (nested) fields to left plan.
      val (leftProjectList, _) = compareAndAddFields(rightChild, left, allowMissingCol)
      if (leftProjectList.map(_.toAttribute) != left.output) {
        Project(leftProjectList, left)
      } else {
        left
      }
    } else {
      left
    }
    Union(leftChild, rightChild)
  }

  // Check column name duplication
  private def checkColumnNames(left: LogicalPlan, right: LogicalPlan): Unit = {
    val caseSensitiveAnalysis = SQLConf.get.caseSensitiveAnalysis
    val leftOutputAttrs = left.output
    val rightOutputAttrs = right.output

    SchemaUtils.checkColumnNameDuplication(
      leftOutputAttrs.map(_.name),
      "in the left attributes",
      caseSensitiveAnalysis)
    SchemaUtils.checkColumnNameDuplication(
      rightOutputAttrs.map(_.name),
      "in the right attributes",
      caseSensitiveAnalysis)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    case e if !e.childrenResolved => e

    case Union(children, byName, allowMissingCol) if byName =>
      val union = children.reduceLeft { (left, right) =>
        checkColumnNames(left, right)
        unionTwoSides(left, right, allowMissingCol)
      }
      CombineUnions(union)
  }
}
