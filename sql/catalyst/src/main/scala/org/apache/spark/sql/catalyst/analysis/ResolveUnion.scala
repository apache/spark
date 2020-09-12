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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, Literal, NamedExpression, WithFields}
import org.apache.spark.sql.catalyst.optimizer.CombineUnions
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils

/**
 * Resolves different children of Union to a common set of columns.
 */
object ResolveUnion extends Rule[LogicalPlan] {
  /**
   * Adds missing fields recursively into given `col` expression, based on the target `StructType`.
   * For example, given `col` as "a struct<a:int, b:int>, b int" and `target` as
   * "a struct<a:int, b:int, c:long>, b int, c string", this method should add `a.c` and `c` to
   * `col` expression.
   */
  private def addFields(col: NamedExpression, target: StructType): Option[Expression] = {
    assert(col.dataType.isInstanceOf[StructType], "Only support StructType.")

    val resolver = SQLConf.get.resolver
    val missingFields =
      StructType.findMissingFields(col.dataType.asInstanceOf[StructType], target, resolver)
    if (missingFields.isEmpty) {
      None
    } else {
      missingFields.map(s => addFieldsInto(col, "", s.fields))
    }
  }

  /**
   * Adds missing fields recursively into given `col` expression. The missing fields are given
   * in `fields`. For example, given `col` as "a struct<a:int, b:int>, b int", and `fields` is
   * "a struct<c:long>, c string". This method will add a nested `a.c` field and a top-level
   * `c` field to `col` and fill null values for them.
   */
  private def addFieldsInto(col: Expression, base: String, fields: Seq[StructField]): Expression = {
    fields.foldLeft(col) { case (currCol, field) =>
      field.dataType match {
        case st: StructType =>
          val resolver = SQLConf.get.resolver
          val colField = currCol.dataType.asInstanceOf[StructType]
            .find(f => resolver(f.name, field.name))
          if (colField.isEmpty) {
            // The whole struct is missing. Add a null.
            WithFields(currCol, s"$base${field.name}", Literal(null, st),
              sortOutputColumns = true)
          } else {
            addFieldsInto(currCol, s"$base${field.name}.", st.fields)
          }
        case dt =>
          // We need to sort columns in result, because we might add another column in other side.
          // E.g., we want to union two structs "a int, b long" and "a int, c string".
          // If we don't sort, we will have "a int, b long, c string" and "a int, c string, b long",
          // which are not compatible.
          WithFields(currCol, s"$base${field.name}", Literal(null, dt),
            sortOutputColumns = true)
      }
    }
  }

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
        val foundDt = found.get.dataType
        (foundDt, lattr.dataType) match {
          case (source: StructType, target: StructType)
              if allowMissingCol && !source.sameType(target) =>
            // Having an output with same name, but different struct type.
            // We need to add missing fields. Note that if there are deeply nested structs such as
            // nested struct of array in struct, we don't support to add missing deeply nested field
            // like that. For such case, simply use original attribute.
            addFields(found.get, target).map { added =>
              aliased += found.get
              Alias(added, found.get.name)()
            }.getOrElse(found.get)
          case _ =>
            // We don't need/try to add missing fields if:
            // 1. The attributes of left and right side are the same struct type
            // 2. The attributes are not struct types. They might be primitive types, or array, map
            //    types. We don't support adding missing fields of nested structs in array or map
            //    types now.
            // 3. `allowMissingCol` is disabled.
            found.get
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

    (rightProjectList, aliased)
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
