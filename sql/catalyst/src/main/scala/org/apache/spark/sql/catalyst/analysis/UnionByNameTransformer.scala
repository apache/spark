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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils

/**
 * Object used to align the children of a by-name [[Union]] by column name. For example, for
 * `df1.unionByName(df2, allowMissingColumns = true)` where `df1` has columns (a, b) and `df2` has
 * columns (b, c), the aligned children are:
 *
 * {{{
 * Project [a#0, b#1, null AS c#5]
 * +- LocalRelation [a#0, b#1]
 *
 * Project [null AS a#4, b#2, c#3]
 * +- LocalRelation [b#2, c#3]
 * }}}
 */
object UnionByNameTransformer extends SQLConfHelper {

  /**
   * Projects the children of a by-name union, whose outputs are `leftOutput` and `rightOutput`, so
   * that their columns line up by name. The right child is always projected; the left child only
   * when `allowMissingCol` adds columns or nested fields to it. `newAlias` builds new columns.
   *
   * The alignment is done in the following steps:
   *  1. Check that neither side has duplicate column names.
   *  2. Build a project list for `right` based on `left` output names.
   *  3. Append the `right` attributes that are not in that project list, delegating failure checks
   *     to `CheckAnalysis`.
   *  4. If `allowMissingCol` is true, build a project list for `left` based on the projected
   *     `right` output names, adding the missing (nested) fields to `left`.
   */
  def alignChildrenByName(
      left: LogicalPlan,
      leftOutput: Seq[Attribute],
      right: LogicalPlan,
      rightOutput: Seq[Attribute],
      allowMissingCol: Boolean,
      newAlias: (Expression, String) => Alias): AlignedUnionChildren = {
    checkColumnNames(leftOutput, rightOutput)

    val (rightProjectList, aliased) =
      compareAndAddFields(leftOutput, rightOutput, allowMissingCol, newAlias)

    val notFoundAttrs = rightOutput.diff(rightProjectList ++ aliased)
    val rightChildProjectList = rightProjectList ++ notFoundAttrs
    val rightChild = Project(rightChildProjectList, right)
    val rightChildOutput = rightChildProjectList.map(_.toAttribute)

    val (leftChild, leftChildOutput) = if (allowMissingCol) {
      val (leftProjectList, _) =
        compareAndAddFields(rightChildOutput, leftOutput, allowMissingCol, newAlias)
      val leftProjectOutput = leftProjectList.map(_.toAttribute)
      if (leftProjectOutput != leftOutput) {
        (Project(leftProjectList, left), leftProjectOutput)
      } else {
        (left, leftOutput)
      }
    } else {
      (left, leftOutput)
    }

    AlignedUnionChildren(
      left = leftChild,
      leftOutput = leftChildOutput,
      right = rightChild,
      rightOutput = rightChildOutput)
  }

  /**
   * Checks for column name duplication in `leftOutputAttrs` and in `rightOutputAttrs`.
   */
  private def checkColumnNames(
      leftOutputAttrs: Seq[Attribute],
      rightOutputAttrs: Seq[Attribute]): Unit = {
    val caseSensitiveAnalysis = conf.caseSensitiveAnalysis

    SchemaUtils.checkColumnNameDuplication(
      leftOutputAttrs.map(_.name),
      caseSensitiveAnalysis)
    SchemaUtils.checkColumnNameDuplication(
      rightOutputAttrs.map(_.name),
      caseSensitiveAnalysis)
  }

  /**
   * This method will compare right to left plan's outputs. If there is one struct attribute
   * at right side has same name with left side struct attribute, but two structs are not the
   * same data type, i.e., some missing (nested) fields at right struct attribute, then this
   * method will try to add missing (nested) fields into the right attribute with null values.
   *
   * For each left attribute, the right attribute with the same name is:
   *  - kept as is, if both sides are primitive types or equivalent complex types.
   *  - aliased after merging its (nested) fields with the left type, if the two types are complex
   *    and have different nested structs at some level. Map types are currently not supported, so
   *    for them the alias wraps the existing attribute.
   * If there is no right attribute with the same name, a null column is added when
   * `allowMissingCol` is true, and an exception is thrown otherwise.
   */
  private def compareAndAddFields(
      leftOutputAttrs: Seq[Attribute],
      rightOutputAttrs: Seq[Attribute],
      allowMissingCol: Boolean,
      newAlias: (Expression, String) => Alias): (Seq[NamedExpression], Seq[NamedExpression]) = {
    val resolver = conf.resolver

    val aliased = mutable.ArrayBuffer.empty[Attribute]

    val rightProjectList = leftOutputAttrs.map { lattr =>
      val found = rightOutputAttrs.find { rattr => resolver(lattr.name, rattr.name) }
      if (found.isDefined) {
        val foundAttr = found.get
        val foundDt = foundAttr.dataType
        if (!DataType.equalsStructurallyByName(foundDt, lattr.dataType, resolver)) {
          aliased += foundAttr
          newAlias(
            mergeFields(foundAttr, lattr.dataType, allowMissingCol, lambdaDepth = 0),
            foundAttr.name)
        } else {
          foundAttr
        }
      } else {
        if (allowMissingCol) {
          newAlias(Literal(null, lattr.dataType), lattr.name)
        } else {
          throw QueryCompilationErrors.cannotResolveColumnNameAmongAttributesError(
            lattr.name, rightOutputAttrs.map(_.name).mkString(", "))
        }
      }
    }

    (rightProjectList, aliased.toSeq)
  }

  /**
   * Handles the merging of complex types. Currently supports structs and arrays recursively. For
   * any other combination of types, `col` is returned as is and the check is left to the analysis
   * of the resulting union.
   */
  private def mergeFields(col: Expression, targetType: DataType,
      allowMissing: Boolean, lambdaDepth: Int): Expression = {
    if (!DataType.equalsStructurallyByName(col.dataType, targetType, conf.resolver)) {
      (col.dataType, targetType) match {
        case (_: StructType, targetStruct: StructType) =>
          addFields(col, targetStruct, allowMissing, lambdaDepth)
        case (_: ArrayType, targetArray: ArrayType) =>
          transformArray(col, targetArray, allowMissing, lambdaDepth)
        case _ =>
          col
      }
    } else {
      col
    }
  }

  /**
   * Adds missing fields recursively into given `col` expression, based on the expected struct
   * fields from merging the two schemas. This is called by `compareAndAddFields` when we find two
   * struct columns with same name but different nested fields. This method will recursively
   * return a new struct with all of the expected fields, adding null values when `col` doesn't
   * already contain them. Currently we don't support merging structs nested inside of maps.
   * Null values are only added if `allowMissing` is true; otherwise, a field missing from `col`
   * throws an exception.
   */
  private def addFields(col: Expression,
     targetType: StructType, allowMissing: Boolean, lambdaDepth: Int): Expression = {
    assert(col.dataType.isInstanceOf[StructType], "Only support StructType.")

    val resolver = conf.resolver
    val colType = col.dataType.asInstanceOf[StructType]

    val newStructFields = mutable.ArrayBuffer.empty[Expression]

    targetType.fields.foreach { expectedField =>
      val currentField = colType.fields.find(f => resolver(f.name, expectedField.name))

      val newExpression = (currentField, expectedField.dataType) match {
        case (Some(cf), expectedType) =>
          mergeFields(
            ExtractValue(col, Literal(cf.name), resolver), expectedType, allowMissing, lambdaDepth)
        case (None, expectedType) =>
          if (allowMissing) {
            Literal(null, expectedType)
          } else {
            throw QueryCompilationErrors.noSuchStructFieldInGivenFieldsError(
              expectedField.name, colType.fields)
          }
      }
      newStructFields ++= Literal(expectedField.name) :: newExpression :: Nil
    }

    colType.fields
      .filter(f => !targetType.fields.exists(tf => resolver(f.name, tf.name)))
      .foreach { f =>
        newStructFields ++= Literal(f.name) :: ExtractValue(col, Literal(f.name), resolver) :: Nil
      }

    val newStruct = CreateNamedStruct(newStructFields.toSeq)
    if (col.nullable) {
      If(IsNull(col), Literal(null, newStruct.dataType), newStruct)
    } else {
      newStruct
    }
  }

  /**
   * Transform the array of structs to the target struct type. The lambda variable is named after
   * its nesting depth `lambdaDepth`, so that resolving the same plan always gives the same names.
   */
  private def transformArray(arrayCol: Expression, targetType: ArrayType,
      allowMissing: Boolean, lambdaDepth: Int) = {
    assert(arrayCol.dataType.isInstanceOf[ArrayType], "Only support ArrayType.")

    val arrayType = arrayCol.dataType.asInstanceOf[ArrayType]

    val x = NamedLambdaVariable(s"x_$lambdaDepth",
      arrayType.elementType,
      arrayType.containsNull)
    val function = mergeFields(x, targetType.elementType, allowMissing, lambdaDepth + 1)
    ArrayTransform(arrayCol, LambdaFunction(function, Seq(x)))
  }
}

/**
 * The children of a by-name union after [[UnionByNameTransformer.alignChildrenByName]], with
 * their outputs.
 */
case class AlignedUnionChildren(
    left: LogicalPlan,
    leftOutput: Seq[Attribute],
    right: LogicalPlan,
    rightOutput: Seq[Attribute])
