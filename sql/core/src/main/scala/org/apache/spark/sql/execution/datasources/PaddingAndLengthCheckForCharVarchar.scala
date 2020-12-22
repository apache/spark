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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, BinaryComparison, Expression, In, Literal, StringRPad}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{CharType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * This rule performs char type padding and length check for both char and varchar.
 *
 * When reading values from column/field of type CHAR(N) or VARCHAR(N), the underlying string value
 * might be over length (e.g. tables w/ external locations), it will fail in this case.
 * Otherwise, right-pad the values to length N for CHAR(N) and remain the same for VARCHAR(N).
 *
 * When comparing char type column/field with string literal or char type column/field,
 * right-pad the shorter one to the longer length.
 */
object PaddingAndLengthCheckForCharVarchar extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val padded = plan.resolveOperatorsUpWithNewOutput {
      case r: LogicalRelation =>
        val projectList = CharVarcharUtils.paddingWithLengthCheck(r.output)
        if (projectList == r.output) {
          r -> Nil
        } else {
          val cleanedOutput = r.output.map(CharVarcharUtils.cleanAttrMetadata)
          val padded = Project(projectList, r.copy(output = cleanedOutput))
          padded -> r.output.zip(padded.output)
        }

      case r: DataSourceV2Relation =>
        val projectList = CharVarcharUtils.paddingWithLengthCheck(r.output)
        if (projectList == r.output) {
          r -> Nil
        } else {
          val cleanedOutput = r.output.map(CharVarcharUtils.cleanAttrMetadata)
          val padded = Project(projectList, r.copy(output = cleanedOutput))
          padded -> r.output.zip(padded.output)
        }

      case r: HiveTableRelation =>
        val projectList = CharVarcharUtils.paddingWithLengthCheck(r.output)
        if (projectList == r.output) {
          r -> Nil
        } else {
          val cleanedDataCols = r.dataCols.map(CharVarcharUtils.cleanAttrMetadata)
          val cleanedPartCols = r.partitionCols.map(CharVarcharUtils.cleanAttrMetadata)
          val padded = Project(projectList,
            r.copy(dataCols = cleanedDataCols, partitionCols = cleanedPartCols))
          padded -> r.output.zip(padded.output)
        }
    }

    padded.resolveOperatorsUp {
      case operator if operator.resolved => operator.transformExpressionsUp {
        // String literal is treated as char type when it's compared to a char type column.
        // We should pad the shorter one to the longer length.
        case b @ BinaryComparison(attr: Attribute, lit) if lit.foldable =>
          padAttrLitCmp(attr, lit).map { newChildren =>
            b.withNewChildren(newChildren)
          }.getOrElse(b)

        case b @ BinaryComparison(lit, attr: Attribute) if lit.foldable =>
          padAttrLitCmp(attr, lit).map { newChildren =>
            b.withNewChildren(newChildren.reverse)
          }.getOrElse(b)

        case i @ In(attr: Attribute, list)
          if attr.dataType == StringType && list.forall(_.foldable) =>
          CharVarcharUtils.getRawType(attr.metadata).flatMap {
            case CharType(length) =>
              val literalCharLengths = list.map(_.eval().asInstanceOf[UTF8String].numChars())
              val targetLen = (length +: literalCharLengths).max
              Some(i.copy(
                value = addPadding(attr, length, targetLen),
                list = list.zip(literalCharLengths).map {
                  case (lit, charLength) => addPadding(lit, charLength, targetLen)
                }))
            case _ => None
          }.getOrElse(i)

        // For char type column or inner field comparison, pad the shorter one to the longer length.
        case b @ BinaryComparison(left: Attribute, right: Attribute) =>
          b.withNewChildren(CharVarcharUtils.addPaddingInStringComparison(Seq(left, right)))

        case i @ In(attr: Attribute, list) if list.forall(_.isInstanceOf[Attribute]) =>
          val newChildren = CharVarcharUtils.addPaddingInStringComparison(
            attr +: list.map(_.asInstanceOf[Attribute]))
          i.copy(value = newChildren.head, list = newChildren.tail)
      }
    }
  }

  private def padAttrLitCmp(attr: Attribute, lit: Expression): Option[Seq[Expression]] = {
    if (attr.dataType == StringType) {
      CharVarcharUtils.getRawType(attr.metadata).flatMap {
        case CharType(length) =>
          val str = lit.eval().asInstanceOf[UTF8String]
          val stringLitLen = str.numChars()
          if (length < stringLitLen) {
            Some(Seq(StringRPad(attr, Literal(stringLitLen)), lit))
          } else if (length > stringLitLen) {
            Some(Seq(attr, StringRPad(lit, Literal(length))))
          } else {
            None
          }
        case _ => None
      }
    } else {
      None
    }
  }

  private def addPadding(expr: Expression, charLength: Int, targetLength: Int): Expression = {
    if (targetLength > charLength) StringRPad(expr, Literal(targetLength)) else expr
  }
}
