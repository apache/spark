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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, ExprId, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.CombineUnions
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.SchemaUtils

/**
 * Resolves different children of Union to a common set of columns.
 */
object ResolveUnion extends Rule[LogicalPlan] {

  def makeUnionOutput(children: Seq[LogicalPlan]): Seq[Attribute] = {
    val seenExprIdSet = mutable.Map[ExprId, ExprId]()
    children.map(_.output).transpose.map { attrs =>
      val firstAttr = attrs.head
      val nullable = attrs.exists(_.nullable)
      val newDt = attrs.map(_.dataType).reduce(StructType.merge)
      // If child's output has attributes having the same `exprId`, we needs to
      // assign a unique `exprId` for them.
      val newExprId = seenExprIdSet.getOrElseUpdate(firstAttr.exprId, NamedExpression.newExprId)
      if (firstAttr.dataType == newDt) {
        firstAttr.withExprId(newExprId).withNullability(nullable)
      } else {
        AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
          newExprId, firstAttr.qualifier)
      }
    }
  }

  private def unionTwoSides(
      left: LogicalPlan,
      right: LogicalPlan,
      allowMissingCol: Boolean): LogicalPlan = {
    val resolver = SQLConf.get.resolver
    val leftOutputAttrs = left.output
    val rightOutputAttrs = right.output

    // Builds a project list for `right` based on `left` output names
    val rightProjectList = leftOutputAttrs.map { lattr =>
      rightOutputAttrs.find { rattr => resolver(lattr.name, rattr.name) }.getOrElse {
        if (allowMissingCol) {
          Alias(Literal(null, lattr.dataType), lattr.name)()
        } else {
          throw new AnalysisException(
            s"""Cannot resolve column name "${lattr.name}" among """ +
              s"""(${rightOutputAttrs.map(_.name).mkString(", ")})""")
        }
      }
    }

    // Delegates failure checks to `CheckAnalysis`
    val notFoundAttrs = rightOutputAttrs.diff(rightProjectList)
    val rightChild = Project(rightProjectList ++ notFoundAttrs, right)

    // Builds a project for `logicalPlan` based on `right` output names, if allowing
    // missing columns.
    val leftChild = if (allowMissingCol) {
      val missingAttrs = notFoundAttrs.map { attr =>
        Alias(Literal(null, attr.dataType), attr.name)()
      }
      if (missingAttrs.nonEmpty) {
        Project(leftOutputAttrs ++ missingAttrs, left)
      } else {
        left
      }
    } else {
      left
    }
    val newUnion = Union(leftChild, rightChild)
    if (newUnion.allChildrenCompatible) {
      val unionOutput = makeUnionOutput(Seq(leftChild, rightChild))
      newUnion.copy(unionOutput = Some(unionOutput))
    } else {
      newUnion
    }
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
    case p if !p.childrenResolved => p

    case Union(children, byName, allowMissingCol, _) if byName =>
      val union = children.reduceLeft { (left, right) =>
        checkColumnNames(left, right)
        unionTwoSides(left, right, allowMissingCol)
      }
      CombineUnions(union)

    case u @ Union(children, _, _, unionOutput)
        if u.allChildrenCompatible && unionOutput.isEmpty =>
      u.copy(unionOutput = Some(makeUnionOutput(children)))
  }
}
