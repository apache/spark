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

import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  BinaryComparison,
  Expression,
  In,
  InSubquery,
  Literal,
  NamedExpression,
  OuterReference,
  ScalarSubquery
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreePattern.{BINARY_COMPARISON, IN, IN_SUBQUERY}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.catalyst.util.CharVarcharUtils.createStringRPad
import org.apache.spark.sql.types.{CharType, Metadata, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Helper object used by the [[ApplyCharTypePadding]] rule. This object is under catalyst
 * package in order to make the methods accessible to single-pass [[Resolver]].
 */
object ApplyCharTypePaddingHelper {

  object AttrOrOuterRef {
    def unapply(e: Expression): Option[Attribute] = e match {
      case a: Attribute => Some(a)
      case OuterReference(a: Attribute) => Some(a)
      case _ => None
    }
  }

  /** A scalar subquery of one CHAR column, with the metadata of that column. */
  object CharScalarSubquery {
    def unapply(e: Expression): Option[Metadata] = e match {
      case s: ScalarSubquery if s.resolved && s.plan.output.length == 1 =>
        Some(s.plan.output.head.metadata)
      case _ => None
    }
  }

  /** An operand of a comparison with a raw type: a column or a scalar subquery of one. */
  object CharOperand {
    def unapply(e: Expression): Option[Metadata] = e match {
      case AttrOrOuterRef(attr) => Some(attr.metadata)
      case CharScalarSubquery(metadata) => Some(metadata)
      case _ => None
    }
  }

  private[sql] def readSidePadding(
      relation: LogicalPlan,
      cleanedRelation: () => LogicalPlan): (LogicalPlan, Seq[(Attribute, Attribute)]) = {
    val projectList = relation.output.map { attr =>
      CharVarcharUtils.addPaddingForScan(attr) match {
        case ne: NamedExpression => ne
        case other => Alias(other, attr.name)(explicitMetadata = Some(attr.metadata))
      }
    }
    if (projectList == relation.output) {
      relation -> Nil
    } else {
      val newPlan = Project(projectList, cleanedRelation())
      newPlan -> relation.output.zip(newPlan.output)
    }
  }

  private[sql] def paddingForStringComparison(
      plan: LogicalPlan,
      padCharCol: Boolean): LogicalPlan = {
    plan.resolveOperatorsUpWithSubqueriesAndPruning(
        _.containsAnyPattern(BINARY_COMPARISON, IN, IN_SUBQUERY)) {
      case operator =>
        operator.transformExpressionsUpWithPruning(
            _.containsAnyPattern(BINARY_COMPARISON, IN, IN_SUBQUERY)) {
          case e if !e.childrenResolved => e
          case withChildrenResolved =>
            singleNodePaddingForStringComparison(withChildrenResolved, padCharCol)
        }
    }
  }

  private[sql] def singleNodePaddingForStringComparison(
      expression: Expression,
      padCharCol: Boolean): Expression =
    expression match {
      // String literal is treated as char type when it's compared to a char type column.
      // We should pad the shorter one to the longer length.
      case b @ BinaryComparison(e @ AttrOrOuterRef(attr), lit) if lit.foldable =>
        padAttrLitCmp(e, attr.metadata, padCharCol, lit)
          .map { newChildren =>
            b.withNewChildren(newChildren)
          }
          .getOrElse(b)

      case b @ BinaryComparison(lit, e @ AttrOrOuterRef(attr)) if lit.foldable =>
        padAttrLitCmp(e, attr.metadata, padCharCol, lit)
          .map { newChildren =>
            b.withNewChildren(newChildren.reverse)
          }
          .getOrElse(b)

      case i @ In(e @ AttrOrOuterRef(attr), list)
          if i.resolved && attr.dataType.isInstanceOf[StringType] && list.forall(_.foldable) =>
        CharVarcharUtils
          .getRawType(attr.metadata)
          .flatMap {
            case c: CharType =>
              // Compute the length of every list element in place, so that each length stays
              // aligned with the element it came from. NULL elements have no length: they can
              // never match, so they are left untouched instead of being padded.
              val literalCharLengths = list.map { lit =>
                Option(lit.eval().asInstanceOf[UTF8String]).map(_.numChars())
              }
              val targetLen = (c.length +: literalCharLengths.flatten).max
              Some(
                i.copy(
                  value = addPadding(e, c.length, targetLen, alwaysPad = padCharCol),
                  list = list.zip(literalCharLengths).map {
                    case (lit, Some(charLength)) =>
                      addPadding(lit, charLength, targetLen, alwaysPad = false)
                    case (lit, None) => lit
                  }
                )
              )
            case _ => None
          }
          .getOrElse(i)

      // For char type column or inner field comparison, pad the shorter one to the longer length.
      case b @ BinaryComparison(e1 @ AttrOrOuterRef(left), e2 @ AttrOrOuterRef(right))
          // For the same attribute, they must be the same length and no padding is needed.
          if !left.semanticEquals(right) =>
        val outerRefs = (e1, e2) match {
          case (_: OuterReference, _: OuterReference) => Seq(left, right)
          case (_: OuterReference, _) => Seq(left)
          case (_, _: OuterReference) => Seq(right)
          case _ => Nil
        }
        val newChildren =
          CharVarcharUtils.addPaddingInStringComparison(Seq(left, right), padCharCol)
        if (outerRefs.nonEmpty) {
          b.withNewChildren(newChildren.map(_.transform {
            case a: Attribute if outerRefs.exists(_.semanticEquals(a)) => OuterReference(a)
          }))
        } else {
          b.withNewChildren(newChildren)
        }

      case i @ In(e @ AttrOrOuterRef(attr), list) if list.forall(_.isInstanceOf[Attribute]) =>
        val newChildren = CharVarcharUtils.addPaddingInStringComparison(
          attr +: list.map(_.asInstanceOf[Attribute]),
          padCharCol
        )
        if (e.isInstanceOf[OuterReference]) {
          i.copy(value = newChildren.head.transform {
            case a: Attribute if a.semanticEquals(attr) => OuterReference(a)
          }, list = newChildren.tail)
        } else {
          i.copy(value = newChildren.head, list = newChildren.tail)
        }

      // Like the cases above, for a scalar subquery of a CHAR column compared with a string
      // literal, a column, or another scalar subquery.
      case b @ BinaryComparison(e @ CharScalarSubquery(metadata), lit) if lit.foldable =>
        padAttrLitCmp(e, metadata, padCharCol, lit).map(b.withNewChildren).getOrElse(b)

      case b @ BinaryComparison(lit, e @ CharScalarSubquery(metadata)) if lit.foldable =>
        padAttrLitCmp(e, metadata, padCharCol, lit)
          .map(newChildren => b.withNewChildren(newChildren.reverse))
          .getOrElse(b)

      case b @ BinaryComparison(e1 @ CharOperand(m1), e2 @ CharOperand(m2))
          if e1.isInstanceOf[ScalarSubquery] || e2.isInstanceOf[ScalarSubquery] =>
        b.withNewChildren(CharVarcharUtils.addPaddingInStringComparison(
          Seq(e1, e2), Seq(m1, m2).map(CharVarcharUtils.getRawType), padCharCol))

      case i: InSubquery if i.resolved => padInSubquery(i, padCharCol)

      case other => other
    }

  /**
   * Pads each CHAR value or string literal of an IN subquery and the CHAR column of the subquery
   * it is compared with, as a comparison with a column pads them. The columns are padded in a
   * Project over the subquery, without the raw type, so that they are padded only once.
   */
  private def padInSubquery(i: InSubquery, padCharCol: Boolean): InSubquery = {
    val padded = i.values.zip(i.query.childOutputs).map {
      case (value @ CharOperand(metadata), col) =>
        val Seq(newValue, newCol) = CharVarcharUtils.addPaddingInStringComparison(
          Seq(value, col),
          Seq(CharVarcharUtils.getRawType(metadata), CharVarcharUtils.getRawType(col.metadata)),
          padCharCol)
        (newValue, Some(newCol).filterNot(_ eq col))
      case (lit: Literal, col) =>
        padAttrLitCmp(col, col.metadata, padCharCol, lit).map { case Seq(newCol, newLit) =>
          (newLit, Some(newCol).filterNot(_ eq col))
        }.getOrElse((lit, None))
      case (value, _) => (value, None)
    }
    if (padded.forall(_._2.isEmpty)) {
      i.copy(values = padded.map(_._1))
    } else {
      val plan = i.query.plan
      // The plan can have more columns than values after de-correlation.
      val projectList = plan.output.zipWithIndex.map { case (col, idx) =>
        padded.lift(idx).flatMap(_._2).map { newCol =>
          Alias(newCol, col.name)(
            qualifier = col.qualifier,
            explicitMetadata = Some(CharVarcharUtils.cleanMetadata(col.metadata)))
        }.getOrElse(col)
      }
      i.copy(values = padded.map(_._1), query = i.query.withNewPlan(Project(projectList, plan)))
    }
  }

  private def padAttrLitCmp(
      expr: Expression,
      metadata: Metadata,
      padCharCol: Boolean,
      lit: Expression): Option[Seq[Expression]] = {
    if (expr.dataType.isInstanceOf[StringType]) {
      CharVarcharUtils.getRawType(metadata).flatMap {
        case c: CharType =>
          val str = lit.eval().asInstanceOf[UTF8String]
          if (str == null) {
            None
          } else {
            val stringLitLen = str.numChars()
            if (c.length < stringLitLen) {
              Some(Seq(createStringRPad(expr, Literal(stringLitLen)), lit))
            } else if (c.length > stringLitLen) {
              val paddedExpr = if (padCharCol) {
                createStringRPad(expr, Literal(c.length))
              } else {
                expr
              }
              Some(Seq(paddedExpr, createStringRPad(lit, Literal(c.length))))
            } else if (padCharCol) {
              Some(Seq(createStringRPad(expr, Literal(c.length)), lit))
            } else {
              None
            }
          }
        case _ => None
      }
    } else {
      None
    }
  }

  private def addPadding(
      expr: Expression,
      charLength: Int,
      targetLength: Int,
      alwaysPad: Boolean): Expression = {
    if (targetLength > charLength) {
      createStringRPad(expr, Literal(targetLength))
    } else if (alwaysPad) {
      createStringRPad(expr, Literal(charLength))
    } else expr
  }
}
