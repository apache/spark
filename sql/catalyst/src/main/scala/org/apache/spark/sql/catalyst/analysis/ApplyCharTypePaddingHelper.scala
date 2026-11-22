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
  ExprUtils,
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

  /**
   * A scalar subquery of one CHAR column, with the metadata of that column. Only a CHAR column
   * itself: padding a struct or an array would read the subquery once for each of its fields.
   */
  object CharScalarSubquery {
    def unapply(e: Expression): Option[Metadata] = e match {
      case s: ScalarSubquery if s.resolved && s.plan.output.length == 1 &&
          isCharType(s.plan.output.head.metadata) =>
        Some(s.plan.output.head.metadata)
      case _ => None
    }
  }

  private def isCharType(metadata: Metadata): Boolean =
    CharVarcharUtils.getRawType(metadata).exists(_.isInstanceOf[CharType])

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
        padAttrLitCmp(e, attr.metadata, padCharCol, lit, lit.eval())
          .map { newChildren =>
            b.withNewChildren(newChildren)
          }
          .getOrElse(b)

      case b @ BinaryComparison(lit, e @ AttrOrOuterRef(attr)) if lit.foldable =>
        padAttrLitCmp(e, attr.metadata, padCharCol, lit, lit.eval())
          .map { newChildren =>
            b.withNewChildren(newChildren.reverse)
          }
          .getOrElse(b)

      case i @ In(e @ AttrOrOuterRef(attr), list)
          if i.resolved && attr.dataType.isInstanceOf[StringType] && list.forall(_.foldable) =>
        CharVarcharUtils.getRawType(attr.metadata).collect {
          case c: CharType => padInList(i, c, list.map(_.eval()), padCharCol)
        }.getOrElse(i)

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
      // Unlike the cases above, these don't truncate values longer than their CHAR length.
      // A comparison of types that don't match is left to CheckAnalysis.
      // As in ConstantFolding, a constant that fails to evaluate, such as one in a branch that is
      // never evaluated, is left to the evaluation of the query.
      case b @ BinaryComparison(e @ CharScalarSubquery(metadata), lit)
          if b.resolved && lit.foldable =>
        ExprUtils.tryEvalFoldable(lit)
          .flatMap(padAttrLitCmp(e, metadata, padCharCol, lit, _, truncate = false))
          .map(b.withNewChildren)
          .getOrElse(b)

      case b @ BinaryComparison(lit, e @ CharScalarSubquery(metadata))
          if b.resolved && lit.foldable =>
        ExprUtils.tryEvalFoldable(lit)
          .flatMap(padAttrLitCmp(e, metadata, padCharCol, lit, _, truncate = false))
          .map(newChildren => b.withNewChildren(newChildren.reverse))
          .getOrElse(b)

      case b @ BinaryComparison(e1 @ CharOperand(m1), e2 @ CharOperand(m2))
          if b.resolved && (e1.isInstanceOf[ScalarSubquery] || e2.isInstanceOf[ScalarSubquery]) =>
        b.withNewChildren(CharVarcharUtils.addPaddingInStringComparison(
          Seq(e1, e2), Seq(m1, m2).map(CharVarcharUtils.getRawType), padCharCol, truncate = false))

      case i @ In(CharScalarSubquery(metadata), list) if i.resolved && list.forall(_.foldable) =>
        val values = list.map(ExprUtils.tryEvalFoldable)
        CharVarcharUtils.getRawType(metadata).collect {
          case c: CharType if values.forall(_.isDefined) =>
            padInList(i, c, values.map(_.get), padCharCol, truncate = false)
        }.getOrElse(i)

      case i: InSubquery if i.resolved => padInSubquery(i, padCharCol)

      case other => other
    }

  /**
   * Pads each CHAR value or string literal of an IN subquery and the CHAR column of the subquery
   * it is compared with, as a comparison with a column pads them, but without truncating values
   * longer than their CHAR length, as read-side padding keeps them. The columns are padded in a
   * Project over the subquery, without the raw type, so that they are padded only once.
   */
  private def padInSubquery(i: InSubquery, padCharCol: Boolean): InSubquery = {
    val padded = i.values.zip(i.query.childOutputs).map {
      case (value @ CharOperand(metadata), col) =>
        val Seq(newValue, newCol) = CharVarcharUtils.addPaddingInStringComparison(
          Seq(value, col),
          Seq(CharVarcharUtils.getRawType(metadata), CharVarcharUtils.getRawType(col.metadata)),
          padCharCol,
          truncate = false)
        (newValue, Some(newCol).filterNot(_ eq col))
      case (lit, col) if lit.dataType.isInstanceOf[StringType] && lit.foldable =>
        ExprUtils.tryEvalFoldable(lit)
          .flatMap(padAttrLitCmp(col, col.metadata, padCharCol, lit, _, truncate = false))
          .map { case Seq(newCol, newLit) => (newLit, Some(newCol).filterNot(_ eq col)) }
          .getOrElse((lit, None))
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

  /**
   * Pads a CHAR value compared with a list of constants with the given values, and the constants,
   * to the longest of their lengths. The length of every list element is computed in place, so
   * that each length stays aligned with the element it came from. NULL elements have no length:
   * they can never match, so they are left untouched instead of being padded. With `truncate =
   * false`, the value is padded like read-side padding, which keeps longer values.
   */
  private def padInList(
      i: In,
      c: CharType,
      values: Seq[Any],
      padCharCol: Boolean,
      truncate: Boolean = true): In = {
    val literalCharLengths = values.map(v => Option(v.asInstanceOf[UTF8String]).map(_.numChars()))
    val targetLen = (c.length +: literalCharLengths.flatten).max
    val newValue = if (truncate) {
      addPadding(i.value, c.length, targetLen, alwaysPad = padCharCol)
    } else if (targetLen > c.length || padCharCol) {
      CharVarcharUtils.createReadSidePadding(i.value, CharType(targetLen, c.collation))
    } else {
      i.value
    }
    i.copy(
      value = newValue,
      list = i.list.zip(literalCharLengths).map {
        case (lit, Some(charLength)) => addPadding(lit, charLength, targetLen, alwaysPad = false)
        case (lit, None) => lit
      })
  }

  // `value` is the value of the constant `lit`. It is only evaluated for a CHAR value: a constant
  // compared with another type can be one that only an optimizer rule can evaluate, such as a cast
  // of a special datetime string.
  private def padAttrLitCmp(
      expr: Expression,
      metadata: Metadata,
      padCharCol: Boolean,
      lit: Expression,
      value: => Any,
      truncate: Boolean = true): Option[Seq[Expression]] = {
    if (expr.dataType.isInstanceOf[StringType]) {
      CharVarcharUtils.getRawType(metadata).flatMap {
        case c: CharType =>
          // The literal is never longer than the length it is padded to.
          def padExpr(length: Int): Expression = if (truncate) {
            createStringRPad(expr, Literal(length))
          } else {
            CharVarcharUtils.createReadSidePadding(expr, CharType(length, c.collation))
          }
          val str = value.asInstanceOf[UTF8String]
          if (str == null) {
            None
          } else {
            val stringLitLen = str.numChars()
            if (c.length < stringLitLen) {
              Some(Seq(padExpr(stringLitLen), lit))
            } else if (c.length > stringLitLen) {
              val paddedExpr = if (padCharCol) padExpr(c.length) else expr
              Some(Seq(paddedExpr, createStringRPad(lit, Literal(c.length))))
            } else if (padCharCol) {
              Some(Seq(padExpr(c.length), lit))
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
