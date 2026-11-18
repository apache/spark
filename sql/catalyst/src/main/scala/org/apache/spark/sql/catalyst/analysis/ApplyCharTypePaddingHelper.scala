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
  Literal,
  NamedExpression,
  Or,
  OuterReference
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreePattern.{BINARY_COMPARISON, IN}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.catalyst.util.CharVarcharUtils.createStringRPad
import org.apache.spark.sql.types.{CharType, DataType, Metadata, StringType, VarcharType}
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
    plan.resolveOperatorsUpWithSubqueriesAndPruning(_.containsAnyPattern(BINARY_COMPARISON, IN)) {
      case operator =>
        operator.transformExpressionsUpWithPruning(_.containsAnyPattern(BINARY_COMPARISON, IN)) {
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

      // `value IN (e1, e2, ...)` returns what `value = e1 OR value = e2 OR ...` returns, and each
      // of these comparisons pads its sides on its own. The two `In` cases below pad the value
      // and all the elements to a common length, which is only right if all the elements are
      // padded the same way. If the list mixes elements that are padded in different ways, e.g. a
      // CHAR column and a STRING column that is not padded at all, split the IN into one IN per
      // group of elements that are padded the same way, so that each element is compared with
      // the value padded for it.
      case i @ InWithMixedPadding(groups) =>
        val newIns = groups.map { group =>
          singleNodePaddingForStringComparison(i.copy(list = group), padCharCol)
        }
        // Keep the IN as it is if no element is padded.
        val padded = newIns.zip(groups).exists { case (newIn, group) =>
          !newIn.fastEquals(i.copy(list = group))
        }
        if (padded) newIns.reduceLeft[Expression](Or(_, _)) else i

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

      case other => other
    }

  // How an element of `value IN (list)` is padded in its comparison with the value.
  private sealed trait InElementPadding
  private case object NotPadded extends InElementPadding
  // A string literal compared with a CHAR value, padded like in an IN list of literals.
  private case object PaddedLiteral extends InElementPadding
  // A column whose type has CHAR, padded together with the columns of the same type, regardless
  // of the CHAR and VARCHAR lengths.
  private case class PaddedColumn(typeWithoutLengths: DataType) extends InElementPadding

  private def inElementPadding(value: Attribute, element: Expression): InElementPadding = {
    val valueRawType = CharVarcharUtils.getRawType(value.metadata)
    element match {
      case e if e.foldable =>
        if (valueRawType.exists(_.isInstanceOf[CharType])) PaddedLiteral else NotPadded
      case a: Attribute =>
        CharVarcharUtils.getRawType(a.metadata)
          .filter(_.existsRecursively(_.isInstanceOf[CharType]))
          .map { rawType =>
            PaddedColumn(rawType.transformRecursively {
              case _: CharType => CharType(0)
              case _: VarcharType => VarcharType(0)
            })
          }
          .getOrElse(NotPadded)
      case _ => NotPadded
    }
  }

  /**
   * Matches `value IN (list)` whose elements are padded in different ways in their comparisons
   * with the value, and returns the groups of elements that are padded the same way.
   */
  private object InWithMixedPadding {
    def unapply(in: In): Option[Seq[Seq[Expression]]] = in.value match {
      case AttrOrOuterRef(attr)
          if in.resolved && CharVarcharUtils.getRawType(attr.metadata).isDefined =>
        val paddings = in.list.map(inElementPadding(attr, _))
        val distinctPaddings = paddings.distinct
        if (distinctPaddings.length > 1) {
          Some(distinctPaddings.map { padding =>
            in.list.zip(paddings).collect { case (element, p) if p == padding => element }
          })
        } else {
          None
        }
      case _ => None
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
