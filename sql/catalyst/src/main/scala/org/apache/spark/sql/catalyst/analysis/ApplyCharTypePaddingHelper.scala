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

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  BinaryComparison,
  Expression,
  GetStructField,
  In,
  Literal,
  NamedExpression,
  OuterReference
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreePattern.{BINARY_COMPARISON, IN}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.catalyst.util.CharVarcharUtils.createStringRPad
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{CharType, DataType, StringType, StructType}
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
   * A column, or a field of a struct column reached through [[GetStructField]], with the raw type
   * of that column or field. The raw type is the one recorded in the column's metadata, so a CHAR
   * or VARCHAR field keeps its length although its data type is a string.
   */
  object RawTypedRef {
    // A value whose type no longer matches its raw type, e.g. after a cast, has none. A CHAR
    // value is any string, as its collation can change while it keeps its metadata, as with
    // Dataset.to.
    def unapply(e: Expression): Option[DataType] = rawType(e).filter {
      case _: CharType => e.dataType.isInstanceOf[StringType]
      case rawType => DataTypeUtils.equalsIgnoreNullability(
        CharVarcharUtils.replaceCharVarcharWithString(rawType), e.dataType)
    }

    private def rawType(e: Expression): Option[DataType] = e match {
      case AttrOrOuterRef(attr) => CharVarcharUtils.getRawType(attr.metadata)
      case GetStructField(child, ordinal, _) =>
        child.dataType match {
          case current: StructType =>
            // The raw struct usually has the fields of the current one. Dataset.to can reorder
            // and rename the fields of a struct at any level, and keeps the column's metadata,
            // so a field with another name is looked up by name, as Dataset.to matches them.
            val name = current(ordinal).name
            rawType(child).flatMap {
              case raw: StructType =>
                raw.lift(ordinal).filter(_.name == name)
                  .orElse(raw.fields.find(f => SQLConf.get.resolver(f.name, name)))
              case _ => None
            }.map(_.dataType)
          case _ => None
        }
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
      // The constant is checked first, so that the raw type is only parsed when it is needed.
      // With a constant on one side, none of the cases below can match.
      case b @ BinaryComparison(e, lit) if isStringConstant(lit) => e match {
        case RawTypedRef(c: CharType) =>
          padAttrLitCmp(e, c, padCharCol, lit).map(b.withNewChildren).getOrElse(b)
        case _ => b
      }

      case b @ BinaryComparison(lit, e) if isStringConstant(lit) => e match {
        case RawTypedRef(c: CharType) =>
          padAttrLitCmp(e, c, padCharCol, lit)
            .map(newChildren => b.withNewChildren(newChildren.reverse))
            .getOrElse(b)
        case _ => b
      }

      case i @ In(e @ RawTypedRef(c: CharType), list) if i.resolved && list.forall(_.foldable) =>
        // Compute the length of every list element in place, so that each length stays aligned
        // with the element it came from. NULL elements have no length: they can never match, so
        // they are left untouched instead of being padded. As in ConstantFolding, an element that
        // fails to evaluate is left as it is, and its failure to the evaluation of the query.
        val literalCharLengths = list.map(lit => tryEvalString(lit).flatten.map(_.numChars()))
        val targetLen = (c.length +: literalCharLengths.flatten).max
        i.copy(
          value = addPadding(e, c.length, targetLen, alwaysPad = padCharCol),
          list = list.zip(literalCharLengths).map {
            case (lit, Some(charLength)) =>
              addPadding(lit, charLength, targetLen, alwaysPad = false)
            case (lit, None) => lit
          }
        )

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

  private def isStringConstant(e: Expression): Boolean =
    e.foldable && e.dataType.isInstanceOf[StringType]

  /**
   * Evaluates a foldable string expression: Some(value), with a null value for NULL, or None if
   * it fails. The comparison may be in a branch that is never evaluated, so as in
   * ConstantFolding, the failure is left to the evaluation of the query.
   */
  private def tryEvalString(e: Expression): Option[Option[UTF8String]] = {
    try {
      Some(Option(e.eval().asInstanceOf[UTF8String]))
    } catch {
      case NonFatal(_) => None
    }
  }

  private def padAttrLitCmp(
      expr: Expression,
      c: CharType,
      padCharCol: Boolean,
      lit: Expression): Option[Seq[Expression]] = {
    // A NULL, or a constant that fails to evaluate, is left as it is.
    tryEvalString(lit).flatten.flatMap { str =>
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
