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

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.analysis.CollationStrength.{Default, Explicit, Implicit}
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.haveSameType
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayAppend, ArrayContains, ArrayExcept, ArrayIntersect, ArrayJoin, ArrayPosition, ArrayRemove, ArraysOverlap, ArrayUnion, AttributeReference, CaseWhen, Cast, Coalesce, Collate, Concat, ConcatWs, Contains, CreateArray, CreateMap, Elt, EndsWith, EqualNullSafe, EqualTo, Expression, ExtractValue, FindInSet, GetMapValue, GreaterThan, GreaterThanOrEqual, Greatest, If, In, InSubquery, Lag, Lead, Least, LessThan, LessThanOrEqual, Levenshtein, Literal, Mask, Overlay, RaiseError, RegExpReplace, SplitPart, StartsWith, StringInstr, StringLocate, StringLPad, StringReplace, StringRPad, StringSplitSQL, StringToMap, StringTranslate, StringTrim, StringTrimLeft, StringTrimRight, SubqueryExpression, SubstringIndex, ToNumber, TryToNumber, VariableReference}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StringType}
import org.apache.spark.sql.util.SchemaUtils

/**
 * Type coercion helper that matches against expressions in order to apply collation type coercion.
 */
object CollationTypeCoercion {

  private val COLLATION_CONTEXT_TAG = new TreeNodeTag[CollationContext]("collationContext")

  private def hasCollationContextTag(expr: Expression): Boolean = {
    expr.getTagValue(COLLATION_CONTEXT_TAG).isDefined
  }

  def apply(expression: Expression): Expression = expression match {
    case cast: Cast if shouldRemoveCast(cast) =>
      cast.child

    case ifExpr: If =>
      ifExpr.withNewChildren(
        ifExpr.predicate +: collateToSingleType(Seq(ifExpr.trueValue, ifExpr.falseValue))
      )

    case caseWhenExpr: CaseWhen if !haveSameType(caseWhenExpr.inputTypesForMerging) =>
      val outputStringType = findLeastCommonStringType(
        caseWhenExpr.branches.map(_._2) ++ caseWhenExpr.elseValue)
      outputStringType match {
        case Some(st) =>
          val newBranches = caseWhenExpr.branches.map { case (condition, value) =>
            (condition, castStringType(value, st))
          }
          val newElseValue =
            caseWhenExpr.elseValue.map(e => castStringType(e, st))
          CaseWhen(newBranches, newElseValue)

        case _ =>
          caseWhenExpr
      }

    case stringLocate: StringLocate =>
      stringLocate.withNewChildren(
        collateToSingleType(Seq(stringLocate.first, stringLocate.second)) :+ stringLocate.third
      )

    case substringIndex: SubstringIndex =>
      substringIndex.withNewChildren(
        collateToSingleType(Seq(substringIndex.first, substringIndex.second)) :+
          substringIndex.third
      )

    case eltExpr: Elt =>
      eltExpr.withNewChildren(eltExpr.children.head +: collateToSingleType(eltExpr.children.tail))

    case overlayExpr: Overlay =>
      overlayExpr.withNewChildren(
        collateToSingleType(Seq(overlayExpr.input, overlayExpr.replace))
          ++ Seq(overlayExpr.pos, overlayExpr.len)
      )

    case regExpReplace: RegExpReplace =>
      val Seq(subject, rep) = collateToSingleType(Seq(regExpReplace.subject, regExpReplace.rep))
      val newChildren = Seq(subject, regExpReplace.regexp, rep, regExpReplace.pos)
      regExpReplace.withNewChildren(newChildren)

    case stringPadExpr @ (_: StringRPad | _: StringLPad) =>
      val Seq(str, len, pad) = stringPadExpr.children
      val Seq(newStr, newPad) = collateToSingleType(Seq(str, pad))
      stringPadExpr.withNewChildren(Seq(newStr, len, newPad))

    case raiseError: RaiseError =>
      val newErrorParams = raiseError.errorParms.dataType match {
        case MapType(StringType, StringType, _) => raiseError.errorParms
        case _ => Cast(raiseError.errorParms, MapType(StringType, StringType))
      }
      raiseError.withNewChildren(Seq(raiseError.errorClass, newErrorParams))

    case framelessOffsetWindow @ (_: Lag | _: Lead) =>
      val Seq(input, offset, default) = framelessOffsetWindow.children
      val Seq(newInput, newDefault) = collateToSingleType(Seq(input, default))
      framelessOffsetWindow.withNewChildren(Seq(newInput, offset, newDefault))

    case mapCreate: CreateMap if mapCreate.children.size % 2 == 0 =>
      // We only take in mapCreate if it has even number of children, as otherwise it should fail
      // with wrong number of arguments
      val newKeys = collateToSingleType(mapCreate.keys)
      val newValues = collateToSingleType(mapCreate.values)
      mapCreate.withNewChildren(newKeys.zip(newValues).flatMap(pair => Seq(pair._1, pair._2)))

    case splitPart: SplitPart =>
      val Seq(str, delimiter, partNum) = splitPart.children
      val Seq(newStr, newDelimiter) = collateToSingleType(Seq(str, delimiter))
      splitPart.withNewChildren(Seq(newStr, newDelimiter, partNum))

    case stringSplitSQL: StringSplitSQL =>
      val Seq(str, delimiter) = stringSplitSQL.children
      val Seq(newStr, newDelimiter) = collateToSingleType(Seq(str, delimiter))
      stringSplitSQL.withNewChildren(Seq(newStr, newDelimiter))

    case levenshtein: Levenshtein =>
      val Seq(left, right, threshold @ _*) = levenshtein.children
      val Seq(newLeft, newRight) = collateToSingleType(Seq(left, right))
      levenshtein.withNewChildren(Seq(newLeft, newRight) ++ threshold)

    case getMap @ GetMapValue(child, key) if getMap.keyType != key.dataType =>
      key match {
        case Literal(_, _: StringType) =>
          GetMapValue(child, Cast(key, getMap.keyType))
        case _ =>
          getMap
      }

    case otherExpr @ (_: In | _: InSubquery | _: CreateArray | _: ArrayJoin | _: Concat |
        _: Greatest | _: Least | _: Coalesce | _: ArrayContains | _: ArrayExcept | _: ConcatWs |
        _: Mask | _: StringReplace | _: StringTranslate | _: StringTrim | _: StringTrimLeft |
        _: StringTrimRight | _: ArrayAppend | _: ArrayIntersect | _: ArrayPosition |
        _: ArrayRemove | _: ArrayUnion | _: ArraysOverlap | _: Contains | _: EndsWith |
        _: EqualNullSafe | _: EqualTo | _: FindInSet | _: GreaterThan | _: GreaterThanOrEqual |
        _: LessThan | _: LessThanOrEqual | _: StartsWith | _: StringInstr | _: ToNumber |
        _: TryToNumber | _: StringToMap) =>
      val newChildren = collateToSingleType(otherExpr.children)
      otherExpr.withNewChildren(newChildren)

    case other => other
  }

  /**
   * If childType is collated and target is UTF8_BINARY, the collation of the output
   * should be that of the childType.
   */
  private def shouldRemoveCast(cast: Cast): Boolean = {
    val isUserDefined = cast.getTagValue(Cast.USER_SPECIFIED_CAST).isDefined
    val isChildTypeCollatedString = cast.child.dataType match {
      case st: StringType => !st.isUTF8BinaryCollation
      case _ => false
    }
    val targetType = cast.dataType

    isUserDefined && isChildTypeCollatedString && targetType == StringType
  }

  /**
   * Extracts StringTypes from filtered hasStringType
   */
  @tailrec
  private def extractStringType(dt: DataType): Option[StringType] = dt match {
    case st: StringType => Some(st)
    case ArrayType(et, _) => extractStringType(et)
    case _ => None
  }

  /**
   * Casts given expression to collated StringType with id equal to collationId only
   * if expression has StringType in the first place.
   */
  def castStringType(expr: Expression, st: StringType): Expression = {
    castStringType(expr.dataType, st)
      .map(dt => Cast(expr, dt))
      .getOrElse(expr)
  }

  private def castStringType(inType: DataType, castType: StringType): Option[DataType] = {
    inType match {
      case st: StringType if st.collationId != castType.collationId =>
        Some(castType)
      case ArrayType(arrType, nullable) =>
        castStringType(arrType, castType).map(ArrayType(_, nullable))
      case _ => None
    }
  }

  /**
   * Collates input expressions to a single collation.
   */
  def collateToSingleType(expressions: Seq[Expression]): Seq[Expression] = {
    val lctOpt = findLeastCommonStringType(expressions)

    lctOpt match {
      case Some(lct) =>
        expressions.map(e => castStringType(e, lct))
      case _ =>
        expressions
    }
  }

  private def findLeastCommonStringType(expressions: Seq[Expression]): Option[StringType] = {
    if (!expressions.exists(e => SchemaUtils.hasNonUTF8BinaryCollation(e.dataType))) {
      return None
    }

    val collationContextWinner = expressions.foldLeft(findCollationContext(expressions.head)) {
      case (Some(left), right) =>
        findCollationContext(right).flatMap { ctx =>
          collationPrecedenceWinner(left, ctx)
        }
      case (None, _) => return None
    }

    collationContextWinner.flatMap { cc =>
      extractStringType(cc.dataType)
    }
  }

  private def findCollationContext(expr: Expression): Option[CollationContext] = {
    val contextOpt = expr match {
      case _ if hasCollationContextTag(expr) =>
        Some(expr.getTagValue(COLLATION_CONTEXT_TAG).get)

      case _ if !expr.dataType.existsRecursively(_.isInstanceOf[StringType]) =>
        None

      case collate: Collate =>
        Some(CollationContext(collate.dataType, Explicit))

      case _: Alias | _: SubqueryExpression | _: AttributeReference | _: VariableReference =>
        Some(CollationContext(expr.dataType, Implicit))

      case _: Literal =>
        Some(CollationContext(expr.dataType, Default))

      case extract: ExtractValue =>
        findCollationContext(extract.child)
          .map(cc => CollationContext(extract.dataType, cc.strength))

      case _ if expr.children.isEmpty =>
        Some(CollationContext(expr.dataType, Default))

      case _ =>
        expr.children
          .flatMap(findCollationContext)
          .foldLeft(Option.empty[CollationContext]) {
            case (Some(left), right) =>
              collationPrecedenceWinner(left, right)
            case (None, right) =>
              Some(right)
          }
    }

    contextOpt.foreach(expr.setTagValue(COLLATION_CONTEXT_TAG, _))
    contextOpt
  }

  private def collationPrecedenceWinner(
      left: CollationContext,
      right: CollationContext): Option[CollationContext] = {

    val (leftStringType, rightStringType) =
      (extractStringType(left.dataType), extractStringType(right.dataType)) match {
        case (None, None) =>
          return None
        case (Some(_), None) =>
          return Some(left)
        case (None, Some(_)) =>
          return Some(right)
        case (Some(l), Some(r)) =>
          (l, r)
      }

    (left.strength, right.strength) match {
      case (Explicit, Explicit) if leftStringType != rightStringType =>
        throw QueryCompilationErrors.explicitCollationMismatchError(
          Seq(leftStringType, rightStringType))

      case (Explicit, _) | (_, Explicit) =>
        if (left.strength == Explicit) Some(left) else Some(right)

      case (Implicit, Implicit) if leftStringType != rightStringType =>
        throw QueryCompilationErrors.implicitCollationMismatchError(
          Seq(leftStringType, rightStringType))

      case (Implicit, _) | (_, Implicit) =>
        if (left.strength == Implicit) Some(left) else Some(right)

      case (Default, Default) if leftStringType != rightStringType =>
        Some(CollationContext(SQLConf.get.defaultStringType, Default))

      case _ =>
        Some(left)
    }
  }
}

/**
 * Represents the strength of collation used for determining precedence in collation resolution.
 */
private sealed trait CollationStrength {}

  private object CollationStrength {
  case object Explicit extends CollationStrength {}
  case object Implicit extends CollationStrength {}
  case object Default extends CollationStrength {}
}

/**
 * Encapsulates the context for collation, including data type and strength.
 *
 * @param dataType The data type associated with this collation context.
 * @param strength The strength level of the collation, which determines its precedence.
 */
private case class CollationContext(dataType: DataType, strength: CollationStrength) {}
