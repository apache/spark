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

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.{hasStringType, haveSameType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StringType}
import org.apache.spark.sql.types.CollationStrength.{Default, Explicit, Implicit}

object CollationTypeCasts extends TypeCoercionRule {
  override val transform: PartialFunction[Expression, Expression] = {
    case e if !e.childrenResolved => e

    case ifExpr: If =>
      ifExpr.withNewChildren(
        ifExpr.predicate +: collateToSingleType(Seq(ifExpr.trueValue, ifExpr.falseValue)))

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
      stringLocate.withNewChildren(collateToSingleType(
        Seq(stringLocate.first, stringLocate.second)) :+ stringLocate.third)

    case substringIndex: SubstringIndex =>
      substringIndex.withNewChildren(
        collateToSingleType(
          Seq(substringIndex.first, substringIndex.second)) :+ substringIndex.third)

    case eltExpr: Elt =>
      eltExpr.withNewChildren(eltExpr.children.head +: collateToSingleType(eltExpr.children.tail))

    case overlayExpr: Overlay =>
      overlayExpr.withNewChildren(collateToSingleType(Seq(overlayExpr.input, overlayExpr.replace))
        ++ Seq(overlayExpr.pos, overlayExpr.len))

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

    case mapCreate : CreateMap if mapCreate.children.size % 2 == 0 =>
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

    case otherExpr @ (
      _: In | _: InSubquery | _: CreateArray | _: ArrayJoin | _: Concat | _: Greatest | _: Least |
      _: Coalesce | _: ArrayContains | _: ArrayExcept | _: ConcatWs | _: Mask | _: StringReplace |
      _: StringTranslate | _: StringTrim | _: StringTrimLeft | _: StringTrimRight | _: ArrayAppend |
      _: ArrayIntersect | _: ArrayPosition | _: ArrayRemove | _: ArrayUnion | _: ArraysOverlap |
      _: Contains | _: EndsWith | _: EqualNullSafe | _: EqualTo | _: FindInSet | _: GreaterThan |
      _: GreaterThanOrEqual | _: LessThan | _: LessThanOrEqual | _: StartsWith | _: StringInstr |
      _: ToNumber | _: TryToNumber | _: StringToMap) =>
      val newChildren = collateToSingleType(otherExpr.children)
      otherExpr.withNewChildren(newChildren)
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
      case st: StringType
          // TODO: should we override equals to do this?
          if st.collationId != castType.collationId || st.strength != castType.strength =>
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
    if (!expressions.exists(e => hasStringType(e.dataType))) {
      return None
    }

    expressions
      .map(e => preprocessCollationStrengths(e).dataType)
      .reduceLeftOption { (left, right) =>
        findLeastCommonStringType(left, right).getOrElse(return None)
      }.collect { case st: StringType => st }
  }

  @tailrec
  private def findLeastCommonStringType(left: DataType, right: DataType): Option[StringType] = {
    (left, right) match {
      case (leftStringType: StringType, rightStringType: StringType) =>
        Some(collationPrecedenceWinner(leftStringType, rightStringType))

      case (ArrayType(elemType, _), stringType: StringType) =>
        findLeastCommonStringType(elemType, stringType)

      case (stringType: StringType, ArrayType(elemType, _)) =>
        findLeastCommonStringType(stringType, elemType)

      case (ArrayType(leftType, _), ArrayType(rightType, _)) =>
        findLeastCommonStringType(leftType, rightType)

      case _ => None
    }
  }

  private def collationPrecedenceWinner(left: StringType, right: StringType): StringType = {
    (left.strength, right.strength) match {
      case (Explicit, Explicit) if left.collationId != right.collationId =>
        throw QueryCompilationErrors.explicitCollationMismatchError(Seq(left, right))

      case (Explicit, _) => left

      case (_, Explicit) => right

      case (Implicit, Implicit) if left.collationId != right.collationId =>
        throw QueryCompilationErrors.implicitCollationMismatchError(Seq(left, right))

      case (Implicit, _) => left

      case (_, Implicit) => right

      case (Default, Default) if left.collationId != right.collationId =>
        SQLConf.get.defaultStringType

      case _ => left
    }
  }

  /**
   * Some expressions are special, and we should preprocess them to have correct collation strength
   * before we start computing the least common string type.
   */
  private def preprocessCollationStrengths(expr: Expression): Expression = expr match {
    // var reference should always have implicit strength
    case _: VariableReference =>
      extractStringType(expr.dataType) match {
        case Some(st) => castStringType(expr, StringType(st.collationId, Implicit))
        case _ => expr
      }

    // user specified cast should always have the collation of child
    // if child is of StringType
    case cast @ Cast(_, _: StringType, _, _)
        if cast.getTagValue(Cast.USER_SPECIFIED_CAST).isDefined =>

      (extractStringType(cast.dataType), extractStringType(cast.child.dataType)) match {
        case (Some(_), Some(childType)) =>
          castStringType(cast.dataType, childType) match {
            case Some(dt) => cast.copy(dataType = dt)
            case None => cast
          }
        case _ =>
          cast
      }

    case _ =>
      expr
  }
}
