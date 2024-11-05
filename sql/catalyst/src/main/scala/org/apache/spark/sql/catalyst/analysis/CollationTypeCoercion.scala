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

import javax.annotation.Nullable

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.{hasStringType, haveSameType}
import org.apache.spark.sql.catalyst.expressions.{
  ArrayAppend,
  ArrayContains,
  ArrayExcept,
  ArrayIntersect,
  ArrayJoin,
  ArrayPosition,
  ArrayRemove,
  ArraysOverlap,
  ArrayUnion,
  CaseWhen,
  Cast,
  Coalesce,
  Collate,
  Concat,
  ConcatWs,
  Contains,
  CreateArray,
  CreateMap,
  Elt,
  EndsWith,
  EqualNullSafe,
  EqualTo,
  Expression,
  FindInSet,
  GetMapValue,
  GreaterThan,
  GreaterThanOrEqual,
  Greatest,
  If,
  In,
  InSubquery,
  Lag,
  Lead,
  Least,
  LessThan,
  LessThanOrEqual,
  Levenshtein,
  Literal,
  Mask,
  Overlay,
  RaiseError,
  RegExpReplace,
  SplitPart,
  StartsWith,
  StringInstr,
  StringLocate,
  StringLPad,
  StringReplace,
  StringRPad,
  StringSplitSQL,
  StringToMap,
  StringTranslate,
  StringTrim,
  StringTrimLeft,
  StringTrimRight,
  SubstringIndex,
  ToNumber,
  TryToNumber
}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StringType}

/**
 * Type coercion helper that matches against expressions in order to apply collation type coercion.
 */
object CollationTypeCoercion {
  def apply(expression: Expression): Expression = expression match {
    case ifExpr: If =>
      ifExpr.withNewChildren(
        ifExpr.predicate +: collateToSingleType(Seq(ifExpr.trueValue, ifExpr.falseValue))
      )

    case caseWhenExpr: CaseWhen if !haveSameType(caseWhenExpr.inputTypesForMerging) =>
      val outputStringType =
        getOutputCollation(caseWhenExpr.branches.map(_._2) ++ caseWhenExpr.elseValue)
      val newBranches = caseWhenExpr.branches.map {
        case (condition, value) =>
          (condition, castStringType(value, outputStringType).getOrElse(value))
      }
      val newElseValue =
        caseWhenExpr.elseValue.map(e => castStringType(e, outputStringType).getOrElse(e))
      CaseWhen(newBranches, newElseValue)

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
   * Extracts StringTypes from filtered hasStringType
   */
  @tailrec
  private def extractStringType(dt: DataType): StringType = dt match {
    case st: StringType => st
    case ArrayType(et, _) => extractStringType(et)
  }

  /**
   * Casts given expression to collated StringType with id equal to collationId only
   * if expression has StringType in the first place.
   * @param expr
   * @param collationId
   * @return
   */
  def castStringType(expr: Expression, st: StringType): Option[Expression] =
    castStringType(expr.dataType, st).map { dt => Cast(expr, dt)}

  private def castStringType(inType: DataType, castType: StringType): Option[DataType] = {
    @Nullable val ret: DataType = inType match {
      case st: StringType if st.collationId != castType.collationId => castType
      case ArrayType(arrType, nullable) =>
        castStringType(arrType, castType).map(ArrayType(_, nullable)).orNull
      case _ => null
    }
    Option(ret)
  }

  /**
   * Collates input expressions to a single collation.
   */
  def collateToSingleType(exprs: Seq[Expression]): Seq[Expression] = {
    val st = getOutputCollation(exprs)

    exprs.map(e => castStringType(e, st).getOrElse(e))
  }

  /**
   * Based on the data types of the input expressions this method determines
   * a collation type which the output will have. This function accepts Seq of
   * any expressions, but will only be affected by collated StringTypes or
   * complex DataTypes with collated StringTypes (e.g. ArrayType)
   */
  def getOutputCollation(expr: Seq[Expression]): StringType = {
    val explicitTypes = expr.filter {
        case _: Collate => true
        case _ => false
      }
      .map(_.dataType.asInstanceOf[StringType].collationId)
      .distinct

    explicitTypes.size match {
      // We have 1 explicit collation
      case 1 => StringType(explicitTypes.head)
      // Multiple explicit collations occurred
      case size if size > 1 =>
        throw QueryCompilationErrors
          .explicitCollationMismatchError(
            explicitTypes.map(t => StringType(t))
          )
      // Only implicit or default collations present
      case 0 =>
        val implicitTypes = expr.filter {
            case Literal(_, _: StringType) => false
            case cast: Cast if cast.getTagValue(Cast.USER_SPECIFIED_CAST).isEmpty =>
              cast.child.dataType.isInstanceOf[StringType]
            case _ => true
          }
          .map(_.dataType)
          .filter(hasStringType)
          .map(extractStringType(_).collationId)
          .distinct

        if (implicitTypes.length > 1) {
          throw QueryCompilationErrors.implicitCollationMismatchError(
            implicitTypes.map(t => StringType(t))
          )
        }
        else {
          implicitTypes.headOption.map(StringType(_)).getOrElse(SQLConf.get.defaultStringType)
        }
    }
  }
}
