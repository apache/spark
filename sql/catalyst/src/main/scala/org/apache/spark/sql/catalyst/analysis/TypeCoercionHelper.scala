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

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.PromoteStrings.conf
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  ArrayJoin,
  BinaryOperator,
  CaseWhen,
  Cast,
  Coalesce,
  Concat,
  CreateArray,
  CreateMap,
  DateAdd,
  DateSub,
  Elt,
  ExpectsInputTypes,
  Expression,
  Greatest,
  If,
  ImplicitCastInputTypes,
  In,
  InSubquery,
  Least,
  ListQuery,
  Literal,
  MapConcat,
  MapZipWith,
  NaNvl,
  RangeFrame,
  ScalaUDF,
  Sequence,
  SpecialFrameBoundary,
  SpecifiedWindowFrame,
  SubtractTimestamps,
  TimeAdd,
  WindowSpecDefinition
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Sum}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.{AbstractArrayType, StringTypeWithCollation}
import org.apache.spark.sql.types.{
  AbstractDataType,
  AnyDataType,
  AnyTimestampTypeExpression,
  ArrayType,
  BinaryType,
  BooleanType,
  DataType,
  DatetimeType,
  DateType,
  DateTypeExpression,
  DecimalType,
  DoubleType,
  FloatType,
  FractionalType,
  IntegerType,
  IntegralType,
  MapType,
  NullType,
  StringType,
  StringTypeExpression,
  StructType,
  TimestampNTZType,
  TimestampType,
  TimestampTypeExpression
}

abstract class TypeCoercionHelper {

  /**
   * A collection of [[Rule]] that can be used to coerce differing types that participate in
   * operations into compatible ones.
   */
  def typeCoercionRules: List[Rule[LogicalPlan]]

  /**
   * Find the tightest common type of two types that might be used in a binary expression.
   * This handles all numeric types except fixed-precision decimals interacting with each other or
   * with primitive types, because in that case the precision and scale of the result depends on
   * the operation. Those rules are implemented in [[DecimalPrecision]].
   */
  val findTightestCommonType: (DataType, DataType) => Option[DataType]

  /**
   * Looking for a widened data type of two given data types with some acceptable loss of precision.
   * E.g. there is no common type for double and decimal because double's range
   * is larger than decimal, and yet decimal is more precise than double, but in
   * union we would cast the decimal into double.
   */
  def findWiderTypeForTwo(t1: DataType, t2: DataType): Option[DataType]

  /**
   * Looking for a widened data type of a given sequence of data types with some acceptable loss
   * of precision.
   * E.g. there is no common type for double and decimal because double's range
   * is larger than decimal, and yet decimal is more precise than double, but in
   * union we would cast the decimal into double.
   */
  def findWiderCommonType(types: Seq[DataType]): Option[DataType]

  /**
   * Given an expected data type, try to cast the expression and return the cast expression.
   *
   * If the expression already fits the input type, we simply return the expression itself.
   * If the expression has an incompatible type that cannot be implicitly cast, return None.
   */
  def implicitCast(e: Expression, expectedType: AbstractDataType): Option[Expression]

  /**
   * Whether casting `from` as `to` is valid.
   */
  def canCast(from: DataType, to: DataType): Boolean

  protected def findTypeForComplex(
      t1: DataType,
      t2: DataType,
      findTypeFunc: (DataType, DataType) => Option[DataType]): Option[DataType] = (t1, t2) match {
    case (ArrayType(et1, containsNull1), ArrayType(et2, containsNull2)) =>
      findTypeFunc(et1, et2).map { et =>
        ArrayType(
          et,
          containsNull1 || containsNull2 ||
          Cast.forceNullable(et1, et) || Cast.forceNullable(et2, et)
        )
      }
    case (MapType(kt1, vt1, valueContainsNull1), MapType(kt2, vt2, valueContainsNull2)) =>
      findTypeFunc(kt1, kt2)
        .filter { kt =>
          !Cast.forceNullable(kt1, kt) && !Cast.forceNullable(kt2, kt)
        }
        .flatMap { kt =>
          findTypeFunc(vt1, vt2).map { vt =>
            MapType(
              kt,
              vt,
              valueContainsNull1 || valueContainsNull2 ||
              Cast.forceNullable(vt1, vt) || Cast.forceNullable(vt2, vt)
            )
          }
        }
    case (StructType(fields1), StructType(fields2)) if fields1.length == fields2.length =>
      val resolver = SQLConf.get.resolver
      fields1.zip(fields2).foldLeft(Option(new StructType())) {
        case (Some(struct), (field1, field2)) if resolver(field1.name, field2.name) =>
          findTypeFunc(field1.dataType, field2.dataType).map { dt =>
            struct.add(
              field1.name,
              dt,
              field1.nullable || field2.nullable ||
              Cast.forceNullable(field1.dataType, dt) || Cast.forceNullable(field2.dataType, dt)
            )
          }
        case _ => None
      }
    case _ => None
  }

  /**
   * Finds a wider type when one or both types are decimals. If the wider decimal type exceeds
   * system limitation, this rule will truncate the decimal type. If a decimal and other fractional
   * types are compared, returns a double type.
   */
  protected def findWiderTypeForDecimal(dt1: DataType, dt2: DataType): Option[DataType] = {
    (dt1, dt2) match {
      case (t1: DecimalType, t2: DecimalType) =>
        Some(DecimalPrecisionTypeCoercion.widerDecimalType(t1, t2))
      case (t: IntegralType, d: DecimalType) =>
        Some(DecimalPrecisionTypeCoercion.widerDecimalType(DecimalType.forType(t), d))
      case (d: DecimalType, t: IntegralType) =>
        Some(DecimalPrecisionTypeCoercion.widerDecimalType(DecimalType.forType(t), d))
      case (_: FractionalType, _: DecimalType) | (_: DecimalType, _: FractionalType) =>
        Some(DoubleType)
      case _ => None
    }
  }

  /**
   * Similar to [[findWiderTypeForTwo]] that can handle decimal types, but can't promote to
   * string. If the wider decimal type exceeds system limitation, this rule will truncate
   * the decimal type before return it.
   */
  private[catalyst] def findWiderTypeWithoutStringPromotionForTwo(
      t1: DataType,
      t2: DataType): Option[DataType] = {
    findTightestCommonType(t1, t2)
      .orElse(findWiderTypeForDecimal(t1, t2))
      .orElse(findTypeForComplex(t1, t2, findWiderTypeWithoutStringPromotionForTwo))
  }

  def findWiderTypeWithoutStringPromotion(types: Seq[DataType]): Option[DataType] = {
    types.foldLeft[Option[DataType]](Some(NullType))(
      (r, c) =>
        r match {
          case Some(d) => findWiderTypeWithoutStringPromotionForTwo(d, c)
          case None => None
        }
    )
  }

  /**
   * Check whether the given types are equal ignoring nullable, containsNull and valueContainsNull.
   */
  def haveSameType(types: Seq[DataType]): Boolean = {
    if (types.size <= 1) {
      true
    } else {
      val head = types.head
      types.tail.forall(e => DataTypeUtils.sameType(e, head))
    }
  }

  protected def castIfNotSameType(expr: Expression, dt: DataType): Expression = {
    if (!DataTypeUtils.sameType(expr.dataType, dt)) {
      Cast(expr, dt)
    } else {
      expr
    }
  }

  protected def findWiderDateTimeType(d1: DatetimeType, d2: DatetimeType): DatetimeType =
    (d1, d2) match {
      case (_: TimestampType, _: DateType) | (_: DateType, _: TimestampType) =>
        TimestampType

      case (_: TimestampType, _: TimestampNTZType) | (_: TimestampNTZType, _: TimestampType) =>
        TimestampType

      case (_: TimestampNTZType, _: DateType) | (_: DateType, _: TimestampNTZType) =>
        TimestampNTZType
    }

  /**
   * Type coercion helper that matches agaist [[In]] and [[InSubquery]] expressions in order to
   * type coerce LHS and RHS to expected types.
   */
  object InTypeCoercion {
    def apply(expression: Expression): Expression = expression match {
      // Handle type casting required between value expression and subquery output
      // in IN subquery.
      case i @ InSubquery(lhs, l: ListQuery) if !i.resolved && lhs.length == l.plan.output.length =>
        // LHS is the value expressions of IN subquery.
        // RHS is the subquery output.
        val rhs = l.plan.output

        val commonTypes = lhs.zip(rhs).flatMap {
          case (l, r) =>
            findWiderTypeForTwo(l.dataType, r.dataType)
        }

        // The number of columns/expressions must match between LHS and RHS of an
        // IN subquery expression.
        if (commonTypes.length == lhs.length) {
          val castedRhs = rhs.zip(commonTypes).map {
            case (e, dt) if e.dataType != dt => Alias(Cast(e, dt), e.name)()
            case (e, _) => e
          }
          val newLhs = lhs.zip(commonTypes).map {
            case (e, dt) if e.dataType != dt => Cast(e, dt)
            case (e, _) => e
          }

          InSubquery(newLhs, l.withNewPlan(Project(castedRhs, l.plan)))
        } else {
          i
        }

      case i @ In(a, b) if b.exists(_.dataType != a.dataType) =>
        findWiderCommonType(i.children.map(_.dataType)) match {
          case Some(finalDataType) => i.withNewChildren(i.children.map(Cast(_, finalDataType)))
          case None => i
        }

      case other => other
    }
  }

  /**
   * Type coercion helper that matches against function expression in order to type coerce function
   * argument types to expected types.
   */
  object FunctionArgumentTypeCoercion {
    def apply(expression: Expression): Expression = expression match {
      case a @ CreateArray(children, _) if !haveSameType(children.map(_.dataType)) =>
        val types = children.map(_.dataType)
        findWiderCommonType(types) match {
          case Some(finalDataType) => a.copy(children.map(castIfNotSameType(_, finalDataType)))
          case None => a
        }

      case c @ Concat(children)
          if children.forall(c => ArrayType.acceptsType(c.dataType)) &&
          !haveSameType(c.inputTypesForMerging) =>
        val types = children.map(_.dataType)
        findWiderCommonType(types) match {
          case Some(finalDataType) => Concat(children.map(castIfNotSameType(_, finalDataType)))
          case None => c
        }

      case aj @ ArrayJoin(arr, d, nr)
          if !AbstractArrayType(StringTypeWithCollation).acceptsType(arr.dataType) &&
          ArrayType.acceptsType(arr.dataType) =>
        val containsNull = arr.dataType.asInstanceOf[ArrayType].containsNull
        implicitCast(arr, ArrayType(StringType, containsNull)) match {
          case Some(castedArr) => ArrayJoin(castedArr, d, nr)
          case None => aj
        }

      case s @ Sequence(_, _, _, timeZoneId)
          if !haveSameType(s.coercibleChildren.map(_.dataType)) =>
        val types = s.coercibleChildren.map(_.dataType)
        findWiderCommonType(types) match {
          case Some(widerDataType) => s.castChildrenTo(widerDataType)
          case None => s
        }

      case m @ MapConcat(children)
          if children.forall(c => MapType.acceptsType(c.dataType)) &&
          !haveSameType(m.inputTypesForMerging) =>
        val types = children.map(_.dataType)
        findWiderCommonType(types) match {
          case Some(finalDataType) => MapConcat(children.map(castIfNotSameType(_, finalDataType)))
          case None => m
        }

      case m @ CreateMap(children, _)
          if m.keys.length == m.values.length &&
          (!haveSameType(m.keys.map(_.dataType)) || !haveSameType(m.values.map(_.dataType))) =>
        val keyTypes = m.keys.map(_.dataType)
        val newKeys = findWiderCommonType(keyTypes) match {
          case Some(finalDataType) => m.keys.map(castIfNotSameType(_, finalDataType))
          case None => m.keys
        }

        val valueTypes = m.values.map(_.dataType)
        val newValues = findWiderCommonType(valueTypes) match {
          case Some(finalDataType) => m.values.map(castIfNotSameType(_, finalDataType))
          case None => m.values
        }

        m.copy(newKeys.zip(newValues).flatMap { case (k, v) => Seq(k, v) })

      // Hive lets you do aggregation of timestamps... for some reason
      case Sum(e @ TimestampTypeExpression(), _) => Sum(Cast(e, DoubleType))
      case Average(e @ TimestampTypeExpression(), _) => Average(Cast(e, DoubleType))

      // Coalesce should return the first non-null value, which could be any column
      // from the list. So we need to make sure the return type is deterministic and
      // compatible with every child column.
      case c @ Coalesce(es) if !haveSameType(c.inputTypesForMerging) =>
        val types = es.map(_.dataType)
        findWiderCommonType(types) match {
          case Some(finalDataType) =>
            Coalesce(es.map(castIfNotSameType(_, finalDataType)))
          case None =>
            c
        }

      // When finding wider type for `Greatest` and `Least`, we should handle decimal types even if
      // we need to truncate, but we should not promote one side to string if the other side is
      // string.g
      case g @ Greatest(children) if !haveSameType(g.inputTypesForMerging) =>
        val types = children.map(_.dataType)
        findWiderTypeWithoutStringPromotion(types) match {
          case Some(finalDataType) => Greatest(children.map(castIfNotSameType(_, finalDataType)))
          case None => g
        }

      case l @ Least(children) if !haveSameType(l.inputTypesForMerging) =>
        val types = children.map(_.dataType)
        findWiderTypeWithoutStringPromotion(types) match {
          case Some(finalDataType) => Least(children.map(castIfNotSameType(_, finalDataType)))
          case None => l
        }

      case NaNvl(l, r) if l.dataType == DoubleType && r.dataType == FloatType =>
        NaNvl(l, Cast(r, DoubleType))
      case NaNvl(l, r) if l.dataType == FloatType && r.dataType == DoubleType =>
        NaNvl(Cast(l, DoubleType), r)
      case NaNvl(l, r) if r.dataType == NullType => NaNvl(l, Cast(r, l.dataType))

      case other => other
    }
  }

  /**
   * Type coercion helper that matches against [[Concat]] expressions in order to type coerce
   * expression's children to expected types.
   */
  object ConcatTypeCoercion {
    def apply(expression: Expression): Expression = expression match {
      // Skip nodes if unresolved or empty children
      case c @ Concat(children) if children.isEmpty => c
      case c @ Concat(children)
          if conf.concatBinaryAsString ||
          !children.map(_.dataType).forall(_ == BinaryType) =>
        val newChildren = c.children.map { e =>
          implicitCast(e, SQLConf.get.defaultStringType).getOrElse(e)
        }
        c.copy(children = newChildren)
      case other => other
    }
  }

  /**
   * Type coercion helper that matches against [[MapZipWith]] expressions in order to type coerce
   * key types of input maps to a common type.
   */
  object MapZipWithTypeCoercion {
    def apply(expression: Expression): Expression = expression match {
      // Lambda function isn't resolved when the rule is executed.
      case m @ MapZipWith(left, right, function)
          if m.arguments.forall(a => MapType.acceptsType(a.dataType)) &&
          !DataTypeUtils.sameType(m.leftKeyType, m.rightKeyType) =>
        findWiderTypeForTwo(m.leftKeyType, m.rightKeyType) match {
          case Some(finalKeyType)
              if !Cast.forceNullable(m.leftKeyType, finalKeyType) &&
              !Cast.forceNullable(m.rightKeyType, finalKeyType) =>
            val newLeft = castIfNotSameType(
              left,
              MapType(finalKeyType, m.leftValueType, m.leftValueContainsNull)
            )
            val newRight = castIfNotSameType(
              right,
              MapType(finalKeyType, m.rightValueType, m.rightValueContainsNull)
            )
            MapZipWith(newLeft, newRight, function)
          case _ => m
        }
      case other => other
    }
  }

  /**
   * Type coercion helper that matches against [[Elt]] expression in order to type coerce
   * expression's children to expected types.
   */
  object EltTypeCoercion {
    def apply(expression: Expression): Expression = expression match {
      case c @ Elt(children, _) if children.size < 2 => c
      case c @ Elt(children, _) =>
        val index = children.head
        val newIndex = implicitCast(index, IntegerType).getOrElse(index)
        val newInputs =
          if (conf.eltOutputAsString ||
            !children.tail.map(_.dataType).forall(_ == BinaryType)) {
            children.tail.map { e =>
              implicitCast(e, SQLConf.get.defaultStringType).getOrElse(e)
            }
          } else {
            children.tail
          }
        c.copy(children = newIndex +: newInputs)
      case other => other
    }
  }

  /**
   * Type coercion helper that matches against a [[CaseWhen]] expression in order to type coerce
   * different branches to a common type.
   */
  object CaseWhenTypeCoercion {
    def apply(expression: Expression): Expression = expression match {
      case c: CaseWhen if !haveSameType(c.inputTypesForMerging) =>
        val maybeCommonType = findWiderCommonType(c.inputTypesForMerging)
        maybeCommonType
          .map { commonType =>
            val newBranches = c.branches.map {
              case (condition, value) =>
                (condition, castIfNotSameType(value, commonType))
            }
            val newElseValue = c.elseValue.map(castIfNotSameType(_, commonType))
            CaseWhen(newBranches, newElseValue)
          }
          .getOrElse(c)

      case other => other
    }
  }

  /**
   * Type coercion helper that matches against an [[If]] expression in order to type coerce
   * different branches to a common type.
   */
  object IfTypeCoercion {
    def apply(expression: Expression): Expression = expression match {
      // Find tightest common type for If, if the true value and false value have different types.
      case i @ If(pred, left, right) if !haveSameType(i.inputTypesForMerging) =>
        findWiderTypeForTwo(left.dataType, right.dataType)
          .map { widestType =>
            val newLeft = castIfNotSameType(left, widestType)
            val newRight = castIfNotSameType(right, widestType)
            If(pred, newLeft, newRight)
          }
          .getOrElse(i) // If there is no applicable conversion, leave expression unchanged.
      case If(Literal(null, NullType), left, right) =>
        If(Literal.create(null, BooleanType), left, right)
      case If(pred, left, right) if pred.dataType == NullType =>
        If(Cast(pred, BooleanType), left, right)
      case other => other
    }
  }

  /**
   * Type coercion helper that matches against expression in order to type coerce expression's
   * input types to expected types.
   */
  object ImplicitTypeCoercion {
    def apply(expression: Expression): Expression = expression match {
      case b @ BinaryOperator(left, right)
          if canHandleTypeCoercion(left.dataType, right.dataType) =>
        findTightestCommonType(left.dataType, right.dataType)
          .map { commonType =>
            if (b.inputType.acceptsType(commonType)) {
              // If the expression accepts the tightest common type, cast to that.
              val newLeft = if (left.dataType == commonType) left else Cast(left, commonType)
              val newRight = if (right.dataType == commonType) right else Cast(right, commonType)
              b.withNewChildren(Seq(newLeft, newRight))
            } else {
              // Otherwise, don't do anything with the expression.
              b
            }
          }
          .getOrElse(b) // If there is no applicable conversion, leave expression unchanged.

      case e: ImplicitCastInputTypes if e.inputTypes.nonEmpty =>
        val children: Seq[Expression] = e.children.zip(e.inputTypes).map {
          case (in, expected) =>
            // If we cannot do the implicit cast, just use the original input.
            implicitCast(in, expected).getOrElse(in)
        }
        e.withNewChildren(children)

      case e: ExpectsInputTypes if e.inputTypes.nonEmpty =>
        // Convert NullType into some specific target type for ExpectsInputTypes that don't do
        // general implicit casting.
        val children: Seq[Expression] = e.children.zip(e.inputTypes).map {
          case (in, expected) =>
            if (in.dataType == NullType && !expected.acceptsType(NullType)) {
              Literal.create(null, expected.defaultConcreteType)
            } else {
              in
            }
        }
        e.withNewChildren(children)

      case udf: ScalaUDF if udf.inputTypes.nonEmpty =>
        val children = udf.children.zip(udf.inputTypes).map {
          case (in, expected) =>
            // Currently Scala UDF will only expect `AnyDataType` at top level, so this trick works.
            // In the future we should create types like `AbstractArrayType`, so that Scala UDF can
            // accept inputs of array type of arbitrary element type.
            if (expected == AnyDataType) {
              in
            } else {
              implicitCast(
                in,
                udfInputToCastType(in.dataType, expected.asInstanceOf[DataType])
              ).getOrElse(in)
            }

        }
        udf.copy(children = children)

      case other => other
    }

    private def canHandleTypeCoercion(leftType: DataType, rightType: DataType): Boolean = {
      (leftType, rightType) match {
        case (_: DecimalType, NullType) => true
        case (NullType, _: DecimalType) => true
        case _ =>
          // If DecimalType operands are involved except for the two cases above,
          // DecimalPrecision will handle it.
          !leftType.isInstanceOf[DecimalType] && !rightType.isInstanceOf[DecimalType] &&
          leftType != rightType
      }
    }

    private def udfInputToCastType(input: DataType, expectedType: DataType): DataType = {
      (input, expectedType) match {
        // SPARK-26308: avoid casting to an arbitrary precision and scale for decimals. Please note
        // that precision and scale cannot be inferred properly for a ScalaUDF because, when it is
        // created, it is not bound to any column. So here the precision and scale of the input
        // column is used.
        case (in: DecimalType, _: DecimalType) => in
        case (ArrayType(dtIn, _), ArrayType(dtExp, nullableExp)) =>
          ArrayType(udfInputToCastType(dtIn, dtExp), nullableExp)
        case (MapType(keyDtIn, valueDtIn, _), MapType(keyDtExp, valueDtExp, nullableExp)) =>
          MapType(
            udfInputToCastType(keyDtIn, keyDtExp),
            udfInputToCastType(valueDtIn, valueDtExp),
            nullableExp
          )
        case (StructType(fieldsIn), StructType(fieldsExp)) =>
          val fieldTypes =
            fieldsIn.map(_.dataType).zip(fieldsExp.map(_.dataType)).map {
              case (dtIn, dtExp) =>
                udfInputToCastType(dtIn, dtExp)
            }
          StructType(fieldsExp.zip(fieldTypes).map {
            case (field, newDt) =>
              field.copy(dataType = newDt)
          })
        case (_, other) => other
      }
    }
  }

  /**
   * Type coercion helper that matches against [[WindowFrameTypeCoercion]] expression in order to
   * type coerce window boundaries to the type they operate upon.
   */
  object WindowFrameTypeCoercion {
    def apply(expression: Expression): Expression = expression match {
      case s @ WindowSpecDefinition(
            _,
            Seq(order),
            SpecifiedWindowFrame(RangeFrame, lower, upper)
          ) =>
        s.copy(
          frameSpecification = SpecifiedWindowFrame(
            RangeFrame,
            createBoundaryCast(lower, order.dataType),
            createBoundaryCast(upper, order.dataType)
          )
        )

      case other => other
    }

    private def createBoundaryCast(boundary: Expression, dt: DataType): Expression = {
      (boundary, dt) match {
        case (e: SpecialFrameBoundary, _) => e
        case (e, _: DateType) => e
        case (e, _: TimestampType) => e
        case (e: Expression, t) if e.dataType != t && canCast(e.dataType, t) =>
          Cast(e, t)
        case _ => boundary
      }
    }
  }

  /**
   * Type coercion helper that matches against date-time expressions in order to type coerce
   * children to expected types.
   */
  object DateTimeOperationsTypeCoercion {
    def apply(expression: Expression): Expression = expression match {
      case d @ DateAdd(AnyTimestampTypeExpression(), _) =>
        d.copy(startDate = Cast(d.startDate, DateType))
      case d @ DateAdd(StringTypeExpression(), _) => d.copy(startDate = Cast(d.startDate, DateType))
      case d @ DateSub(AnyTimestampTypeExpression(), _) =>
        d.copy(startDate = Cast(d.startDate, DateType))
      case d @ DateSub(StringTypeExpression(), _) => d.copy(startDate = Cast(d.startDate, DateType))

      case s @ SubtractTimestamps(DateTypeExpression(), AnyTimestampTypeExpression(), _, _) =>
        s.copy(left = Cast(s.left, s.right.dataType))
      case s @ SubtractTimestamps(AnyTimestampTypeExpression(), DateTypeExpression(), _, _) =>
        s.copy(right = Cast(s.right, s.left.dataType))
      case s @ SubtractTimestamps(AnyTimestampTypeExpression(), AnyTimestampTypeExpression(), _, _)
          if s.left.dataType != s.right.dataType =>
        val newLeft = castIfNotSameType(s.left, TimestampNTZType)
        val newRight = castIfNotSameType(s.right, TimestampNTZType)
        s.copy(left = newLeft, right = newRight)

      case t @ TimeAdd(StringTypeExpression(), _, _) => t.copy(start = Cast(t.start, TimestampType))

      case other => other
    }
  }

  /**
   * ANSI type coercion helper that matches against date-time expressions in order to type coerce
   * children to expected types.
   */
  object AnsiDateTimeOperationsTypeCoercion {
    def apply(expression: Expression): Expression = expression match {
      case d @ DateAdd(AnyTimestampTypeExpression(), _) =>
        d.copy(startDate = Cast(d.startDate, DateType))
      case d @ DateSub(AnyTimestampTypeExpression(), _) =>
        d.copy(startDate = Cast(d.startDate, DateType))

      case s @ SubtractTimestamps(DateTypeExpression(), AnyTimestampTypeExpression(), _, _) =>
        s.copy(left = Cast(s.left, s.right.dataType))
      case s @ SubtractTimestamps(AnyTimestampTypeExpression(), DateTypeExpression(), _, _) =>
        s.copy(right = Cast(s.right, s.left.dataType))
      case s @ SubtractTimestamps(AnyTimestampTypeExpression(), AnyTimestampTypeExpression(), _, _)
          if s.left.dataType != s.right.dataType =>
        val newLeft = castIfNotSameType(s.left, TimestampNTZType)
        val newRight = castIfNotSameType(s.right, TimestampNTZType)
        s.copy(left = newLeft, right = newRight)

      case other => other
    }
  }
}
