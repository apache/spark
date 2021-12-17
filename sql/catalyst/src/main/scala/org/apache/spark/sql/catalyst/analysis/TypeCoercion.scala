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
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

abstract class TypeCoercionBase {
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
        ArrayType(et, containsNull1 || containsNull2 ||
          Cast.forceNullable(et1, et) || Cast.forceNullable(et2, et))
      }
    case (MapType(kt1, vt1, valueContainsNull1), MapType(kt2, vt2, valueContainsNull2)) =>
      findTypeFunc(kt1, kt2)
        .filter { kt => !Cast.forceNullable(kt1, kt) && !Cast.forceNullable(kt2, kt) }
        .flatMap { kt =>
          findTypeFunc(vt1, vt2).map { vt =>
            MapType(kt, vt, valueContainsNull1 || valueContainsNull2 ||
              Cast.forceNullable(vt1, vt) || Cast.forceNullable(vt2, vt))
          }
      }
    case (StructType(fields1), StructType(fields2)) if fields1.length == fields2.length =>
      val resolver = SQLConf.get.resolver
      fields1.zip(fields2).foldLeft(Option(new StructType())) {
        case (Some(struct), (field1, field2)) if resolver(field1.name, field2.name) =>
          findTypeFunc(field1.dataType, field2.dataType).map { dt =>
            struct.add(field1.name, dt, field1.nullable || field2.nullable ||
              Cast.forceNullable(field1.dataType, dt) || Cast.forceNullable(field2.dataType, dt))
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
        Some(DecimalPrecision.widerDecimalType(t1, t2))
      case (t: IntegralType, d: DecimalType) =>
        Some(DecimalPrecision.widerDecimalType(DecimalType.forType(t), d))
      case (d: DecimalType, t: IntegralType) =>
        Some(DecimalPrecision.widerDecimalType(DecimalType.forType(t), d))
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
    types.foldLeft[Option[DataType]](Some(NullType))((r, c) => r match {
      case Some(d) => findWiderTypeWithoutStringPromotionForTwo(d, c)
      case None => None
    })
  }

  /**
   * Check whether the given types are equal ignoring nullable, containsNull and valueContainsNull.
   */
  def haveSameType(types: Seq[DataType]): Boolean = {
    if (types.size <= 1) {
      true
    } else {
      val head = types.head
      types.tail.forall(_.sameType(head))
    }
  }

  protected def castIfNotSameType(expr: Expression, dt: DataType): Expression = {
    if (!expr.dataType.sameType(dt)) {
      Cast(expr, dt)
    } else {
      expr
    }
  }

  /**
   * Type coercion rule that combines multiple type coercion rules and applies them in a single tree
   * traversal.
   */
  class CombinedTypeCoercionRule(rules: Seq[TypeCoercionRule]) extends TypeCoercionRule {
    override def transform: PartialFunction[Expression, Expression] = {
      val transforms = rules.map(_.transform)
      Function.unlift { e: Expression =>
        val result = transforms.foldLeft(e) {
          case (current, transform) => transform.applyOrElse(current, identity[Expression])
        }
        if (result ne e) {
          Some(result)
        } else {
          None
        }
      }
    }
  }

  /**
   * Widens the data types of the children of Union/Except/Intersect.
   * 1. When ANSI mode is off:
   *   Loosely based on rules from "Hadoop: The Definitive Guide" 2nd edition, by Tom White
   *
   *   The implicit conversion rules can be summarized as follows:
   *     - Any integral numeric type can be implicitly converted to a wider type.
   *     - All the integral numeric types, FLOAT, and (perhaps surprisingly) STRING can be
   *       implicitly converted to DOUBLE.
   *     - TINYINT, SMALLINT, and INT can all be converted to FLOAT.
   *     - BOOLEAN types cannot be converted to any other type.
   *     - Any integral numeric type can be implicitly converted to decimal type.
   *     - two different decimal types will be converted into a wider decimal type for both of them.
   *     - decimal type will be converted into double if there float or double together with it.
   *
   *   All types when UNION-ed with strings will be promoted to
   *   strings. Other string conversions are handled by PromoteStrings.
   *
   *   Widening types might result in loss of precision in the following cases:
   *   - IntegerType to FloatType
   *   - LongType to FloatType
   *   - LongType to DoubleType
   *   - DecimalType to Double
   *
   * 2. When ANSI mode is on:
   *   The implicit conversion is determined by the closest common data type from the precedent
   *   lists from left and right child. See the comments of Object `AnsiTypeCoercion` for details.
   */
  object WidenSetOperationTypes extends Rule[LogicalPlan] {

    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan resolveOperatorsUpWithNewOutput {
        case s @ Except(left, right, isAll) if s.childrenResolved &&
          left.output.length == right.output.length && !s.resolved =>
          val newChildren: Seq[LogicalPlan] = buildNewChildrenWithWiderTypes(left :: right :: Nil)
          if (newChildren.isEmpty) {
            s -> Nil
          } else {
            assert(newChildren.length == 2)
            val attrMapping = left.output.zip(newChildren.head.output)
            Except(newChildren.head, newChildren.last, isAll) -> attrMapping
          }

        case s @ Intersect(left, right, isAll) if s.childrenResolved &&
          left.output.length == right.output.length && !s.resolved =>
          val newChildren: Seq[LogicalPlan] = buildNewChildrenWithWiderTypes(left :: right :: Nil)
          if (newChildren.isEmpty) {
            s -> Nil
          } else {
            assert(newChildren.length == 2)
            val attrMapping = left.output.zip(newChildren.head.output)
            Intersect(newChildren.head, newChildren.last, isAll) -> attrMapping
          }

        case s: Union if s.childrenResolved && !s.byName &&
          s.children.forall(_.output.length == s.children.head.output.length) && !s.resolved =>
          val newChildren: Seq[LogicalPlan] = buildNewChildrenWithWiderTypes(s.children)

          if (newChildren.isEmpty) {
            s -> Nil
          } else {
            val attrMapping = s.children.head.output.zip(newChildren.head.output)
            s.copy(children = newChildren) -> attrMapping
          }
      }
    }

    /** Build new children with the widest types for each attribute among all the children */
    private def buildNewChildrenWithWiderTypes(children: Seq[LogicalPlan]): Seq[LogicalPlan] = {
      require(children.forall(_.output.length == children.head.output.length))

      // Get a sequence of data types, each of which is the widest type of this specific attribute
      // in all the children
      val targetTypes: Seq[Option[DataType]] =
        getWidestTypes(children, attrIndex = 0, mutable.Queue[Option[DataType]]())

      if (targetTypes.exists(_.isDefined)) {
        // Add an extra Project if the targetTypes are different from the original types.
        children.map(widenTypes(_, targetTypes))
      } else {
        Nil
      }
    }

    /** Get the widest type for each attribute in all the children */
    @tailrec private def getWidestTypes(
        children: Seq[LogicalPlan],
        attrIndex: Int,
        castedTypes: mutable.Queue[Option[DataType]]): Seq[Option[DataType]] = {
      // Return the result after the widen data types have been found for all the children
      if (attrIndex >= children.head.output.length) return castedTypes.toSeq

      // For the attrIndex-th attribute, find the widest type
      val widenTypeOpt = findWiderCommonType(children.map(_.output(attrIndex).dataType))
      castedTypes.enqueue(widenTypeOpt)
      getWidestTypes(children, attrIndex + 1, castedTypes)
    }

    /** Given a plan, add an extra project on top to widen some columns' data types. */
    private def widenTypes(plan: LogicalPlan, targetTypes: Seq[Option[DataType]]): LogicalPlan = {
      var changed = false
      val casted = plan.output.zip(targetTypes).map {
        case (e, Some(dt)) if e.dataType != dt =>
          changed = true
          Alias(Cast(e, dt, Some(conf.sessionLocalTimeZone)), e.name)()
        case (e, _) => e
      }
      if (changed) {
        Project(casted, plan)
      } else {
        plan
      }
    }
  }

  /**
   * Handles type coercion for both IN expression with subquery and IN
   * expressions without subquery.
   * 1. In the first case, find the common type by comparing the left hand side (LHS)
   *    expression types against corresponding right hand side (RHS) expression derived
   *    from the subquery expression's plan output. Inject appropriate casts in the
   *    LHS and RHS side of IN expression.
   *
   * 2. In the second case, convert the value and in list expressions to the
   *    common operator type by looking at all the argument types and finding
   *    the closest one that all the arguments can be cast to. When no common
   *    operator type is found the original expression will be returned and an
   *    Analysis Exception will be raised at the type checking phase.
   */
  object InConversion extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      // Handle type casting required between value expression and subquery output
      // in IN subquery.
      case i @ InSubquery(lhs, ListQuery(sub, children, exprId, _, conditions))
          if !i.resolved && lhs.length == sub.output.length =>
        // LHS is the value expressions of IN subquery.
        // RHS is the subquery output.
        val rhs = sub.output

        val commonTypes = lhs.zip(rhs).flatMap { case (l, r) =>
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

          val newSub = Project(castedRhs, sub)
          InSubquery(newLhs, ListQuery(newSub, children, exprId, newSub.output, conditions))
        } else {
          i
        }

      case i @ In(a, b) if b.exists(_.dataType != a.dataType) =>
        findWiderCommonType(i.children.map(_.dataType)) match {
          case Some(finalDataType) => i.withNewChildren(i.children.map(Cast(_, finalDataType)))
          case None => i
        }
    }
  }

  /**
   * This ensure that the types for various functions are as expected.
   */
  object FunctionArgumentConversion extends TypeCoercionRule {

    override val transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case a @ CreateArray(children, _) if !haveSameType(children.map(_.dataType)) =>
        val types = children.map(_.dataType)
        findWiderCommonType(types) match {
          case Some(finalDataType) => a.copy(children.map(castIfNotSameType(_, finalDataType)))
          case None => a
        }

      case c @ Concat(children) if children.forall(c => ArrayType.acceptsType(c.dataType)) &&
        !haveSameType(c.inputTypesForMerging) =>
        val types = children.map(_.dataType)
        findWiderCommonType(types) match {
          case Some(finalDataType) => Concat(children.map(castIfNotSameType(_, finalDataType)))
          case None => c
        }

      case aj @ ArrayJoin(arr, d, nr) if !ArrayType(StringType).acceptsType(arr.dataType) &&
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

      case m @ MapConcat(children) if children.forall(c => MapType.acceptsType(c.dataType)) &&
          !haveSameType(m.inputTypesForMerging) =>
        val types = children.map(_.dataType)
        findWiderCommonType(types) match {
          case Some(finalDataType) => MapConcat(children.map(castIfNotSameType(_, finalDataType)))
          case None => m
        }

      case m @ CreateMap(children, _) if m.keys.length == m.values.length &&
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
      case Sum(e @ TimestampType(), _) => Sum(Cast(e, DoubleType))
      case Average(e @ TimestampType(), _) => Average(Cast(e, DoubleType))

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
    }
  }

  /**
   * Hive only performs integral division with the DIV operator. The arguments to / are always
   * converted to fractional types.
   */
  object Division extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who has not been resolved yet,
      // as this is an extra rule which should be applied at last.
      case e if !e.childrenResolved => e

      // Decimal and Double remain the same
      case d: Divide if d.dataType == DoubleType => d
      case d: Divide if d.dataType.isInstanceOf[DecimalType] => d
      case d @ Divide(left, right, _) if isNumericOrNull(left) && isNumericOrNull(right) =>
        d.copy(left = Cast(left, DoubleType), right = Cast(right, DoubleType))
    }

    private def isNumericOrNull(ex: Expression): Boolean = {
      // We need to handle null types in case a query contains null literals.
      ex.dataType.isInstanceOf[NumericType] || ex.dataType == NullType
    }
  }

  /**
   * The DIV operator always returns long-type value.
   * This rule cast the integral inputs to long type, to avoid overflow during calculation.
   */
  object IntegralDivision extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      case e if !e.childrenResolved => e
      case d @ IntegralDivide(left, right, _) =>
        d.copy(left = mayCastToLong(left), right = mayCastToLong(right))
    }

    private def mayCastToLong(expr: Expression): Expression = expr.dataType match {
      case _: ByteType | _: ShortType | _: IntegerType => Cast(expr, LongType)
      case _ => expr
    }
  }

  /**
   * Coerces the type of different branches of a CASE WHEN statement to a common type.
   */
  object CaseWhenCoercion extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      case c: CaseWhen if c.childrenResolved && !haveSameType(c.inputTypesForMerging) =>
        val maybeCommonType = findWiderCommonType(c.inputTypesForMerging)
        maybeCommonType.map { commonType =>
          val newBranches = c.branches.map { case (condition, value) =>
            (condition, castIfNotSameType(value, commonType))
          }
          val newElseValue = c.elseValue.map(castIfNotSameType(_, commonType))
          CaseWhen(newBranches, newElseValue)
        }.getOrElse(c)
    }
  }

  /**
   * Coerces the type of different branches of If statement to a common type.
   */
  object IfCoercion extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      case e if !e.childrenResolved => e
      // Find tightest common type for If, if the true value and false value have different types.
      case i @ If(pred, left, right) if !haveSameType(i.inputTypesForMerging) =>
        findWiderTypeForTwo(left.dataType, right.dataType).map { widestType =>
          val newLeft = castIfNotSameType(left, widestType)
          val newRight = castIfNotSameType(right, widestType)
          If(pred, newLeft, newRight)
        }.getOrElse(i)  // If there is no applicable conversion, leave expression unchanged.
      case If(Literal(null, NullType), left, right) =>
        If(Literal.create(null, BooleanType), left, right)
      case If(pred, left, right) if pred.dataType == NullType =>
        If(Cast(pred, BooleanType), left, right)
    }
  }

  /**
   * Coerces NullTypes in the Stack expression to the column types of the corresponding positions.
   */
  object StackCoercion extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      case s @ Stack(children) if s.childrenResolved && s.hasFoldableNumRows =>
        Stack(children.zipWithIndex.map {
          // The first child is the number of rows for stack.
          case (e, 0) => e
          case (Literal(null, NullType), index: Int) =>
            Literal.create(null, s.findDataType(index))
          case (e, _) => e
        })
    }
  }

  /**
   * Coerces the types of [[Concat]] children to expected ones.
   *
   * If `spark.sql.function.concatBinaryAsString` is false and all children types are binary,
   * the expected types are binary. Otherwise, the expected ones are strings.
   */
  object ConcatCoercion extends TypeCoercionRule {

    override val transform: PartialFunction[Expression, Expression] = {
      // Skip nodes if unresolved or empty children
      case c @ Concat(children) if !c.childrenResolved || children.isEmpty => c
      case c @ Concat(children) if conf.concatBinaryAsString ||
        !children.map(_.dataType).forall(_ == BinaryType) =>
        val newChildren = c.children.map { e =>
          implicitCast(e, StringType).getOrElse(e)
        }
        c.copy(children = newChildren)
    }
  }

  /**
   * Coerces key types of two different [[MapType]] arguments of the [[MapZipWith]] expression
   * to a common type.
   */
  object MapZipWithCoercion extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      // Lambda function isn't resolved when the rule is executed.
      case m @ MapZipWith(left, right, function) if m.arguments.forall(a => a.resolved &&
          MapType.acceptsType(a.dataType)) && !m.leftKeyType.sameType(m.rightKeyType) =>
        findWiderTypeForTwo(m.leftKeyType, m.rightKeyType) match {
          case Some(finalKeyType) if !Cast.forceNullable(m.leftKeyType, finalKeyType) &&
              !Cast.forceNullable(m.rightKeyType, finalKeyType) =>
            val newLeft = castIfNotSameType(
              left,
              MapType(finalKeyType, m.leftValueType, m.leftValueContainsNull))
            val newRight = castIfNotSameType(
              right,
              MapType(finalKeyType, m.rightValueType, m.rightValueContainsNull))
            MapZipWith(newLeft, newRight, function)
          case _ => m
        }
    }
  }

  /**
   * Coerces the types of [[Elt]] children to expected ones.
   *
   * If `spark.sql.function.eltOutputAsString` is false and all children types are binary,
   * the expected types are binary. Otherwise, the expected ones are strings.
   */
  object EltCoercion extends TypeCoercionRule {

    override val transform: PartialFunction[Expression, Expression] = {
      // Skip nodes if unresolved or not enough children
      case c @ Elt(children, _) if !c.childrenResolved || children.size < 2 => c
      case c @ Elt(children, _) =>
        val index = children.head
        val newIndex = implicitCast(index, IntegerType).getOrElse(index)
        val newInputs = if (conf.eltOutputAsString ||
          !children.tail.map(_.dataType).forall(_ == BinaryType)) {
          children.tail.map { e =>
            implicitCast(e, StringType).getOrElse(e)
          }
        } else {
          children.tail
        }
        c.copy(children = newIndex +: newInputs)
    }
  }

  /**
   * Casts types according to the expected input types for [[Expression]]s.
   */
  object ImplicitTypeCasts extends TypeCoercionRule {

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

    override val transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case b @ BinaryOperator(left, right)
          if canHandleTypeCoercion(left.dataType, right.dataType) =>
        findTightestCommonType(left.dataType, right.dataType).map { commonType =>
          if (b.inputType.acceptsType(commonType)) {
            // If the expression accepts the tightest common type, cast to that.
            val newLeft = if (left.dataType == commonType) left else Cast(left, commonType)
            val newRight = if (right.dataType == commonType) right else Cast(right, commonType)
            b.withNewChildren(Seq(newLeft, newRight))
          } else {
            // Otherwise, don't do anything with the expression.
            b
          }
        }.getOrElse(b)  // If there is no applicable conversion, leave expression unchanged.

      case e: ImplicitCastInputTypes if e.inputTypes.nonEmpty =>
        val children: Seq[Expression] = e.children.zip(e.inputTypes).map { case (in, expected) =>
          // If we cannot do the implicit cast, just use the original input.
          implicitCast(in, expected).getOrElse(in)
        }
        e.withNewChildren(children)

      case e: ExpectsInputTypes if e.inputTypes.nonEmpty =>
        // Convert NullType into some specific target type for ExpectsInputTypes that don't do
        // general implicit casting.
        val children: Seq[Expression] = e.children.zip(e.inputTypes).map { case (in, expected) =>
          if (in.dataType == NullType && !expected.acceptsType(NullType)) {
            Literal.create(null, expected.defaultConcreteType)
          } else {
            in
          }
        }
        e.withNewChildren(children)

      case udf: ScalaUDF if udf.inputTypes.nonEmpty =>
        val children = udf.children.zip(udf.inputTypes).map { case (in, expected) =>
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
          MapType(udfInputToCastType(keyDtIn, keyDtExp),
            udfInputToCastType(valueDtIn, valueDtExp),
            nullableExp)
        case (StructType(fieldsIn), StructType(fieldsExp)) =>
          val fieldTypes =
            fieldsIn.map(_.dataType).zip(fieldsExp.map(_.dataType)).map { case (dtIn, dtExp) =>
              udfInputToCastType(dtIn, dtExp)
            }
          StructType(fieldsExp.zip(fieldTypes).map { case (field, newDt) =>
            field.copy(dataType = newDt)
          })
        case (_, other) => other
      }
    }
  }

  /**
   * Cast WindowFrame boundaries to the type they operate upon.
   */
  object WindowFrameCoercion extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      case s @ WindowSpecDefinition(_, Seq(order), SpecifiedWindowFrame(RangeFrame, lower, upper))
          if order.resolved =>
        s.copy(frameSpecification = SpecifiedWindowFrame(
          RangeFrame,
          createBoundaryCast(lower, order.dataType),
          createBoundaryCast(upper, order.dataType)))
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
   * A special rule to support string literal as the second argument of date_add/date_sub functions,
   * to keep backward compatibility as a temporary workaround.
   * TODO(SPARK-28589): implement ANSI type type coercion and handle string literals.
   */
  object StringLiteralCoercion extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e
      case DateAdd(l, r) if r.dataType == StringType && r.foldable =>
        val days = try {
          AnsiCast(r, IntegerType).eval().asInstanceOf[Int]
        } catch {
          case e: NumberFormatException =>
            throw QueryCompilationErrors.secondArgumentOfFunctionIsNotIntegerError("date_add", e)
        }
        DateAdd(l, Literal(days))
      case DateSub(l, r) if r.dataType == StringType && r.foldable =>
        val days = try {
          AnsiCast(r, IntegerType).eval().asInstanceOf[Int]
        } catch {
          case e: NumberFormatException =>
            throw QueryCompilationErrors.secondArgumentOfFunctionIsNotIntegerError("date_sub", e)
        }
        DateSub(l, Literal(days))
    }
  }
}

/**
 * A collection of [[Rule]] that can be used to coerce differing types that participate in
 * operations into compatible ones.
 *
 * Notes about type widening / tightest common types: Broadly, there are two cases when we need
 * to widen data types (e.g. union, binary comparison). In case 1, we are looking for a common
 * data type for two or more data types, and in this case no loss of precision is allowed. Examples
 * include type inference in JSON (e.g. what's the column's data type if one row is an integer
 * while the other row is a long?). In case 2, we are looking for a widened data type with
 * some acceptable loss of precision (e.g. there is no common type for double and decimal because
 * double's range is larger than decimal, and yet decimal is more precise than double, but in
 * union we would cast the decimal into double).
 */
object TypeCoercion extends TypeCoercionBase {

  override def typeCoercionRules: List[Rule[LogicalPlan]] =
    WidenSetOperationTypes ::
    new CombinedTypeCoercionRule(
      InConversion ::
      PromoteStrings ::
      DecimalPrecision ::
      BooleanEquality ::
      FunctionArgumentConversion ::
      ConcatCoercion ::
      MapZipWithCoercion ::
      EltCoercion ::
      CaseWhenCoercion ::
      IfCoercion ::
      StackCoercion ::
      Division ::
      IntegralDivision ::
      ImplicitTypeCasts ::
      DateTimeOperations ::
      WindowFrameCoercion ::
      StringLiteralCoercion :: Nil) :: Nil

  override def canCast(from: DataType, to: DataType): Boolean = Cast.canCast(from, to)

  // See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types.
  // The conversion for integral and floating point types have a linear widening hierarchy:
  val numericPrecedence =
    IndexedSeq(
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType)

  override val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
      case (t1, t2) if t1 == t2 => Some(t1)
      case (NullType, t1) => Some(t1)
      case (t1, NullType) => Some(t1)

      case (t1: IntegralType, t2: DecimalType) if t2.isWiderThan(t1) =>
        Some(t2)
      case (t1: DecimalType, t2: IntegralType) if t1.isWiderThan(t2) =>
        Some(t1)

      // Promote numeric types to the highest of the two
      case (t1: NumericType, t2: NumericType)
          if !t1.isInstanceOf[DecimalType] && !t2.isInstanceOf[DecimalType] =>
        val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
        Some(numericPrecedence(index))

      case (_: TimestampType, _: DateType) | (_: DateType, _: TimestampType) =>
        Some(TimestampType)

      case (t1: DayTimeIntervalType, t2: DayTimeIntervalType) =>
        Some(DayTimeIntervalType(t1.startField.min(t2.startField), t1.endField.max(t2.endField)))
      case (t1: YearMonthIntervalType, t2: YearMonthIntervalType) =>
        Some(YearMonthIntervalType(t1.startField.min(t2.startField), t1.endField.max(t2.endField)))

      case (_: TimestampNTZType, _: DateType) | (_: DateType, _: TimestampNTZType) =>
        Some(TimestampNTZType)

      case (t1, t2) => findTypeForComplex(t1, t2, findTightestCommonType)
  }

  /** Promotes all the way to StringType. */
  private def stringPromotion(dt1: DataType, dt2: DataType): Option[DataType] = (dt1, dt2) match {
    case (StringType, t2: AtomicType) if t2 != BinaryType && t2 != BooleanType => Some(StringType)
    case (t1: AtomicType, StringType) if t1 != BinaryType && t1 != BooleanType => Some(StringType)
    case _ => None
  }

  // Return whether a string literal can be promoted as the give data type in a binary comparison.
  private def canPromoteAsInBinaryComparison(dt: DataType) = dt match {
    // If a binary comparison contains interval type and string type, we can't decide which
    // interval type the string should be promoted as. There are many possible interval
    // types, such as year interval, month interval, day interval, hour interval, etc.
    case _: YearMonthIntervalType | _: DayTimeIntervalType => false
    // There is no need to add `Cast` for comparison between strings.
    case _: StringType => false
    case _: AtomicType => true
    case _ => false
  }

  /**
   * This function determines the target type of a comparison operator when one operand
   * is a String and the other is not. It also handles when one op is a Date and the
   * other is a Timestamp by making the target type to be String.
   */
  def findCommonTypeForBinaryComparison(
      dt1: DataType, dt2: DataType, conf: SQLConf): Option[DataType] = (dt1, dt2) match {
    case (StringType, DateType)
      => if (conf.castDatetimeToString) Some(StringType) else Some(DateType)
    case (DateType, StringType)
      => if (conf.castDatetimeToString) Some(StringType) else Some(DateType)
    case (StringType, TimestampType)
      => if (conf.castDatetimeToString) Some(StringType) else Some(TimestampType)
    case (TimestampType, StringType)
      => if (conf.castDatetimeToString) Some(StringType) else Some(TimestampType)
    case (StringType, NullType) => Some(StringType)
    case (NullType, StringType) => Some(StringType)

    // Cast to TimestampType when we compare DateType with TimestampType
    // i.e. TimeStamp('2017-03-01 00:00:00') eq Date('2017-03-01') = true
    case (TimestampType, DateType) => Some(TimestampType)
    case (DateType, TimestampType) => Some(TimestampType)

    // There is no proper decimal type we can pick,
    // using double type is the best we can do.
    // See SPARK-22469 for details.
    case (n: DecimalType, s: StringType) => Some(DoubleType)
    case (s: StringType, n: DecimalType) => Some(DoubleType)

    case (l: StringType, r: AtomicType) if canPromoteAsInBinaryComparison(r) => Some(r)
    case (l: AtomicType, r: StringType) if canPromoteAsInBinaryComparison(l) => Some(l)
    case (l, r) => None
  }

  override def findWiderTypeForTwo(t1: DataType, t2: DataType): Option[DataType] = {
    findTightestCommonType(t1, t2)
      .orElse(findWiderTypeForDecimal(t1, t2))
      .orElse(stringPromotion(t1, t2))
      .orElse(findTypeForComplex(t1, t2, findWiderTypeForTwo))
  }

  override def findWiderCommonType(types: Seq[DataType]): Option[DataType] = {
    // findWiderTypeForTwo doesn't satisfy the associative law, i.e. (a op b) op c may not equal
    // to a op (b op c). This is only a problem for StringType or nested StringType in ArrayType.
    // Excluding these types, findWiderTypeForTwo satisfies the associative law. For instance,
    // (TimestampType, IntegerType, StringType) should have StringType as the wider common type.
    val (stringTypes, nonStringTypes) = types.partition(hasStringType(_))
    (stringTypes.distinct ++ nonStringTypes).foldLeft[Option[DataType]](Some(NullType))((r, c) =>
      r match {
        case Some(d) => findWiderTypeForTwo(d, c)
        case _ => None
      })
  }

  override def implicitCast(e: Expression, expectedType: AbstractDataType): Option[Expression] = {
    implicitCast(e.dataType, expectedType).map { dt =>
      if (dt == e.dataType) e else Cast(e, dt)
    }
  }

  private def implicitCast(inType: DataType, expectedType: AbstractDataType): Option[DataType] = {
    // Note that ret is nullable to avoid typing a lot of Some(...) in this local scope.
    // We wrap immediately an Option after this.
    @Nullable val ret: DataType = (inType, expectedType) match {
      // If the expected type is already a parent of the input type, no need to cast.
      case _ if expectedType.acceptsType(inType) => inType

      // Cast null type (usually from null literals) into target types
      case (NullType, target) => target.defaultConcreteType

      // If the function accepts any numeric type and the input is a string, we follow the hive
      // convention and cast that input into a double
      case (StringType, NumericType) => NumericType.defaultConcreteType

      // Implicit cast among numeric types. When we reach here, input type is not acceptable.

      // If input is a numeric type but not decimal, and we expect a decimal type,
      // cast the input to decimal.
      case (d: NumericType, DecimalType) => DecimalType.forType(d)
      // For any other numeric types, implicitly cast to each other, e.g. long -> int, int -> long
      case (_: NumericType, target: NumericType) => target

      // Implicit cast between date time types
      case (DateType, TimestampType) => TimestampType
      case (DateType, AnyTimestampType) => AnyTimestampType.defaultConcreteType
      case (TimestampType | TimestampNTZType, DateType) => DateType

      // Implicit cast from/to string
      case (StringType, DecimalType) => DecimalType.SYSTEM_DEFAULT
      case (StringType, target: NumericType) => target
      case (StringType, DateType) => DateType
      case (StringType, TimestampType) => TimestampType
      case (StringType, AnyTimestampType) => AnyTimestampType.defaultConcreteType
      case (StringType, BinaryType) => BinaryType
      // Cast any atomic type to string.
      case (any: AtomicType, StringType) if any != StringType => StringType

      // When we reach here, input type is not acceptable for any types in this type collection,
      // try to find the first one we can implicitly cast.
      case (_, TypeCollection(types)) =>
        types.flatMap(implicitCast(inType, _)).headOption.orNull

      // Implicit cast between array types.
      //
      // Compare the nullabilities of the from type and the to type, check whether the cast of
      // the nullability is resolvable by the following rules:
      // 1. If the nullability of the to type is true, the cast is always allowed;
      // 2. If the nullability of the to type is false, and the nullability of the from type is
      // true, the cast is never allowed;
      // 3. If the nullabilities of both the from type and the to type are false, the cast is
      // allowed only when Cast.forceNullable(fromType, toType) is false.
      case (ArrayType(fromType, fn), ArrayType(toType: DataType, true)) =>
        implicitCast(fromType, toType).map(ArrayType(_, true)).orNull

      case (ArrayType(fromType, true), ArrayType(toType: DataType, false)) => null

      case (ArrayType(fromType, false), ArrayType(toType: DataType, false))
          if !Cast.forceNullable(fromType, toType) =>
        implicitCast(fromType, toType).map(ArrayType(_, false)).orNull

      // Implicit cast between Map types.
      // Follows the same semantics of implicit casting between two array types.
      // Refer to documentation above. Make sure that both key and values
      // can not be null after the implicit cast operation by calling forceNullable
      // method.
      case (MapType(fromKeyType, fromValueType, fn), MapType(toKeyType, toValueType, tn))
          if !Cast.forceNullable(fromKeyType, toKeyType) && Cast.resolvableNullability(fn, tn) =>
        if (Cast.forceNullable(fromValueType, toValueType) && !tn) {
          null
        } else {
          val newKeyType = implicitCast(fromKeyType, toKeyType).orNull
          val newValueType = implicitCast(fromValueType, toValueType).orNull
          if (newKeyType != null && newValueType != null) {
            MapType(newKeyType, newValueType, tn)
          } else {
            null
          }
        }

      case _ => null
    }
    Option(ret)
  }

  /**
   * The method finds a common type for data types that differ only in nullable flags, including
   * `nullable`, `containsNull` of [[ArrayType]] and `valueContainsNull` of [[MapType]].
   * If the input types are different besides nullable flags, None is returned.
   */
  def findCommonTypeDifferentOnlyInNullFlags(t1: DataType, t2: DataType): Option[DataType] = {
    if (t1 == t2) {
      Some(t1)
    } else {
      findTypeForComplex(t1, t2, findCommonTypeDifferentOnlyInNullFlags)
    }
  }

  def findCommonTypeDifferentOnlyInNullFlags(types: Seq[DataType]): Option[DataType] = {
    if (types.isEmpty) {
      None
    } else {
      types.tail.foldLeft[Option[DataType]](Some(types.head)) {
        case (Some(t1), t2) => findCommonTypeDifferentOnlyInNullFlags(t1, t2)
        case _ => None
      }
    }
  }

  /**
   * Whether the data type contains StringType.
   */
  def hasStringType(dt: DataType): Boolean = dt match {
    case StringType => true
    case ArrayType(et, _) => hasStringType(et)
    // Add StructType if we support string promotion for struct fields in the future.
    case _ => false
  }

  /**
   * Promotes strings that appear in arithmetic expressions.
   */
  object PromoteStrings extends TypeCoercionRule {
    private def castExpr(expr: Expression, targetType: DataType): Expression = {
      (expr.dataType, targetType) match {
        case (NullType, dt) => Literal.create(null, targetType)
        case (l, dt) if (l != dt) => Cast(expr, targetType)
        case _ => expr
      }
    }

    override def transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case a @ BinaryArithmetic(left @ StringType(), right)
        if right.dataType != CalendarIntervalType =>
        a.makeCopy(Array(Cast(left, DoubleType), right))
      case a @ BinaryArithmetic(left, right @ StringType())
        if left.dataType != CalendarIntervalType =>
        a.makeCopy(Array(left, Cast(right, DoubleType)))

      // For equality between string and timestamp we cast the string to a timestamp
      // so that things like rounding of subsecond precision does not affect the comparison.
      case p @ Equality(left @ StringType(), right @ TimestampType()) =>
        p.makeCopy(Array(Cast(left, TimestampType), right))
      case p @ Equality(left @ TimestampType(), right @ StringType()) =>
        p.makeCopy(Array(left, Cast(right, TimestampType)))

      case p @ BinaryComparison(left, right)
          if findCommonTypeForBinaryComparison(left.dataType, right.dataType, conf).isDefined =>
        val commonType = findCommonTypeForBinaryComparison(left.dataType, right.dataType, conf).get
        p.makeCopy(Array(castExpr(left, commonType), castExpr(right, commonType)))
    }
  }

  /**
   * Changes numeric values to booleans so that expressions like true = 1 can be evaluated.
   */
  object BooleanEquality extends TypeCoercionRule {
    private val trueValues = Seq(1.toByte, 1.toShort, 1, 1L, Decimal.ONE)
    private val falseValues = Seq(0.toByte, 0.toShort, 0, 0L, Decimal.ZERO)

    override def transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      // Hive treats (true = 1) as true and (false = 0) as true,
      // all other cases are considered as false.

      // We may simplify the expression if one side is literal numeric values
      // TODO: Maybe these rules should go into the optimizer.
      case EqualTo(bool @ BooleanType(), Literal(value, _: NumericType))
        if trueValues.contains(value) => bool
      case EqualTo(bool @ BooleanType(), Literal(value, _: NumericType))
        if falseValues.contains(value) => Not(bool)
      case EqualTo(Literal(value, _: NumericType), bool @ BooleanType())
        if trueValues.contains(value) => bool
      case EqualTo(Literal(value, _: NumericType), bool @ BooleanType())
        if falseValues.contains(value) => Not(bool)
      case EqualNullSafe(bool @ BooleanType(), Literal(value, _: NumericType))
        if trueValues.contains(value) => And(IsNotNull(bool), bool)
      case EqualNullSafe(bool @ BooleanType(), Literal(value, _: NumericType))
        if falseValues.contains(value) => And(IsNotNull(bool), Not(bool))
      case EqualNullSafe(Literal(value, _: NumericType), bool @ BooleanType())
        if trueValues.contains(value) => And(IsNotNull(bool), bool)
      case EqualNullSafe(Literal(value, _: NumericType), bool @ BooleanType())
        if falseValues.contains(value) => And(IsNotNull(bool), Not(bool))

      case EqualTo(left @ BooleanType(), right @ NumericType()) =>
        EqualTo(Cast(left, right.dataType), right)
      case EqualTo(left @ NumericType(), right @ BooleanType()) =>
        EqualTo(left, Cast(right, left.dataType))
      case EqualNullSafe(left @ BooleanType(), right @ NumericType()) =>
        EqualNullSafe(Cast(left, right.dataType), right)
      case EqualNullSafe(left @ NumericType(), right @ BooleanType()) =>
        EqualNullSafe(left, Cast(right, left.dataType))
    }
  }

  object DateTimeOperations extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e
      case d @ DateAdd(AnyTimestampType(), _) => d.copy(startDate = Cast(d.startDate, DateType))
      case d @ DateAdd(StringType(), _) => d.copy(startDate = Cast(d.startDate, DateType))
      case d @ DateSub(AnyTimestampType(), _) => d.copy(startDate = Cast(d.startDate, DateType))
      case d @ DateSub(StringType(), _) => d.copy(startDate = Cast(d.startDate, DateType))

      case s @ SubtractTimestamps(DateType(), AnyTimestampType(), _, _) =>
        s.copy(left = Cast(s.left, s.right.dataType))
      case s @ SubtractTimestamps(AnyTimestampType(), DateType(), _, _) =>
        s.copy(right = Cast(s.right, s.left.dataType))
      case s @ SubtractTimestamps(AnyTimestampType(), AnyTimestampType(), _, _)
        if s.left.dataType != s.right.dataType =>
        val newLeft = castIfNotSameType(s.left, TimestampNTZType)
        val newRight = castIfNotSameType(s.right, TimestampNTZType)
        s.copy(left = newLeft, right = newRight)

      case t @ TimeAdd(StringType(), _, _) => t.copy(start = Cast(t.start, TimestampType))
    }
  }

}

trait TypeCoercionRule extends Rule[LogicalPlan] with Logging {
  /**
   * Applies any changes to [[AttributeReference]] data types that are made by the transform method
   * to instances higher in the query tree.
   */
  def apply(plan: LogicalPlan): LogicalPlan = {
    val typeCoercionFn = transform
    plan.transformUpWithBeforeAndAfterRuleOnChildren(!_.analyzed, ruleId) {
      case (beforeMapChildren, afterMapChildren) =>
        if (!afterMapChildren.childrenResolved) {
          afterMapChildren
        } else {
          // Only propagate types if the children have changed.
          val withPropagatedTypes = if (beforeMapChildren ne afterMapChildren) {
            propagateTypes(afterMapChildren)
          } else {
            beforeMapChildren
          }
          withPropagatedTypes.transformExpressionsUp(typeCoercionFn)
        }
    }
  }

  def transform: PartialFunction[Expression, Expression]

  private def propagateTypes(plan: LogicalPlan): LogicalPlan = {
    // Check if the inputs have changed.
    val references = AttributeMap(plan.references.collect {
      case a if a.resolved => a -> a
    }.toSeq)
    def sameButDifferent(a: Attribute): Boolean = {
      references.get(a).exists(b => b.dataType != a.dataType || b.nullable != a.nullable)
    }
    val inputMap = AttributeMap(plan.inputSet.collect {
      case a if a.resolved && sameButDifferent(a) => a -> a
    }.toSeq)
    if (inputMap.isEmpty) {
      // Nothing changed.
      plan
    } else {
      // Update the references if the dataType/nullability has changed.
      plan transformExpressions {
        case a: AttributeReference =>
          inputMap.getOrElse(a, a)
      }
    }
  }
}
