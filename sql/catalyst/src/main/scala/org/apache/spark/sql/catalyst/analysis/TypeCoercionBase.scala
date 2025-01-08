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
import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  CaseWhen,
  Cast,
  Concat,
  Elt,
  Expression,
  MapZipWith,
  Stack,
  WindowSpecDefinition
}
import org.apache.spark.sql.catalyst.plans.logical.{
  Call,
  Except,
  Intersect,
  LogicalPlan,
  Project,
  Union,
  Unpivot
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure
import org.apache.spark.sql.types.DataType

abstract class TypeCoercionBase extends TypeCoercionHelper {

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
   * A type coercion rule that implicitly casts procedure arguments to expected types.
   */
  object ProcedureArgumentCoercion extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case c @ Call(ResolvedProcedure(_, _, procedure: BoundProcedure), args, _) if c.resolved =>
        val expectedDataTypes = procedure.parameters.map(_.dataType)
        val coercedArgs = args.zip(expectedDataTypes).map {
          case (arg, expectedType) => implicitCast(arg, expectedType).getOrElse(arg)
        }
        c.copy(args = coercedArgs)
    }
  }

  /**
   * Widens the data types of the [[Unpivot]] values.
   */
  object UnpivotCoercion extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case up: Unpivot if up.canBeCoercioned && !up.valuesTypeCoercioned =>
        // get wider data type of inner values at same idx
        val valueDataTypes = up.values.get.head.zipWithIndex.map {
          case (_, idx) => findWiderTypeWithoutStringPromotion(up.values.get.map(_(idx).dataType))
        }

        // cast inner values to type according to their idx
        val values = up.values.get.map(
          values =>
            values.zipWithIndex.map {
              case (value, idx) => (value, valueDataTypes(idx))
            } map {
              case (value, Some(valueType)) if value.dataType != valueType =>
                Alias(Cast(value, valueType), value.name)()
              case (value, _) => value
            }
        )

        up.copy(values = Some(values))
    }
  }

  /**
   * Widens the data types of the children of Union/Except/Intersect.
   * 1. When ANSI mode is off:
   *  Loosely based on rules from "Hadoop: The Definitive Guide" 2nd edition, by Tom White
   *
   *  The implicit conversion rules can be summarized as follows:
   *     - Any integral numeric type can be implicitly converted to a wider type.
   *     - All the integral numeric types, FLOAT, and (perhaps surprisingly) STRING can be
   *       implicitly converted to DOUBLE.
   *     - TINYINT, SMALLINT, and INT can all be converted to FLOAT.
   *     - BOOLEAN types cannot be converted to any other type.
   *     - Any integral numeric type can be implicitly converted to decimal type.
   *     - two different decimal types will be converted into a wider decimal type for both of them.
   *     - decimal type will be converted into double if there float or double together with it.
   *
   *  All types when UNION-ed with strings will be promoted to
   *  strings. Other string conversions are handled by PromoteStrings.
   *
   *  Widening types might result in loss of precision in the following cases:
   *   - IntegerType to FloatType
   *   - LongType to FloatType
   *   - LongType to DoubleType
   *   - DecimalType to Double
   *
   * 2. When ANSI mode is on:
   *  The implicit conversion is determined by the closest common data type from the precedent
   *  lists from left and right child. See the comments of Object `AnsiTypeCoercion` for details.
   */
  object WidenSetOperationTypes extends Rule[LogicalPlan] {

    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan resolveOperatorsUpWithNewOutput {
        case s @ Except(left, right, isAll)
            if s.childrenResolved &&
            left.output.length == right.output.length && !s.resolved =>
          val newChildren: Seq[LogicalPlan] = buildNewChildrenWithWiderTypes(left :: right :: Nil)
          if (newChildren.isEmpty) {
            s -> Nil
          } else {
            assert(newChildren.length == 2)
            val attrMapping = left.output.zip(newChildren.head.output)
            Except(newChildren.head, newChildren.last, isAll) -> attrMapping
          }

        case s @ Intersect(left, right, isAll)
            if s.childrenResolved &&
            left.output.length == right.output.length && !s.resolved =>
          val newChildren: Seq[LogicalPlan] = buildNewChildrenWithWiderTypes(left :: right :: Nil)
          if (newChildren.isEmpty) {
            s -> Nil
          } else {
            assert(newChildren.length == 2)
            val attrMapping = left.output.zip(newChildren.head.output)
            Intersect(newChildren.head, newChildren.last, isAll) -> attrMapping
          }

        case s: Union
            if s.childrenResolved && !s.byName &&
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
      case withChildrenResolved => InTypeCoercion(withChildrenResolved)
    }
  }

  /**
   * This ensure that the types for various functions are as expected.
   */
  object FunctionArgumentConversion extends TypeCoercionRule {

    override val transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e
      case withChildrenResolved => FunctionArgumentTypeCoercion(withChildrenResolved)
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
      case withChildrenResolved => DivisionTypeCoercion(withChildrenResolved)
    }
  }

  /**
   * The DIV operator always returns long-type value.
   * This rule cast the integral inputs to long type, to avoid overflow during calculation.
   */
  object IntegralDivision extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      case e if !e.childrenResolved => e
      case withChildrenResolved => IntegralDivisionTypeCoercion(withChildrenResolved)
    }
  }

  /**
   * Coerces the type of different branches of a CASE WHEN statement to a common type.
   */
  object CaseWhenCoercion extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      case c: CaseWhen if !c.childrenResolved => c
      case withChildrenResolved => CaseWhenTypeCoercion(withChildrenResolved)
    }
  }

  /**
   * Coerces the type of different branches of If statement to a common type.
   */
  object IfCoercion extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      case e if !e.childrenResolved => e
      case withChildrenResolved => IfTypeCoercion(withChildrenResolved)
    }
  }

  /**
   * Coerces NullTypes in the Stack expression to the column types of the corresponding positions.
   */
  object StackCoercion extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      case s: Stack if !s.childrenResolved => s
      case withChildrenResolved => StackTypeCoercion(withChildrenResolved)
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
      case c: Concat if !c.childrenResolved => c
      case withChildrenResolved => ConcatTypeCoercion(withChildrenResolved)
    }
  }

  /**
   * Coerces key types of two different [[MapType]] arguments of the [[MapZipWith]] expression
   * to a common type.
   */
  object MapZipWithCoercion extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      case m: MapZipWith if m.arguments.exists(a => !a.resolved) => m
      case withArgumentsResolved => MapZipWithTypeCoercion(withArgumentsResolved)
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
      case c: Elt if !c.childrenResolved => c
      case withChildrenResolved => EltTypeCoercion(withChildrenResolved)
    }
  }

  /**
   * Casts types according to the expected input types for [[Expression]]s.
   */
  object ImplicitTypeCasts extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e
      case withChildrenResolved => ImplicitTypeCoercion(withChildrenResolved)
    }
  }

  /**
   * Cast WindowFrame boundaries to the type they operate upon.
   */
  object WindowFrameCoercion extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      case s @ WindowSpecDefinition(_, Seq(order), _) if !order.resolved => s
      case withOrderResolved => WindowFrameTypeCoercion(withOrderResolved)
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
      case withChildrenResolved => StringLiteralTypeCoercion(withChildrenResolved)
    }
  }
}
