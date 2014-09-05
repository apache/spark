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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.types.{DataType, FractionalType, IntegralType, NumericType, NativeType}

abstract class Expression extends TreeNode[Expression] {
  self: Product =>

  /** The narrowest possible type that is produced when this expression is evaluated. */
  type EvaluatedType <: Any

  /**
   * Returns true when an expression is a candidate for static evaluation before the query is
   * executed.
   *
   * The following conditions are used to determine suitability for constant folding:
   *  - A [[Coalesce]] is foldable if all of its children are foldable
   *  - A [[BinaryExpression]] is foldable if its both left and right child are foldable
   *  - A [[Not]], [[IsNull]], or [[IsNotNull]] is foldable if its child is foldable
   *  - A [[Literal]] is foldable
   *  - A [[Cast]] or [[UnaryMinus]] is foldable if its child is foldable
   */
  def foldable: Boolean = false
  def nullable: Boolean
  def references: AttributeSet = AttributeSet(children.flatMap(_.references.iterator))

  /** Returns the result of evaluating this expression on a given input Row */
  def eval(input: Row = null): EvaluatedType

  /**
   * Returns `true` if this expression and all its children have been resolved to a specific schema
   * and `false` if it still contains any unresolved placeholders. Implementations of expressions
   * should override this if the resolution of this type of expression involves more than just
   * the resolution of its children.
   */
  lazy val resolved: Boolean = childrenResolved

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  def dataType: DataType

  /**
   * Returns true if  all the children of this expression have been resolved to a specific schema
   * and false if any still contains any unresolved placeholders.
   */
  def childrenResolved = !children.exists(!_.resolved)

  /**
   * A set of helper functions that return the correct descendant of `scala.math.Numeric[T]` type
   * and do any casting necessary of child evaluation.
   */
  @inline
  def n1(e: Expression, i: Row, f: ((Numeric[Any], Any) => Any)): Any  = {
    val evalE = e.eval(i)
    if (evalE == null) {
      null
    } else {
      e.dataType match {
        case n: NumericType =>
          val castedFunction = f.asInstanceOf[(Numeric[n.JvmType], n.JvmType) => n.JvmType]
          castedFunction(n.numeric, evalE.asInstanceOf[n.JvmType])
        case other => sys.error(s"Type $other does not support numeric operations")
      }
    }
  }

  /**
   * Evaluation helper function for 2 Numeric children expressions. Those expressions are supposed
   * to be in the same data type, and also the return type.
   * Either one of the expressions result is null, the evaluation result should be null.
   */
  @inline
  protected final def n2(
      i: Row,
      e1: Expression,
      e2: Expression,
      f: ((Numeric[Any], Any, Any) => Any)): Any  = {

    if (e1.dataType != e2.dataType) {
      throw new TreeNodeException(this,  s"Types do not match ${e1.dataType} != ${e2.dataType}")
    }

    val evalE1 = e1.eval(i)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = e2.eval(i)
      if (evalE2 == null) {
        null
      } else {
        e1.dataType match {
          case n: NumericType =>
            f.asInstanceOf[(Numeric[n.JvmType], n.JvmType, n.JvmType) => n.JvmType](
              n.numeric, evalE1.asInstanceOf[n.JvmType], evalE2.asInstanceOf[n.JvmType])
          case other => sys.error(s"Type $other does not support numeric operations")
        }
      }
    }
  }

  /**
   * Evaluation helper function for 2 Fractional children expressions. Those expressions are
   * supposed to be in the same data type, and also the return type.
   * Either one of the expressions result is null, the evaluation result should be null.
   */
  @inline
  protected final def f2(
      i: Row,
      e1: Expression,
      e2: Expression,
      f: ((Fractional[Any], Any, Any) => Any)): Any  = {
    if (e1.dataType != e2.dataType) {
      throw new TreeNodeException(this,  s"Types do not match ${e1.dataType} != ${e2.dataType}")
    }

    val evalE1 = e1.eval(i: Row)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = e2.eval(i: Row)
      if (evalE2 == null) {
        null
      } else {
        e1.dataType match {
          case ft: FractionalType =>
            f.asInstanceOf[(Fractional[ft.JvmType], ft.JvmType, ft.JvmType) => ft.JvmType](
              ft.fractional, evalE1.asInstanceOf[ft.JvmType], evalE2.asInstanceOf[ft.JvmType])
          case other => sys.error(s"Type $other does not support fractional operations")
        }
      }
    }
  }

  /**
   * Evaluation helper function for 2 Integral children expressions. Those expressions are
   * supposed to be in the same data type, and also the return type.
   * Either one of the expressions result is null, the evaluation result should be null.
   */
  @inline
  protected final def i2(
      i: Row,
      e1: Expression,
      e2: Expression,
      f: ((Integral[Any], Any, Any) => Any)): Any  = {
    if (e1.dataType != e2.dataType) {
      throw new TreeNodeException(this,  s"Types do not match ${e1.dataType} != ${e2.dataType}")
    }

    val evalE1 = e1.eval(i)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = e2.eval(i)
      if (evalE2 == null) {
        null
      } else {
        e1.dataType match {
          case i: IntegralType =>
            f.asInstanceOf[(Integral[i.JvmType], i.JvmType, i.JvmType) => i.JvmType](
              i.integral, evalE1.asInstanceOf[i.JvmType], evalE2.asInstanceOf[i.JvmType])
          case other => sys.error(s"Type $other does not support numeric operations")
        }
      }
    }
  }

  /**
   * Evaluation helper function for 2 Comparable children expressions. Those expressions are
   * supposed to be in the same data type, and the return type should be Integer:
   * Negative value: 1st argument less than 2nd argument
   * Zero:  1st argument equals 2nd argument
   * Positive value: 1st argument greater than 2nd argument
   *
   * Either one of the expressions result is null, the evaluation result should be null.
   */
  @inline
  protected final def c2(
      i: Row,
      e1: Expression,
      e2: Expression,
      f: ((Ordering[Any], Any, Any) => Any)): Any  = {
    if (e1.dataType != e2.dataType) {
      throw new TreeNodeException(this,  s"Types do not match ${e1.dataType} != ${e2.dataType}")
    }

    val evalE1 = e1.eval(i)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = e2.eval(i)
      if (evalE2 == null) {
        null
      } else {
        e1.dataType match {
          case i: NativeType =>
            f.asInstanceOf[(Ordering[i.JvmType], i.JvmType, i.JvmType) => Boolean](
              i.ordering, evalE1.asInstanceOf[i.JvmType], evalE2.asInstanceOf[i.JvmType])
          case other => sys.error(s"Type $other does not support ordered operations")
        }
      }
    }
  }
}

abstract class BinaryExpression extends Expression with trees.BinaryNode[Expression] {
  self: Product =>

  def symbol: String

  override def foldable = left.foldable && right.foldable

  override def toString = s"($left $symbol $right)"
}

abstract class LeafExpression extends Expression with trees.LeafNode[Expression] {
  self: Product =>
}

abstract class UnaryExpression extends Expression with trees.UnaryNode[Expression] {
  self: Product =>


}
