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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._

/**
 * A named lambda variable.
 */
case class NamedLambdaVariable(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    value: AtomicReference[Any] = new AtomicReference(),
    exprId: ExprId = NamedExpression.newExprId)
  extends LeafExpression
  with NamedExpression
  with CodegenFallback {

  override def qualifier: Option[String] = None

  override def newInstance(): NamedExpression =
    copy(value = new AtomicReference(), exprId = NamedExpression.newExprId)

  override def toAttribute: Attribute = {
    AttributeReference(name, dataType, nullable, Metadata.empty)(exprId, None)
  }

  override def eval(input: InternalRow): Any = value.get

  override def toString: String = s"lambda $name#${exprId.id}$typeSuffix"

  override def simpleString: String = s"lambda $name#${exprId.id}: ${dataType.simpleString}"
}

/**
 * A lambda function and its arguments. A lambda function can be hidden when a user wants to
 * process an completely independent expression in a [[HigherOrderFunction]], the lambda function
 * and its variables are then only used for internal bookkeeping within the higher order function.
 */
case class LambdaFunction(
    function: Expression,
    arguments: Seq[NamedExpression],
    hidden: Boolean = false)
  extends Expression with CodegenFallback {

  override def children: Seq[Expression] = function +: arguments
  override def dataType: DataType = function.dataType
  override def nullable: Boolean = function.nullable

  lazy val bound: Boolean = arguments.forall(_.resolved)

  override def eval(input: InternalRow): Any = function.eval(input)
}

/**
 * A higher order function takes one or more (lambda) functions and applies these to some objects.
 * The function produces a number of variables which can be consumed by some lambda function.
 */
trait HigherOrderFunction extends Expression {

  override def children: Seq[Expression] = inputs ++ functions

  /**
   * Inputs to the higher ordered function.
   */
  def inputs: Seq[Expression]

  /**
   * All inputs have been resolved. This means that the types and nullabilty of (most of) the
   * lambda function arguments is known, and that we can start binding the lambda functions.
   */
  lazy val inputResolved: Boolean = inputs.forall(_.resolved)

  /**
   * Functions applied by the higher order function.
   */
  def functions: Seq[Expression]

  /**
   * All inputs must be resolved and all functions must be resolved lambda functions.
   */
  override lazy val resolved: Boolean = inputResolved && functions.forall {
    case l: LambdaFunction => l.resolved
    case _ => false
  }

  /**
   * Bind the lambda functions to the [[HigherOrderFunction]] using the given bind function. The
   * bind function takes the potential lambda and it's (partial) arguments and converts this into
   * a bound lambda function.
   */
  def bind(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): HigherOrderFunction

  @transient lazy val functionsForEval: Seq[Expression] = {
    val x = functions.map {
      case LambdaFunction(function, arguments, hidden) =>
        val argumentMap = arguments.map { arg => arg.exprId -> arg }.toMap
        println((1, argumentMap)) // scalastyle:off
        function.transformUp {
          case variable: NamedLambdaVariable if argumentMap.contains(variable.exprId) =>
            println((2, variable)) // scalastyle:off
            argumentMap(variable.exprId)
        }
    }
    println((3, x)) // scalastyle:off
    x
  }
}

trait ArrayBasedHigherOrderFunction extends HigherOrderFunction with ExpectsInputTypes {

  override def inputs: Seq[Expression]

  override def functions: Seq[Expression]

  def expectingFunctionType: AbstractDataType = AnyDataType

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, expectingFunctionType)

  @transient lazy val functionForEval: Expression = functionsForEval.head
}

/**
 * Transform elements in an array using the transform function. This is similar to
 * a `map` in functional programming.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, func) - Transforms elements in an array using the function.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), x -> x + 1);
       array(2, 3, 4)
      > SELECT _FUNC_(array(1, 2, 3), (x, i) -> x + i);
       array(1, 3, 5)
  """,
  since = "2.4.0")
case class ArrayTransform(
    input: Expression,
    function: Expression)
  extends ArrayBasedHigherOrderFunction with CodegenFallback {

  override val inputs = input :: Nil

  override val functions = function :: Nil

  override def nullable: Boolean = input.nullable

  override def dataType: ArrayType = ArrayType(function.dataType, function.nullable)

  override def bind(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): ArrayTransform = {
    val (elementType, containsNull) = input.dataType match {
      case ArrayType(elementType, containsNull) => (elementType, containsNull)
      case _ =>
        val ArrayType(elementType, containsNull) = ArrayType.defaultConcreteType
        (elementType, containsNull)
    }
    function match {
      case LambdaFunction(_, arguments, _) if arguments.size == 2 =>
        println("args1"); println(arguments) // scalastyle:off
        copy(function = f(function, (elementType, containsNull) :: (IntegerType, false) :: Nil))
      case x =>
        println("fn"); println(x) // scalastyle:off
        copy(function = f(function, (elementType, containsNull) :: Nil))
    }
  }

  @transient lazy val (elementVar, indexVar) = {
    val LambdaFunction(_, (elementVar: NamedLambdaVariable) +: tail, _) = function
    val indexVar = if (tail.nonEmpty) {
      Some(tail.head.asInstanceOf[NamedLambdaVariable])
    } else {
      None
    }
    println((elementVar, indexVar)) // scalastyle:off
    (elementVar, indexVar)
  }

  override def eval(input: InternalRow): Any = {
    val arr = this.input.eval(input).asInstanceOf[ArrayData]
    if (arr == null) {
      null
    } else {
      val f = functionForEval
      val result = new GenericArrayData(new Array[Any](arr.numElements))
      var i = 0
      while (i < arr.numElements) {
        elementVar.value.set(arr.get(i, elementVar.dataType))
        if (indexVar.isDefined) {
          indexVar.get.value.set(i)
        }
        result.update(i, f.eval(input))
        i += 1
      }
      result
    }
  }

  override def prettyName: String = "transform"
}

/**
  * Transform elements in an array using the transform function. This is similar to
  * a `map` in functional programming.
  */
@ExpressionDescription(
  usage = "_FUNC_(expr, func) - Transforms elements in an array using the function.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), x -> x + 1);
       array(2, 3, 4)
      > SELECT _FUNC_(array(1, 2, 3), (x, i) -> x + i);
       array(1, 3, 5)
  """,
  since = "2.4.0")
case class ArraysZipWith(
    left: Expression,
    right: Expression,
    function: Expression)
  extends ArrayBasedHigherOrderFunction with CodegenFallback {

  override def inputs: Seq[Expression] = List(left, right)

  override def functions: Seq[Expression] = List(function)

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, ArrayType, expectingFunctionType)

  override def nullable: Boolean = inputs.exists(_.nullable)

  override def dataType: ArrayType = ArrayType(function.dataType, function.nullable)

  println(this.children)
  override def bind(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): ArraysZipWith = {
    val (leftElementType, leftContainsNull) = left.dataType match {
      case ArrayType(elementType, containsNull) => (elementType, containsNull)
      case _ =>
        val ArrayType(elementType, containsNull) = ArrayType.defaultConcreteType
        (elementType, containsNull)
    }
    val (rightElementType, rightContainsNull) = right.dataType match {
      case ArrayType(elementType, containsNull) => (elementType, containsNull)
      case _ =>
        val ArrayType(elementType, containsNull) = ArrayType.defaultConcreteType
        (elementType, containsNull)
    }
    copy(function = f(function, (leftElementType, leftContainsNull) :: (rightElementType, rightContainsNull) :: Nil))
  }

  @transient lazy val (arr1Var, arr2Var) = {
    val LambdaFunction(_, (arr1Var: NamedLambdaVariable) :: (arr2Var: NamedLambdaVariable) :: Nil, _) = function
    (arr1Var, arr2Var)
  }

  override def eval(input: InternalRow): Any = {
    val leftArr = left.eval(input).asInstanceOf[ArrayData]
    val rightArr = right.eval(input).asInstanceOf[ArrayData]

    if (leftArr == null || rightArr == null) {
      null
    } else {
      val resultLength = math.max(leftArr.numElements(), rightArr.numElements())
      val f = functionForEval
      val result = new GenericArrayData(new Array[Any](resultLength))
      var i = 0
      while (i < resultLength) {
        if(i < leftArr.numElements()) {
          arr1Var.value.set(leftArr.get(i, arr1Var.dataType))
        } else {
          arr1Var.value.set(null)
        }
        if(i < rightArr.numElements()) {
          arr2Var.value.set(rightArr.get(i, arr2Var.dataType))
        } else {
          arr2Var.value.set(null)
        }
        result.update(i, f.eval(input))
        i += 1
      }
      result
    }
  }

  override def prettyName: String = "zip_with"
}
