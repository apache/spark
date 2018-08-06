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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
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

  @transient lazy val functionsForEval: Seq[Expression] = functions.map {
    case LambdaFunction(function, arguments, hidden) =>
      val argumentMap = arguments.map { arg => arg.exprId -> arg }.toMap
      function.transformUp {
        case variable: NamedLambdaVariable if argumentMap.contains(variable.exprId) =>
          argumentMap(variable.exprId)
      }
  }
}

/**
 * Trait for functions having as input one argument and one function.
 */
trait UnaryHigherOrderFunction extends HigherOrderFunction with ExpectsInputTypes {

  def input: Expression

  override def inputs: Seq[Expression] = input :: Nil

  def function: Expression

  override def functions: Seq[Expression] = function :: Nil

  def expectingFunctionType: AbstractDataType = AnyDataType

  @transient lazy val functionForEval: Expression = functionsForEval.head

  /**
   * Called by [[eval]]. If a subclass keeps the default nullability, it can override this method
   * in order to save null-check code.
   */
  protected def nullSafeEval(inputRow: InternalRow, input: Any): Any =
    sys.error(s"UnaryHigherOrderFunction must override either eval or nullSafeEval")

  override def eval(inputRow: InternalRow): Any = {
    val value = input.eval(inputRow)
    if (value == null) {
      null
    } else {
      nullSafeEval(inputRow, value)
    }
  }
}

trait ArrayBasedUnaryHigherOrderFunction extends UnaryHigherOrderFunction {
  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, expectingFunctionType)
}

trait MapBasedUnaryHigherOrderFunction extends UnaryHigherOrderFunction {
  override def inputTypes: Seq[AbstractDataType] = Seq(MapType, expectingFunctionType)
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
  extends ArrayBasedUnaryHigherOrderFunction with CodegenFallback {

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
        copy(function = f(function, (elementType, containsNull) :: (IntegerType, false) :: Nil))
      case _ =>
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
    (elementVar, indexVar)
  }

  override def nullSafeEval(inputRow: InternalRow, inputValue: Any): Any = {
    val arr = inputValue.asInstanceOf[ArrayData]
    val f = functionForEval
    val result = new GenericArrayData(new Array[Any](arr.numElements))
    var i = 0
    while (i < arr.numElements) {
      elementVar.value.set(arr.get(i, elementVar.dataType))
      if (indexVar.isDefined) {
        indexVar.get.value.set(i)
      }
      result.update(i, f.eval(inputRow))
      i += 1
    }
    result
  }

  override def prettyName: String = "transform"
}

/**
 * Filters entries in a map using the provided function.
 */
@ExpressionDescription(
usage = "_FUNC_(expr, func) - Filters entries in a map using the function.",
examples = """
    Examples:
      > SELECT _FUNC_(map(1, 0, 2, 2, 3, -1), (k, v) -> k > v);
       [1 -> 0, 3 -> -1]
  """,
since = "2.4.0")
case class MapFilter(
    input: Expression,
    function: Expression)
  extends MapBasedUnaryHigherOrderFunction with CodegenFallback {

  @transient val (keyType, valueType, valueContainsNull) = input.dataType match {
    case MapType(kType, vType, vContainsNull) => (kType, vType, vContainsNull)
    case _ =>
      val MapType(kType, vType, vContainsNull) = MapType.defaultConcreteType
      (kType, vType, vContainsNull)
  }

  @transient lazy val (keyVar, valueVar) = {
    val args = function.asInstanceOf[LambdaFunction].arguments
    (args.head.asInstanceOf[NamedLambdaVariable], args.tail.head.asInstanceOf[NamedLambdaVariable])
  }

  override def bind(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): MapFilter = {
    function match {
      case LambdaFunction(_, _, _) =>
        copy(function = f(function, (keyType, false) :: (valueType, valueContainsNull) :: Nil))
    }
  }

  override def nullable: Boolean = input.nullable

  override def nullSafeEval(inputRow: InternalRow, value: Any): Any = {
    val m = value.asInstanceOf[MapData]
    val f = functionForEval
    val retKeys = new mutable.ListBuffer[Any]
    val retValues = new mutable.ListBuffer[Any]
    m.foreach(keyType, valueType, (k, v) => {
      keyVar.value.set(k)
      valueVar.value.set(v)
      if (f.eval(inputRow).asInstanceOf[Boolean]) {
        retKeys += k
        retValues += v
      }
    })
    ArrayBasedMapData(retKeys.toArray, retValues.toArray)
  }

  override def dataType: DataType = input.dataType

  override def expectingFunctionType: AbstractDataType = BooleanType

  override def prettyName: String = "map_filter"
}
