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
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.array.ByteArrayMethods

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

object LambdaFunction {
  val identity: LambdaFunction = {
    val id = UnresolvedAttribute.quoted("id")
    LambdaFunction(id, Seq(id))
  }
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

trait ArrayBasedHigherOrderFunction extends HigherOrderFunction with ExpectsInputTypes {

  def input: Expression

  override def inputs: Seq[Expression] = input :: Nil

  def function: Expression

  override def functions: Seq[Expression] = function :: Nil

  def expectingFunctionType: AbstractDataType = AnyDataType

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, expectingFunctionType)

  @transient lazy val functionForEval: Expression = functionsForEval.head
}

object ArrayBasedHigherOrderFunction {

  def elementArgumentType(dt: DataType): (DataType, Boolean) = {
    dt match {
      case ArrayType(elementType, containsNull) => (elementType, containsNull)
      case _ =>
        val ArrayType(elementType, containsNull) = ArrayType.defaultConcreteType
        (elementType, containsNull)
    }
  }
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

  override def nullable: Boolean = input.nullable

  override def dataType: ArrayType = ArrayType(function.dataType, function.nullable)

  override def bind(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): ArrayTransform = {
    val elem = ArrayBasedHigherOrderFunction.elementArgumentType(input.dataType)
    function match {
      case LambdaFunction(_, arguments, _) if arguments.size == 2 =>
        copy(function = f(function, elem :: (IntegerType, false) :: Nil))
      case _ =>
        copy(function = f(function, elem :: Nil))
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
 * Filters the input array using the given lambda function.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, func) - Filters the input array using the given predicate.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), x -> x % 2 == 1);
       array(1, 3)
  """,
  since = "2.4.0")
case class ArrayFilter(
    input: Expression,
    function: Expression)
  extends ArrayBasedHigherOrderFunction with CodegenFallback {

  override def nullable: Boolean = input.nullable

  override def dataType: DataType = input.dataType

  override def expectingFunctionType: AbstractDataType = BooleanType

  override def bind(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): ArrayFilter = {
    val elem = ArrayBasedHigherOrderFunction.elementArgumentType(input.dataType)
    copy(function = f(function, elem :: Nil))
  }

  @transient lazy val LambdaFunction(_, Seq(elementVar: NamedLambdaVariable), _) = function

  override def eval(input: InternalRow): Any = {
    val arr = this.input.eval(input).asInstanceOf[ArrayData]
    if (arr == null) {
      null
    } else {
      val f = functionForEval
      val buffer = new mutable.ArrayBuffer[Any](arr.numElements)
      var i = 0
      while (i < arr.numElements) {
        elementVar.value.set(arr.get(i, elementVar.dataType))
        if (f.eval(input).asInstanceOf[Boolean]) {
          buffer += elementVar.value.get
        }
        i += 1
      }
      new GenericArrayData(buffer)
    }
  }

  override def prettyName: String = "filter"
}

/**
 * Applies a binary operator to a start value and all elements in the array.
 */
@ExpressionDescription(
  usage =
    """
      _FUNC_(expr, start, merge, finish) - Applies a binary operator to an initial state and all
      elements in the array, and reduces this to a single state. The final state is converted
      into the final result by applying a finish function.
    """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), (acc, x) -> acc + x);
       6
      > SELECT _FUNC_(array(1, 2, 3), (acc, x) -> acc + x, acc -> acc * 10);
       60
  """,
  since = "2.4.0")
case class ArrayAggregate(
    input: Expression,
    zero: Expression,
    merge: Expression,
    finish: Expression)
  extends HigherOrderFunction with CodegenFallback {

  def this(input: Expression, zero: Expression, merge: Expression) = {
    this(input, zero, merge, LambdaFunction.identity)
  }

  override def inputs: Seq[Expression] = input :: zero :: Nil

  override def functions: Seq[Expression] = merge :: finish :: Nil

  override def nullable: Boolean = input.nullable || finish.nullable

  override def dataType: DataType = finish.dataType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!ArrayType.acceptsType(input.dataType)) {
      TypeCheckResult.TypeCheckFailure(
        s"argument 1 requires ${ArrayType.simpleString} type, " +
          s"however, '${input.sql}' is of ${input.dataType.catalogString} type.")
    } else if (!DataType.equalsStructurally(
        zero.dataType, merge.dataType, ignoreNullability = true)) {
      TypeCheckResult.TypeCheckFailure(
        s"argument 3 requires ${zero.dataType.simpleString} type, " +
          s"however, '${merge.sql}' is of ${merge.dataType.catalogString} type.")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def bind(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): ArrayAggregate = {
    // Be very conservative with nullable. We cannot be sure that the accumulator does not
    // evaluate to null. So we always set nullable to true here.
    val elem = ArrayBasedHigherOrderFunction.elementArgumentType(input.dataType)
    val acc = zero.dataType -> true
    val newMerge = f(merge, acc :: elem :: Nil)
    val newFinish = f(finish, acc :: Nil)
    copy(merge = newMerge, finish = newFinish)
  }

  @transient lazy val LambdaFunction(_,
    Seq(accForMergeVar: NamedLambdaVariable, elementVar: NamedLambdaVariable), _) = merge
  @transient lazy val LambdaFunction(_, Seq(accForFinishVar: NamedLambdaVariable), _) = finish

  override def eval(input: InternalRow): Any = {
    val arr = this.input.eval(input).asInstanceOf[ArrayData]
    if (arr == null) {
      null
    } else {
      val Seq(mergeForEval, finishForEval) = functionsForEval
      accForMergeVar.value.set(zero.eval(input))
      var i = 0
      while (i < arr.numElements()) {
        elementVar.value.set(arr.get(i, elementVar.dataType))
        accForMergeVar.value.set(mergeForEval.eval(input))
        i += 1
      }
      accForFinishVar.value.set(accForMergeVar.value.get)
      finishForEval.eval(input)
    }
  }

  override def prettyName: String = "aggregate"
}

/**
 * Merges two given maps into a single map by applying function to the pair of values with
 * the same key.
 */
@ExpressionDescription(
  usage =
    """
      _FUNC_(map1, map2, function) - Merges two given maps into a single map by applying
      function to the pair of values with the same key. For keys only presented in one map,
      NULL will be passed as the value for the missing key. If an input map contains duplicated
      keys, only the first entry of the duplicated key is passed into the lambda function.
    """,
  examples = """
    Examples:
      > SELECT _FUNC_(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), (k, v1, v2) -> concat(v1, v2));
       {1:"ax",2:"by"}
  """,
  since = "2.4.0")
case class MapZipWith(left: Expression, right: Expression, function: Expression)
  extends HigherOrderFunction with CodegenFallback {

  @transient lazy val functionForEval: Expression = functionsForEval.head

  @transient lazy val MapType(keyType, leftValueType, _) = getMapType(left)

  @transient lazy val MapType(_, rightValueType, _) = getMapType(right)

  @transient lazy val ordering = TypeUtils.getInterpretedOrdering(keyType)

  override def inputs: Seq[Expression] = left :: right :: Nil

  override def functions: Seq[Expression] = function :: Nil

  override def nullable: Boolean = left.nullable || right.nullable

  override def dataType: DataType = MapType(keyType, function.dataType, function.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (MapType(k1, _, _), MapType(k2, _, _)) if k1.sameType(k2) =>
        TypeUtils.checkForOrderingExpr(k1, s"function $prettyName")
      case _ => TypeCheckResult.TypeCheckFailure(s"The input to function $prettyName should have " +
        s"been two ${MapType.simpleString}s with the same key type, but it's " +
        s"[${left.dataType.catalogString}, ${right.dataType.catalogString}].")
    }
  }

  private def getMapType(expr: Expression) = expr.dataType match {
    case m: MapType => m
    case _ => MapType.defaultConcreteType
  }

  override def bind(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): MapZipWith = {
    val arguments = Seq((keyType, false), (leftValueType, true), (rightValueType, true))
    copy(function = f(function, arguments))
  }

  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = right.eval(input)
      if (value2 == null) {
        null
      } else {
        nullSafeEval(input, value1, value2)
      }
    }
  }

  @transient lazy val LambdaFunction(_, Seq(
    keyVar: NamedLambdaVariable,
    value1Var: NamedLambdaVariable,
    value2Var: NamedLambdaVariable),
    _) = function

  private def keyTypeSupportsEquals = keyType match {
    case BinaryType => false
    case _: AtomicType => true
    case _ => false
  }

  @transient private lazy val getKeysWithValueIndexes:
      (ArrayData, ArrayData) => Seq[(Any, Array[Option[Int]])] = {
    if (keyTypeSupportsEquals) {
      getKeysWithIndexesFast
    } else {
      getKeysWithIndexesBruteForce
    }
  }

  private def assertSizeOfArrayBuffer(size: Int): Unit = {
    if (size > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
      throw new RuntimeException(s"Unsuccessful try to zip maps with $size " +
        s"unique keys due to exceeding the array size limit " +
        s"${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}.")
    }
  }

  private def getKeysWithIndexesFast(keys1: ArrayData, keys2: ArrayData) = {
    val arrayBuffer = new mutable.ArrayBuffer[(Any, Array[Option[Int]])]
    val hashMap = new mutable.OpenHashMap[Any, Array[Option[Int]]]
    val keys = Array(keys1, keys2)
    var z = 0
    while(z < 2) {
      var i = 0
      val array = keys(z)
      while (i < array.numElements()) {
        val key = array.get(i, keyType)
        hashMap.get(key) match {
          case Some(indexes) =>
            if (indexes(z).isEmpty) indexes(z) = Some(i)
          case None =>
            assertSizeOfArrayBuffer(arrayBuffer.size)
            val indexes = Array[Option[Int]](None, None)
            indexes(z) = Some(i)
            hashMap.put(key, indexes)
            arrayBuffer += Tuple2(key, indexes)
        }
        i += 1
      }
      z += 1
    }
    arrayBuffer
  }

  private def getKeysWithIndexesBruteForce(keys1: ArrayData, keys2: ArrayData) = {
    val arrayBuffer = new mutable.ArrayBuffer[(Any, Array[Option[Int]])]
    val keys = Array(keys1, keys2)
    var z = 0
    while(z < 2) {
      var i = 0
      val array = keys(z)
      while (i < array.numElements()) {
        val key = array.get(i, keyType)
        var found = false
        var j = 0
        while (!found && j < arrayBuffer.size) {
          val (bufferKey, indexes) = arrayBuffer(j)
          if (ordering.equiv(bufferKey, key)) {
            found = true
            if(indexes(z).isEmpty) indexes(z) = Some(i)
          }
          j += 1
        }
        if (!found) {
          assertSizeOfArrayBuffer(arrayBuffer.size)
          val indexes = Array[Option[Int]](None, None)
          indexes(z) = Some(i)
          arrayBuffer += Tuple2(key, indexes)
        }
        i += 1
      }
      z += 1
    }
    arrayBuffer
  }

  private def getValue(valueData: ArrayData, eType: DataType, index: Option[Int]) = index match {
    case Some(i) => valueData.get(i, eType)
    case None => null
  }

  private def nullSafeEval(inputRow: InternalRow, value1: Any, value2: Any): Any = {
    val mapData1 = value1.asInstanceOf[MapData]
    val mapData2 = value2.asInstanceOf[MapData]
    val keysWithIndexes = getKeysWithValueIndexes(mapData1.keyArray(), mapData2.keyArray())
    val length = keysWithIndexes.length
    val keys = new GenericArrayData(new Array[Any](length))
    val values = new GenericArrayData(new Array[Any](length))
    val valueData1 = mapData1.valueArray()
    val valueData2 = mapData2.valueArray()
    var i = 0
    while(i < length) {
      val (key, indexes) = keysWithIndexes(i)
      val v1 = getValue(valueData1, leftValueType, indexes(0))
      val v2 = getValue(valueData2, rightValueType, indexes(1))
      keyVar.value.set(key)
      value1Var.value.set(v1)
      value2Var.value.set(v2)
      keys.update(i, key)
      values.update(i, functionForEval.eval(inputRow))
      i += 1
    }
    new ArrayBasedMapData(keys, values)
  }

  override def prettyName: String = "map_zip_with"
}
