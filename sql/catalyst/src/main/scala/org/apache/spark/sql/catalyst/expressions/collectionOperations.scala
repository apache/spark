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

import java.util.Comparator
import java.util.regex.Pattern

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Given an array or map, returns its size. Returns -1 if null.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the size of an array or a map.",
  extended = " > SELECT _FUNC_(array('b', 'd', 'c', 'a'));\n 4")
case class Size(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(ArrayType, MapType))
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      -1
    } else child.dataType match {
      case _: ArrayType => value.asInstanceOf[ArrayData].numElements()
      case _: MapType => value.asInstanceOf[MapData].numElements()
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    ev.copy(code = s"""
      boolean ${ev.isNull} = false;
      ${childGen.code}
      ${ctx.javaType(dataType)} ${ev.value} = ${childGen.isNull} ? -1 :
        (${childGen.value}).numElements();""", isNull = "false")
  }
}

/**
 * Returns an unordered array containing the keys of the map.
 */
@ExpressionDescription(
  usage = "_FUNC_(map) - Returns an unordered array containing the keys of the map.",
  extended = " > SELECT _FUNC_(map(1, 'a', 2, 'b'));\n [1,2]")
case class MapKeys(child: Expression)
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  override def dataType: DataType = ArrayType(child.dataType.asInstanceOf[MapType].keyType)

  override def nullSafeEval(map: Any): Any = {
    map.asInstanceOf[MapData].keyArray()
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"${ev.value} = ($c).keyArray();")
  }

  override def prettyName: String = "map_keys"
}

/**
 * Returns an unordered array containing the values of the map.
 */
@ExpressionDescription(
  usage = "_FUNC_(map) - Returns an unordered array containing the values of the map.",
  extended = " > SELECT _FUNC_(map(1, 'a', 2, 'b'));\n [\"a\",\"b\"]")
case class MapValues(child: Expression)
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  override def dataType: DataType = ArrayType(child.dataType.asInstanceOf[MapType].valueType)

  override def nullSafeEval(map: Any): Any = {
    map.asInstanceOf[MapData].valueArray()
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"${ev.value} = ($c).valueArray();")
  }

  override def prettyName: String = "map_values"
}

/**
 * Sorts the input array in ascending / descending order according to the natural ordering of
 * the array elements and returns it.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(array(obj1, obj2, ...), ascendingOrder) - Sorts the input array in ascending order according to the natural ordering of the array elements.",
  extended = " > SELECT _FUNC_(array('b', 'd', 'c', 'a'), true);\n 'a', 'b', 'c', 'd'")
// scalastyle:on line.size.limit
case class SortArray(base: Expression, ascendingOrder: Expression)
  extends BinaryExpression with ExpectsInputTypes with CodegenFallback {

  def this(e: Expression) = this(e, Literal(true))

  override def left: Expression = base
  override def right: Expression = ascendingOrder
  override def dataType: DataType = base.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, BooleanType)

  override def checkInputDataTypes(): TypeCheckResult = base.dataType match {
    case ArrayType(dt, _) if RowOrdering.isOrderable(dt) =>
      TypeCheckResult.TypeCheckSuccess
    case ArrayType(dt, _) =>
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName does not support sorting array of type ${dt.simpleString}")
    case _ =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input.")
  }

  @transient
  private lazy val lt: Comparator[Any] = {
    val ordering = base.dataType match {
      case _ @ ArrayType(n: AtomicType, _) => n.ordering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(a: ArrayType, _) => a.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(s: StructType, _) => s.interpretedOrdering.asInstanceOf[Ordering[Any]]
    }

    new Comparator[Any]() {
      override def compare(o1: Any, o2: Any): Int = {
        if (o1 == null && o2 == null) {
          0
        } else if (o1 == null) {
          -1
        } else if (o2 == null) {
          1
        } else {
          ordering.compare(o1, o2)
        }
      }
    }
  }

  @transient
  private lazy val gt: Comparator[Any] = {
    val ordering = base.dataType match {
      case _ @ ArrayType(n: AtomicType, _) => n.ordering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(a: ArrayType, _) => a.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(s: StructType, _) => s.interpretedOrdering.asInstanceOf[Ordering[Any]]
    }

    new Comparator[Any]() {
      override def compare(o1: Any, o2: Any): Int = {
        if (o1 == null && o2 == null) {
          0
        } else if (o1 == null) {
          1
        } else if (o2 == null) {
          -1
        } else {
          -ordering.compare(o1, o2)
        }
      }
    }
  }

  override def nullSafeEval(array: Any, ascending: Any): Any = {
    val elementType = base.dataType.asInstanceOf[ArrayType].elementType
    val data = array.asInstanceOf[ArrayData].toArray[AnyRef](elementType)
    if (elementType != NullType) {
      java.util.Arrays.sort(data, if (ascending.asInstanceOf[Boolean]) lt else gt)
    }
    new GenericArrayData(data.asInstanceOf[Array[Any]])
  }

  override def prettyName: String = "sort_array"
}

/**
 * Checks if the array (left) has the element (right) and pattern match in
 * case left is Array of type string
 */

@ExpressionDescription(
  usage = """_FUNC_(array, value) - Returns TRUE if the array contains the value or
    for string arrays, if string matches with the any pattern in the array.
    This is complete word match""",
  extended = """ > SELECT _FUNC_(array("\\d\\s\\d", "2", "3"), "1 5");\n true""")
case class ArrayContains(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = BooleanType

  override def inputTypes: Seq[AbstractDataType] = right.dataType match {
    case NullType => Seq()
    case _ => left.dataType match {
      case n @ ArrayType(element, _) => Seq(n, element)
      case _ => Seq()
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (right.dataType == NullType) {
      TypeCheckResult.TypeCheckFailure("Null typed values cannot be used as arguments")
    } else if (!left.dataType.isInstanceOf[ArrayType]
      || left.dataType.asInstanceOf[ArrayType].elementType != right.dataType) {
      TypeCheckResult.TypeCheckFailure(
        "Arguments must be an array followed by a value of same type as the array members")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def nullable: Boolean = {
    left.nullable || right.nullable || left.dataType.asInstanceOf[ArrayType].containsNull
  }

  // last regex in string, we will update the pattern iff regexp value changed.
  @transient private var lastRegexArray: ArrayData = _
  // last regex pattern, we cache it for performance concern
  @transient private var patternArray: Array[Pattern] = _


  override def nullSafeEval(arrAny: Any, value: Any): Any = {
    val arr = arrAny.asInstanceOf[ArrayData]
    var hasNull = false
    if (right.dataType == StringType) {
      if (!arr.equals(lastRegexArray)) {
        lastRegexArray = arr.copy()
        patternArray = new Array[Pattern](arr.numElements())
        lastRegexArray.foreach(StringType, (i : Int, str : Any) => if (str == null) {
          patternArray(i) = null
        } else {
          patternArray(i) = Pattern.compile("^".concat(str.toString).concat("$"))
        })
      }
      patternArray.foreach(v => if (v == null) {
        hasNull = true
        false
      } else if (v.matcher(value.asInstanceOf[UTF8String].toString).find()) {
        return true
      })
    } else {
      arr.asInstanceOf[ArrayData].foreach(right.dataType, (i, v) =>
        if (v == null) {
          hasNull = true
        } else if (v == value) {
          return true
        }
      )
    }

    if (hasNull) {
      null
    } else {
      false
    }
  }
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val termLastRegexArray = ctx.freshName("lastRegexArray")
    val termPatternArray = ctx.freshName("patternArray")
    val patternClassNamePattern = classOf[Pattern].getCanonicalName.stripSuffix("[]")
    val arrayDataClassNamePattern = classOf[ArrayData].getCanonicalName.stripSuffix("[]")

    ctx.addMutableState(s"$arrayDataClassNamePattern", termLastRegexArray,
      s"${termLastRegexArray} = null;")
    ctx.addMutableState(s"$patternClassNamePattern[]", termPatternArray,
      s"${termPatternArray} = null;")

    nullSafeCodeGen(ctx, ev, (arr, value) => {
      val i = ctx.freshName("i")
      var getValue = ctx.getValue(arr, right.dataType, i)
      val code = if (right.dataType == StringType) {
        s"""
        if (!$arr.equals(${termLastRegexArray})) {
          // regex Array value changed
          ${termPatternArray} = new ${patternClassNamePattern}[$arr.numElements()];
          ${termLastRegexArray} = $arr.copy();
          for (int $i = 0; $i < $arr.numElements(); $i ++) {
            if ($arr.isNullAt($i)) {
              ${termPatternArray}[$i] = null;
            } else {
              ${termPatternArray}[$i] = ${patternClassNamePattern}.compile(
              "^".concat(${getValue}.toString()).concat("$$"));
            }
          }
        }""".stripMargin
      } else ""
      val k = {
        if (right.dataType == StringType) {
          getValue = s"${termPatternArray}[$i]"
        }
        s"""
        for (int $i = 0; $i < $arr.numElements(); $i ++) {
          if ($arr.isNullAt($i)) {
            ${ev.isNull} = true;
          } else if (${genEqual(ctx, ev, right.dataType, value, getValue)}) {
            ${ev.isNull} = false;
            ${ev.value} = true;
            break;
          }
        }""".stripMargin }
      code + k
    }
    )
  }

  def genEqual(ctx: CodegenContext, ev: ExprCode, dataType: DataType,
               c1: String, c2: String): String = dataType match {
    case StringType => s"${c2}.matcher($c1.toString()).find()".stripMargin
    case _ => ctx.genEqual(dataType, c1, c2)
  }

  override def prettyName: String = "array_contains"
}
