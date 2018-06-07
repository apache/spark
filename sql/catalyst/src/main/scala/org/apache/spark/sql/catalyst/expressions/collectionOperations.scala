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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.ArraySortLike.NullOrder
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData, TypeUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}

/**
 * Base trait for [[BinaryExpression]]s with two arrays of the same element type and implicit
 * casting.
 */
trait BinaryArrayExpressionWithImplicitCast extends BinaryExpression
  with ImplicitCastInputTypes {

  @transient protected lazy val elementType: DataType =
    inputTypes.head.asInstanceOf[ArrayType].elementType

  override def inputTypes: Seq[AbstractDataType] = {
    (left.dataType, right.dataType) match {
      case (ArrayType(e1, hasNull1), ArrayType(e2, hasNull2)) =>
        TypeCoercion.findTightestCommonType(e1, e2) match {
          case Some(dt) => Seq(ArrayType(dt, hasNull1), ArrayType(dt, hasNull2))
          case _ => Seq.empty
        }
      case _ => Seq.empty
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (ArrayType(e1, _), ArrayType(e2, _)) if e1.sameType(e2) =>
        TypeCheckResult.TypeCheckSuccess
      case _ => TypeCheckResult.TypeCheckFailure(s"input to function $prettyName should have " +
        s"been two ${ArrayType.simpleString}s with same element type, but it's " +
        s"[${left.dataType.simpleString}, ${right.dataType.simpleString}]")
    }
  }
}


/**
 * Given an array or map, returns its size. Returns -1 if null.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the size of an array or a map. Returns -1 if null.",
  examples = """
    Examples:
      > SELECT _FUNC_(array('b', 'd', 'c', 'a'));
       4
  """)
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
    ev.copy(code = code"""
      boolean ${ev.isNull} = false;
      ${childGen.code}
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${childGen.isNull} ? -1 :
        (${childGen.value}).numElements();""", isNull = FalseLiteral)
  }
}

/**
 * Returns an unordered array containing the keys of the map.
 */
@ExpressionDescription(
  usage = "_FUNC_(map) - Returns an unordered array containing the keys of the map.",
  examples = """
    Examples:
      > SELECT _FUNC_(map(1, 'a', 2, 'b'));
       [1,2]
  """)
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
  examples = """
    Examples:
      > SELECT _FUNC_(map(1, 'a', 2, 'b'));
       ["a","b"]
  """)
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
 * Returns an unordered array of all entries in the given map.
 */
@ExpressionDescription(
  usage = "_FUNC_(map) - Returns an unordered array of all entries in the given map.",
  examples = """
    Examples:
      > SELECT _FUNC_(map(1, 'a', 2, 'b'));
       [(1,"a"),(2,"b")]
  """,
  since = "2.4.0")
case class MapEntries(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  lazy val childDataType: MapType = child.dataType.asInstanceOf[MapType]

  override def dataType: DataType = {
    ArrayType(
      StructType(
        StructField("key", childDataType.keyType, false) ::
        StructField("value", childDataType.valueType, childDataType.valueContainsNull) ::
        Nil),
      false)
  }

  override protected def nullSafeEval(input: Any): Any = {
    val childMap = input.asInstanceOf[MapData]
    val keys = childMap.keyArray()
    val values = childMap.valueArray()
    val length = childMap.numElements()
    val resultData = new Array[AnyRef](length)
    var i = 0;
    while (i < length) {
      val key = keys.get(i, childDataType.keyType)
      val value = values.get(i, childDataType.valueType)
      val row = new GenericInternalRow(Array[Any](key, value))
      resultData.update(i, row)
      i += 1
    }
    new GenericArrayData(resultData)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => {
      val numElements = ctx.freshName("numElements")
      val keys = ctx.freshName("keys")
      val values = ctx.freshName("values")
      val isKeyPrimitive = CodeGenerator.isPrimitiveType(childDataType.keyType)
      val isValuePrimitive = CodeGenerator.isPrimitiveType(childDataType.valueType)
      val code = if (isKeyPrimitive && isValuePrimitive) {
        genCodeForPrimitiveElements(ctx, keys, values, ev.value, numElements)
      } else {
        genCodeForAnyElements(ctx, keys, values, ev.value, numElements)
      }
      s"""
         |final int $numElements = $c.numElements();
         |final ArrayData $keys = $c.keyArray();
         |final ArrayData $values = $c.valueArray();
         |$code
       """.stripMargin
    })
  }

  private def getKey(varName: String) = CodeGenerator.getValue(varName, childDataType.keyType, "z")

  private def getValue(varName: String) = {
    CodeGenerator.getValue(varName, childDataType.valueType, "z")
  }

  private def genCodeForPrimitiveElements(
      ctx: CodegenContext,
      keys: String,
      values: String,
      arrayData: String,
      numElements: String): String = {
    val unsafeRow = ctx.freshName("unsafeRow")
    val unsafeArrayData = ctx.freshName("unsafeArrayData")
    val structsOffset = ctx.freshName("structsOffset")
    val calculateHeader = "UnsafeArrayData.calculateHeaderPortionInBytes"

    val baseOffset = Platform.BYTE_ARRAY_OFFSET
    val wordSize = UnsafeRow.WORD_SIZE
    val structSize = UnsafeRow.calculateBitSetWidthInBytes(2) + wordSize * 2
    val structSizeAsLong = structSize + "L"
    val keyTypeName = CodeGenerator.primitiveTypeName(childDataType.keyType)
    val valueTypeName = CodeGenerator.primitiveTypeName(childDataType.keyType)

    val valueAssignment = s"$unsafeRow.set$valueTypeName(1, ${getValue(values)});"
    val valueAssignmentChecked = if (childDataType.valueContainsNull) {
      s"""
         |if ($values.isNullAt(z)) {
         |  $unsafeRow.setNullAt(1);
         |} else {
         |  $valueAssignment
         |}
       """.stripMargin
    } else {
      valueAssignment
    }

    val assignmentLoop = (byteArray: String) =>
      s"""
         |final int $structsOffset = $calculateHeader($numElements) + $numElements * $wordSize;
         |UnsafeRow $unsafeRow = new UnsafeRow(2);
         |for (int z = 0; z < $numElements; z++) {
         |  long offset = $structsOffset + z * $structSizeAsLong;
         |  $unsafeArrayData.setLong(z, (offset << 32) + $structSizeAsLong);
         |  $unsafeRow.pointTo($byteArray, $baseOffset + offset, $structSize);
         |  $unsafeRow.set$keyTypeName(0, ${getKey(keys)});
         |  $valueAssignmentChecked
         |}
         |$arrayData = $unsafeArrayData;
       """.stripMargin

    ctx.createUnsafeArrayWithFallback(
      unsafeArrayData,
      numElements,
      structSize + wordSize,
      assignmentLoop,
      genCodeForAnyElements(ctx, keys, values, arrayData, numElements))
  }

  private def genCodeForAnyElements(
      ctx: CodegenContext,
      keys: String,
      values: String,
      arrayData: String,
      numElements: String): String = {
    val genericArrayClass = classOf[GenericArrayData].getName
    val rowClass = classOf[GenericInternalRow].getName
    val data = ctx.freshName("internalRowArray")

    val isValuePrimitive = CodeGenerator.isPrimitiveType(childDataType.valueType)
    val getValueWithCheck = if (childDataType.valueContainsNull && isValuePrimitive) {
      s"$values.isNullAt(z) ? null : (Object)${getValue(values)}"
    } else {
      getValue(values)
    }

    s"""
       |final Object[] $data = new Object[$numElements];
       |for (int z = 0; z < $numElements; z++) {
       |  $data[z] = new $rowClass(new Object[]{${getKey(keys)}, $getValueWithCheck});
       |}
       |$arrayData = new $genericArrayClass($data);
     """.stripMargin
  }

  override def prettyName: String = "map_entries"
}

/**
 * Common base class for [[SortArray]] and [[ArraySort]].
 */
trait ArraySortLike extends ExpectsInputTypes {
  protected def arrayExpression: Expression

  protected def nullOrder: NullOrder

  @transient
  private lazy val lt: Comparator[Any] = {
    val ordering = arrayExpression.dataType match {
      case _ @ ArrayType(n: AtomicType, _) => n.ordering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(a: ArrayType, _) => a.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(s: StructType, _) => s.interpretedOrdering.asInstanceOf[Ordering[Any]]
    }

    new Comparator[Any]() {
      override def compare(o1: Any, o2: Any): Int = {
        if (o1 == null && o2 == null) {
          0
        } else if (o1 == null) {
          nullOrder
        } else if (o2 == null) {
          -nullOrder
        } else {
          ordering.compare(o1, o2)
        }
      }
    }
  }

  @transient
  private lazy val gt: Comparator[Any] = {
    val ordering = arrayExpression.dataType match {
      case _ @ ArrayType(n: AtomicType, _) => n.ordering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(a: ArrayType, _) => a.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(s: StructType, _) => s.interpretedOrdering.asInstanceOf[Ordering[Any]]
    }

    new Comparator[Any]() {
      override def compare(o1: Any, o2: Any): Int = {
        if (o1 == null && o2 == null) {
          0
        } else if (o1 == null) {
          -nullOrder
        } else if (o2 == null) {
          nullOrder
        } else {
          -ordering.compare(o1, o2)
        }
      }
    }
  }

  def elementType: DataType = arrayExpression.dataType.asInstanceOf[ArrayType].elementType
  def containsNull: Boolean = arrayExpression.dataType.asInstanceOf[ArrayType].containsNull

  def sortEval(array: Any, ascending: Boolean): Any = {
    val data = array.asInstanceOf[ArrayData].toArray[AnyRef](elementType)
    if (elementType != NullType) {
      java.util.Arrays.sort(data, if (ascending) lt else gt)
    }
    new GenericArrayData(data.asInstanceOf[Array[Any]])
  }

  def sortCodegen(ctx: CodegenContext, ev: ExprCode, base: String, order: String): String = {
    val arrayData = classOf[ArrayData].getName
    val genericArrayData = classOf[GenericArrayData].getName
    val unsafeArrayData = classOf[UnsafeArrayData].getName
    val array = ctx.freshName("array")
    val c = ctx.freshName("c")
    if (elementType == NullType) {
      s"${ev.value} = $base.copy();"
    } else {
      val elementTypeTerm = ctx.addReferenceObj("elementTypeTerm", elementType)
      val sortOrder = ctx.freshName("sortOrder")
      val o1 = ctx.freshName("o1")
      val o2 = ctx.freshName("o2")
      val jt = CodeGenerator.javaType(elementType)
      val comp = if (CodeGenerator.isPrimitiveType(elementType)) {
        val bt = CodeGenerator.boxedType(elementType)
        val v1 = ctx.freshName("v1")
        val v2 = ctx.freshName("v2")
        s"""
           |$jt $v1 = (($bt) $o1).${jt}Value();
           |$jt $v2 = (($bt) $o2).${jt}Value();
           |int $c = ${ctx.genComp(elementType, v1, v2)};
         """.stripMargin
      } else {
        s"int $c = ${ctx.genComp(elementType, s"(($jt) $o1)", s"(($jt) $o2)")};"
      }
      val nonNullPrimitiveAscendingSort =
        if (CodeGenerator.isPrimitiveType(elementType) && !containsNull) {
          val javaType = CodeGenerator.javaType(elementType)
          val primitiveTypeName = CodeGenerator.primitiveTypeName(elementType)
          s"""
             |if ($order) {
             |  $javaType[] $array = $base.to${primitiveTypeName}Array();
             |  java.util.Arrays.sort($array);
             |  ${ev.value} = $unsafeArrayData.fromPrimitiveArray($array);
             |} else
           """.stripMargin
        } else {
          ""
        }
      s"""
         |$nonNullPrimitiveAscendingSort
         |{
         |  Object[] $array = $base.toObjectArray($elementTypeTerm);
         |  final int $sortOrder = $order ? 1 : -1;
         |  java.util.Arrays.sort($array, new java.util.Comparator() {
         |    @Override public int compare(Object $o1, Object $o2) {
         |      if ($o1 == null && $o2 == null) {
         |        return 0;
         |      } else if ($o1 == null) {
         |        return $sortOrder * $nullOrder;
         |      } else if ($o2 == null) {
         |        return -$sortOrder * $nullOrder;
         |      }
         |      $comp
         |      return $sortOrder * $c;
         |    }
         |  });
         |  ${ev.value} = new $genericArrayData($array);
         |}
       """.stripMargin
    }
  }

}

object ArraySortLike {
  type NullOrder = Int
  // Least: place null element at the first of the array for ascending order
  // Greatest: place null element at the end of the array for ascending order
  object NullOrder {
    val Least: NullOrder = -1
    val Greatest: NullOrder = 1
  }
}

/**
 * Sorts the input array in ascending / descending order according to the natural ordering of
 * the array elements and returns it.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(array[, ascendingOrder]) - Sorts the input array in ascending or descending order
      according to the natural ordering of the array elements. Null elements will be placed
      at the beginning of the returned array in ascending order or at the end of the returned
      array in descending order.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array('b', 'd', null, 'c', 'a'), true);
       [null,"a","b","c","d"]
  """)
// scalastyle:on line.size.limit
case class SortArray(base: Expression, ascendingOrder: Expression)
  extends BinaryExpression with ArraySortLike {

  def this(e: Expression) = this(e, Literal(true))

  override def left: Expression = base
  override def right: Expression = ascendingOrder
  override def dataType: DataType = base.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, BooleanType)

  override def arrayExpression: Expression = base
  override def nullOrder: NullOrder = NullOrder.Least

  override def checkInputDataTypes(): TypeCheckResult = base.dataType match {
    case ArrayType(dt, _) if RowOrdering.isOrderable(dt) =>
      ascendingOrder match {
        case Literal(_: Boolean, BooleanType) =>
          TypeCheckResult.TypeCheckSuccess
        case _ =>
          TypeCheckResult.TypeCheckFailure(
            "Sort order in second argument requires a boolean literal.")
      }
    case ArrayType(dt, _) =>
      val dtSimple = dt.simpleString
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName does not support sorting array of type $dtSimple which is not orderable")
    case _ =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input.")
  }

  override def nullSafeEval(array: Any, ascending: Any): Any = {
    sortEval(array, ascending.asInstanceOf[Boolean])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (b, order) => sortCodegen(ctx, ev, b, order))
  }

  override def prettyName: String = "sort_array"
}


/**
 * Sorts the input array in ascending order according to the natural ordering of
 * the array elements and returns it.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(array) - Sorts the input array in ascending order. The elements of the input array must
      be orderable. Null elements will be placed at the end of the returned array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array('b', 'd', null, 'c', 'a'));
       ["a","b","c","d",null]
  """,
  since = "2.4.0")
// scalastyle:on line.size.limit
case class ArraySort(child: Expression) extends UnaryExpression with ArraySortLike {

  override def dataType: DataType = child.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  override def arrayExpression: Expression = child
  override def nullOrder: NullOrder = NullOrder.Greatest

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case ArrayType(dt, _) if RowOrdering.isOrderable(dt) =>
      TypeCheckResult.TypeCheckSuccess
    case ArrayType(dt, _) =>
      val dtSimple = dt.simpleString
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName does not support sorting array of type $dtSimple which is not orderable")
    case _ =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input.")
  }

  override def nullSafeEval(array: Any): Any = {
    sortEval(array, true)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => sortCodegen(ctx, ev, c, "true"))
  }

  override def prettyName: String = "array_sort"
}

/**
 * Returns a reversed string or an array with reverse order of elements.
 */
@ExpressionDescription(
  usage = "_FUNC_(array) - Returns a reversed string or an array with reverse order of elements.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL');
       LQS krapS
      > SELECT _FUNC_(array(2, 1, 4, 3));
       [3, 4, 1, 2]
  """,
  since = "1.5.0",
  note = "Reverse logic for arrays is available since 2.4.0."
)
case class Reverse(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  // Input types are utilized by type coercion in ImplicitTypeCasts.
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(StringType, ArrayType))

  override def dataType: DataType = child.dataType

  lazy val elementType: DataType = dataType.asInstanceOf[ArrayType].elementType

  override def nullSafeEval(input: Any): Any = input match {
    case a: ArrayData => new GenericArrayData(a.toObjectArray(elementType).reverse)
    case s: UTF8String => s.reverse()
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => dataType match {
      case _: StringType => stringCodeGen(ev, c)
      case _: ArrayType => arrayCodeGen(ctx, ev, c)
    })
  }

  private def stringCodeGen(ev: ExprCode, childName: String): String = {
    s"${ev.value} = ($childName).reverse();"
  }

  private def arrayCodeGen(ctx: CodegenContext, ev: ExprCode, childName: String): String = {
    val length = ctx.freshName("length")
    val javaElementType = CodeGenerator.javaType(elementType)
    val isPrimitiveType = CodeGenerator.isPrimitiveType(elementType)

    val initialization = if (isPrimitiveType) {
      s"$childName.copy()"
    } else {
      s"new ${classOf[GenericArrayData].getName()}(new Object[$length])"
    }

    val numberOfIterations = if (isPrimitiveType) s"$length / 2" else length

    val swapAssigments = if (isPrimitiveType) {
      val setFunc = "set" + CodeGenerator.primitiveTypeName(elementType)
      val getCall = (index: String) => CodeGenerator.getValue(ev.value, elementType, index)
      s"""|boolean isNullAtK = ${ev.value}.isNullAt(k);
          |boolean isNullAtL = ${ev.value}.isNullAt(l);
          |if(!isNullAtK) {
          |  $javaElementType el = ${getCall("k")};
          |  if(!isNullAtL) {
          |    ${ev.value}.$setFunc(k, ${getCall("l")});
          |  } else {
          |    ${ev.value}.setNullAt(k);
          |  }
          |  ${ev.value}.$setFunc(l, el);
          |} else if (!isNullAtL) {
          |  ${ev.value}.$setFunc(k, ${getCall("l")});
          |  ${ev.value}.setNullAt(l);
          |}""".stripMargin
    } else {
      s"${ev.value}.update(k, ${CodeGenerator.getValue(childName, elementType, "l")});"
    }

    s"""
       |final int $length = $childName.numElements();
       |${ev.value} = $initialization;
       |for(int k = 0; k < $numberOfIterations; k++) {
       |  int l = $length - k - 1;
       |  $swapAssigments
       |}
     """.stripMargin
  }

  override def prettyName: String = "reverse"
}

/**
 * Checks if the array (left) has the element (right)
 */
@ExpressionDescription(
  usage = "_FUNC_(array, value) - Returns true if the array contains the value.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), 2);
       true
  """)
case class ArrayContains(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = BooleanType

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(right.dataType)

  override def inputTypes: Seq[AbstractDataType] = right.dataType match {
    case NullType => Seq.empty
    case _ => left.dataType match {
      case n @ ArrayType(element, _) => Seq(n, element)
      case _ => Seq.empty
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
      TypeUtils.checkForOrderingExpr(right.dataType, s"function $prettyName")
    }
  }

  override def nullable: Boolean = {
    left.nullable || right.nullable || left.dataType.asInstanceOf[ArrayType].containsNull
  }

  override def nullSafeEval(arr: Any, value: Any): Any = {
    var hasNull = false
    arr.asInstanceOf[ArrayData].foreach(right.dataType, (i, v) =>
      if (v == null) {
        hasNull = true
      } else if (ordering.equiv(v, value)) {
        return true
      }
    )
    if (hasNull) {
      null
    } else {
      false
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (arr, value) => {
      val i = ctx.freshName("i")
      val getValue = CodeGenerator.getValue(arr, right.dataType, i)
      s"""
      for (int $i = 0; $i < $arr.numElements(); $i ++) {
        if ($arr.isNullAt($i)) {
          ${ev.isNull} = true;
        } else if (${ctx.genEqual(right.dataType, value, getValue)}) {
          ${ev.isNull} = false;
          ${ev.value} = true;
          break;
        }
      }
     """
    })
  }

  override def prettyName: String = "array_contains"
}

/**
 * Checks if the two arrays contain at least one common element.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(a1, a2) - Returns true if a1 contains at least a non-null element present also in a2. If the arrays have no common element and they are both non-empty and either of them contains a null element null is returned, false otherwise.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), array(3, 4, 5));
       true
  """, since = "2.4.0")
// scalastyle:off line.size.limit
case class ArraysOverlap(left: Expression, right: Expression)
  extends BinaryArrayExpressionWithImplicitCast {

  override def checkInputDataTypes(): TypeCheckResult = super.checkInputDataTypes() match {
    case TypeCheckResult.TypeCheckSuccess =>
      TypeUtils.checkForOrderingExpr(elementType, s"function $prettyName")
    case failure => failure
  }

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(elementType)

  @transient private lazy val elementTypeSupportEquals = elementType match {
    case BinaryType => false
    case _: AtomicType => true
    case _ => false
  }

  @transient private lazy val doEvaluation = if (elementTypeSupportEquals) {
    fastEval _
  } else {
    bruteForceEval _
  }

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = {
    left.nullable || right.nullable || left.dataType.asInstanceOf[ArrayType].containsNull ||
      right.dataType.asInstanceOf[ArrayType].containsNull
  }

  override def nullSafeEval(a1: Any, a2: Any): Any = {
    doEvaluation(a1.asInstanceOf[ArrayData], a2.asInstanceOf[ArrayData])
  }

  /**
   * A fast implementation which puts all the elements from the smaller array in a set
   * and then performs a lookup on it for each element of the bigger one.
   * This eval mode works only for data types which implements properly the equals method.
   */
  private def fastEval(arr1: ArrayData, arr2: ArrayData): Any = {
    var hasNull = false
    val (bigger, smaller) = if (arr1.numElements() > arr2.numElements()) {
      (arr1, arr2)
    } else {
      (arr2, arr1)
    }
    if (smaller.numElements() > 0) {
      val smallestSet = new mutable.HashSet[Any]
      smaller.foreach(elementType, (_, v) =>
        if (v == null) {
          hasNull = true
        } else {
          smallestSet += v
        })
      bigger.foreach(elementType, (_, v1) =>
        if (v1 == null) {
          hasNull = true
        } else if (smallestSet.contains(v1)) {
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

  /**
   * A slower evaluation which performs a nested loop and supports all the data types.
   */
  private def bruteForceEval(arr1: ArrayData, arr2: ArrayData): Any = {
    var hasNull = false
    if (arr1.numElements() > 0 && arr2.numElements() > 0) {
      arr1.foreach(elementType, (_, v1) =>
        if (v1 == null) {
          hasNull = true
        } else {
          arr2.foreach(elementType, (_, v2) =>
            if (v2 == null) {
              hasNull = true
            } else if (ordering.equiv(v1, v2)) {
              return true
            }
          )
        })
    }
    if (hasNull) {
      null
    } else {
      false
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (a1, a2) => {
      val smaller = ctx.freshName("smallerArray")
      val bigger = ctx.freshName("biggerArray")
      val comparisonCode = if (elementTypeSupportEquals) {
        fastCodegen(ctx, ev, smaller, bigger)
      } else {
        bruteForceCodegen(ctx, ev, smaller, bigger)
      }
      s"""
         |ArrayData $smaller;
         |ArrayData $bigger;
         |if ($a1.numElements() > $a2.numElements()) {
         |  $bigger = $a1;
         |  $smaller = $a2;
         |} else {
         |  $smaller = $a1;
         |  $bigger = $a2;
         |}
         |if ($smaller.numElements() > 0) {
         |  $comparisonCode
         |}
       """.stripMargin
    })
  }

  /**
   * Code generation for a fast implementation which puts all the elements from the smaller array
   * in a set and then performs a lookup on it for each element of the bigger one.
   * It works only for data types which implements properly the equals method.
   */
  private def fastCodegen(ctx: CodegenContext, ev: ExprCode, smaller: String, bigger: String): String = {
    val i = ctx.freshName("i")
    val getFromSmaller = CodeGenerator.getValue(smaller, elementType, i)
    val getFromBigger = CodeGenerator.getValue(bigger, elementType, i)
    val javaElementClass = CodeGenerator.boxedType(elementType)
    val javaSet = classOf[java.util.HashSet[_]].getName
    val set = ctx.freshName("set")
    val addToSetFromSmallerCode = nullSafeElementCodegen(
      smaller, i, s"$set.add($getFromSmaller);", s"${ev.isNull} = true;")
    val elementIsInSetCode = nullSafeElementCodegen(
      bigger,
      i,
      s"""
         |if ($set.contains($getFromBigger)) {
         |  ${ev.isNull} = false;
         |  ${ev.value} = true;
         |  break;
         |}
       """.stripMargin,
      s"${ev.isNull} = true;")
    s"""
       |$javaSet<$javaElementClass> $set = new $javaSet<$javaElementClass>();
       |for (int $i = 0; $i < $smaller.numElements(); $i ++) {
       |  $addToSetFromSmallerCode
       |}
       |for (int $i = 0; $i < $bigger.numElements(); $i ++) {
       |  $elementIsInSetCode
       |}
     """.stripMargin
  }

  /**
   * Code generation for a slower evaluation which performs a nested loop and supports all the data types.
   */
  private def bruteForceCodegen(ctx: CodegenContext, ev: ExprCode, smaller: String, bigger: String): String = {
    val i = ctx.freshName("i")
    val j = ctx.freshName("j")
    val getFromSmaller = CodeGenerator.getValue(smaller, elementType, j)
    val getFromBigger = CodeGenerator.getValue(bigger, elementType, i)
    val compareValues = nullSafeElementCodegen(
      smaller,
      j,
      s"""
         |if (${ctx.genEqual(elementType, getFromSmaller, getFromBigger)}) {
         |  ${ev.isNull} = false;
         |  ${ev.value} = true;
         |}
       """.stripMargin,
      s"${ev.isNull} = true;")
    val isInSmaller = nullSafeElementCodegen(
      bigger,
      i,
      s"""
         |for (int $j = 0; $j < $smaller.numElements() && !${ev.value}; $j ++) {
         |  $compareValues
         |}
       """.stripMargin,
      s"${ev.isNull} = true;")
    s"""
       |for (int $i = 0; $i < $bigger.numElements() && !${ev.value}; $i ++) {
       |  $isInSmaller
       |}
     """.stripMargin
  }

  def nullSafeElementCodegen(
      arrayVar: String,
      index: String,
      code: String,
      isNullCode: String): String = {
    if (inputTypes.exists(_.asInstanceOf[ArrayType].containsNull)) {
      s"""
         |if ($arrayVar.isNullAt($index)) {
         |  $isNullCode
         |} else {
         |  $code
         |}
       """.stripMargin
    } else {
      code
    }
  }

  override def prettyName: String = "arrays_overlap"
}

/**
 * Slices an array according to the requested start index and length
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(x, start, length) - Subsets array x starting from index start (or starting from the end if start is negative) with the specified length.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3, 4), 2, 2);
       [2,3]
      > SELECT _FUNC_(array(1, 2, 3, 4), -2, 2);
       [3,4]
  """, since = "2.4.0")
// scalastyle:on line.size.limit
case class Slice(x: Expression, start: Expression, length: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = x.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, IntegerType, IntegerType)

  override def children: Seq[Expression] = Seq(x, start, length)

  lazy val elementType: DataType = x.dataType.asInstanceOf[ArrayType].elementType

  override def nullSafeEval(xVal: Any, startVal: Any, lengthVal: Any): Any = {
    val startInt = startVal.asInstanceOf[Int]
    val lengthInt = lengthVal.asInstanceOf[Int]
    val arr = xVal.asInstanceOf[ArrayData]
    val startIndex = if (startInt == 0) {
      throw new RuntimeException(
        s"Unexpected value for start in function $prettyName: SQL array indices start at 1.")
    } else if (startInt < 0) {
      startInt + arr.numElements()
    } else {
      startInt - 1
    }
    if (lengthInt < 0) {
      throw new RuntimeException(s"Unexpected value for length in function $prettyName: " +
        "length must be greater than or equal to 0.")
    }
    // startIndex can be negative if start is negative and its absolute value is greater than the
    // number of elements in the array
    if (startIndex < 0 || startIndex >= arr.numElements()) {
      return new GenericArrayData(Array.empty[AnyRef])
    }
    val data = arr.toSeq[AnyRef](elementType)
    new GenericArrayData(data.slice(startIndex, startIndex + lengthInt))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (x, start, length) => {
      val startIdx = ctx.freshName("startIdx")
      val resLength = ctx.freshName("resLength")
      val defaultIntValue = CodeGenerator.defaultValue(CodeGenerator.JAVA_INT, false)
      s"""
         |${CodeGenerator.JAVA_INT} $startIdx = $defaultIntValue;
         |${CodeGenerator.JAVA_INT} $resLength = $defaultIntValue;
         |if ($start == 0) {
         |  throw new RuntimeException("Unexpected value for start in function $prettyName: "
         |    + "SQL array indices start at 1.");
         |} else if ($start < 0) {
         |  $startIdx = $start + $x.numElements();
         |} else {
         |  // arrays in SQL are 1-based instead of 0-based
         |  $startIdx = $start - 1;
         |}
         |if ($length < 0) {
         |  throw new RuntimeException("Unexpected value for length in function $prettyName: "
         |    + "length must be greater than or equal to 0.");
         |} else if ($length > $x.numElements() - $startIdx) {
         |  $resLength = $x.numElements() - $startIdx;
         |} else {
         |  $resLength = $length;
         |}
         |${genCodeForResult(ctx, ev, x, startIdx, resLength)}
       """.stripMargin
    })
  }

  def genCodeForResult(
      ctx: CodegenContext,
      ev: ExprCode,
      inputArray: String,
      startIdx: String,
      resLength: String): String = {
    val values = ctx.freshName("values")
    val i = ctx.freshName("i")
    val getValue = CodeGenerator.getValue(inputArray, elementType, s"$i + $startIdx")
    if (!CodeGenerator.isPrimitiveType(elementType)) {
      val arrayClass = classOf[GenericArrayData].getName
      s"""
         |Object[] $values;
         |if ($startIdx < 0 || $startIdx >= $inputArray.numElements()) {
         |  $values = new Object[0];
         |} else {
         |  $values = new Object[$resLength];
         |  for (int $i = 0; $i < $resLength; $i ++) {
         |    $values[$i] = $getValue;
         |  }
         |}
         |${ev.value} = new $arrayClass($values);
       """.stripMargin
    } else {
      val primitiveValueTypeName = CodeGenerator.primitiveTypeName(elementType)
      s"""
         |if ($startIdx < 0 || $startIdx >= $inputArray.numElements()) {
         |  $resLength = 0;
         |}
         |${ctx.createUnsafeArray(values, resLength, elementType, s" $prettyName failed.")}
         |for (int $i = 0; $i < $resLength; $i ++) {
         |  if ($inputArray.isNullAt($i + $startIdx)) {
         |    $values.setNullAt($i);
         |  } else {
         |    $values.set$primitiveValueTypeName($i, $getValue);
         |  }
         |}
         |${ev.value} = $values;
       """.stripMargin
    }
  }
}

/**
 * Creates a String containing all the elements of the input array separated by the delimiter.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(array, delimiter[, nullReplacement]) - Concatenates the elements of the given array
      using the delimiter and an optional string to replace nulls. If no value is set for
      nullReplacement, any null value is filtered.""",
  examples = """
    Examples:
      > SELECT _FUNC_(array('hello', 'world'), ' ');
       hello world
      > SELECT _FUNC_(array('hello', null ,'world'), ' ');
       hello world
      > SELECT _FUNC_(array('hello', null ,'world'), ' ', ',');
       hello , world
  """, since = "2.4.0")
case class ArrayJoin(
    array: Expression,
    delimiter: Expression,
    nullReplacement: Option[Expression]) extends Expression with ExpectsInputTypes {

  def this(array: Expression, delimiter: Expression) = this(array, delimiter, None)

  def this(array: Expression, delimiter: Expression, nullReplacement: Expression) =
    this(array, delimiter, Some(nullReplacement))

  override def inputTypes: Seq[AbstractDataType] = if (nullReplacement.isDefined) {
    Seq(ArrayType(StringType), StringType, StringType)
  } else {
    Seq(ArrayType(StringType), StringType)
  }

  override def children: Seq[Expression] = if (nullReplacement.isDefined) {
    Seq(array, delimiter, nullReplacement.get)
  } else {
    Seq(array, delimiter)
  }

  override def nullable: Boolean = children.exists(_.nullable)

  override def foldable: Boolean = children.forall(_.foldable)

  override def eval(input: InternalRow): Any = {
    val arrayEval = array.eval(input)
    if (arrayEval == null) return null
    val delimiterEval = delimiter.eval(input)
    if (delimiterEval == null) return null
    val nullReplacementEval = nullReplacement.map(_.eval(input))
    if (nullReplacementEval.contains(null)) return null

    val buffer = new UTF8StringBuilder()
    var firstItem = true
    val nullHandling = nullReplacementEval match {
      case Some(rep) => (prependDelimiter: Boolean) => {
        if (!prependDelimiter) {
          buffer.append(delimiterEval.asInstanceOf[UTF8String])
        }
        buffer.append(rep.asInstanceOf[UTF8String])
        true
      }
      case None => (_: Boolean) => false
    }
    arrayEval.asInstanceOf[ArrayData].foreach(StringType, (_, item) => {
      if (item == null) {
        if (nullHandling(firstItem)) {
          firstItem = false
        }
      } else {
        if (!firstItem) {
          buffer.append(delimiterEval.asInstanceOf[UTF8String])
        }
        buffer.append(item.asInstanceOf[UTF8String])
        firstItem = false
      }
    })
    buffer.build()
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val code = nullReplacement match {
      case Some(replacement) =>
        val replacementGen = replacement.genCode(ctx)
        val nullHandling = (buffer: String, delimiter: String, firstItem: String) => {
          s"""
             |if (!$firstItem) {
             |  $buffer.append($delimiter);
             |}
             |$buffer.append(${replacementGen.value});
             |$firstItem = false;
           """.stripMargin
        }
        val execCode = if (replacement.nullable) {
          ctx.nullSafeExec(replacement.nullable, replacementGen.isNull) {
            genCodeForArrayAndDelimiter(ctx, ev, nullHandling)
          }
        } else {
          genCodeForArrayAndDelimiter(ctx, ev, nullHandling)
        }
        s"""
           |${replacementGen.code}
           |$execCode
         """.stripMargin
      case None => genCodeForArrayAndDelimiter(ctx, ev,
        (_: String, _: String, _: String) => "// nulls are ignored")
    }
    if (nullable) {
      ev.copy(
        code"""
           |boolean ${ev.isNull} = true;
           |UTF8String ${ev.value} = null;
           |$code
         """.stripMargin)
    } else {
      ev.copy(
        code"""
           |UTF8String ${ev.value} = null;
           |$code
         """.stripMargin, FalseLiteral)
    }
  }

  private def genCodeForArrayAndDelimiter(
      ctx: CodegenContext,
      ev: ExprCode,
      nullEval: (String, String, String) => String): String = {
    val arrayGen = array.genCode(ctx)
    val delimiterGen = delimiter.genCode(ctx)
    val buffer = ctx.freshName("buffer")
    val bufferClass = classOf[UTF8StringBuilder].getName
    val i = ctx.freshName("i")
    val firstItem = ctx.freshName("firstItem")
    val resultCode =
      s"""
         |$bufferClass $buffer = new $bufferClass();
         |boolean $firstItem = true;
         |for (int $i = 0; $i < ${arrayGen.value}.numElements(); $i ++) {
         |  if (${arrayGen.value}.isNullAt($i)) {
         |    ${nullEval(buffer, delimiterGen.value, firstItem)}
         |  } else {
         |    if (!$firstItem) {
         |      $buffer.append(${delimiterGen.value});
         |    }
         |    $buffer.append(${CodeGenerator.getValue(arrayGen.value, StringType, i)});
         |    $firstItem = false;
         |  }
         |}
         |${ev.value} = $buffer.build();""".stripMargin

    if (array.nullable || delimiter.nullable) {
      arrayGen.code + ctx.nullSafeExec(array.nullable, arrayGen.isNull) {
        delimiterGen.code + ctx.nullSafeExec(delimiter.nullable, delimiterGen.isNull) {
          s"""
             |${ev.isNull} = false;
             |$resultCode""".stripMargin
        }
      }
    } else {
      s"""
         |${arrayGen.code}
         |${delimiterGen.code}
         |$resultCode""".stripMargin
    }
  }

  override def dataType: DataType = StringType

}

/**
 * Returns the minimum value in the array.
 */
@ExpressionDescription(
  usage = "_FUNC_(array) - Returns the minimum value in the array. NULL elements are skipped.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 20, null, 3));
       1
  """, since = "2.4.0")
case class ArrayMin(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val typeCheckResult = super.checkInputDataTypes()
    if (typeCheckResult.isSuccess) {
      TypeUtils.checkForOrderingExpr(dataType, s"function $prettyName")
    } else {
      typeCheckResult
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    val javaType = CodeGenerator.javaType(dataType)
    val i = ctx.freshName("i")
    val item = ExprCode(EmptyBlock,
      isNull = JavaCode.isNullExpression(s"${childGen.value}.isNullAt($i)"),
      value = JavaCode.expression(CodeGenerator.getValue(childGen.value, dataType, i), dataType))
    ev.copy(code =
      code"""
         |${childGen.code}
         |boolean ${ev.isNull} = true;
         |$javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |if (!${childGen.isNull}) {
         |  for (int $i = 0; $i < ${childGen.value}.numElements(); $i ++) {
         |    ${ctx.reassignIfSmaller(dataType, ev, item)}
         |  }
         |}
      """.stripMargin)
  }

  override protected def nullSafeEval(input: Any): Any = {
    var min: Any = null
    input.asInstanceOf[ArrayData].foreach(dataType, (_, item) =>
      if (item != null && (min == null || ordering.lt(item, min))) {
        min = item
      }
    )
    min
  }

  override def dataType: DataType = child.dataType match {
    case ArrayType(dt, _) => dt
    case _ => throw new IllegalStateException(s"$prettyName accepts only arrays.")
  }

  override def prettyName: String = "array_min"
}

/**
 * Returns the maximum value in the array.
 */
@ExpressionDescription(
  usage = "_FUNC_(array) - Returns the maximum value in the array. NULL elements are skipped.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 20, null, 3));
       20
  """, since = "2.4.0")
case class ArrayMax(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val typeCheckResult = super.checkInputDataTypes()
    if (typeCheckResult.isSuccess) {
      TypeUtils.checkForOrderingExpr(dataType, s"function $prettyName")
    } else {
      typeCheckResult
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    val javaType = CodeGenerator.javaType(dataType)
    val i = ctx.freshName("i")
    val item = ExprCode(EmptyBlock,
      isNull = JavaCode.isNullExpression(s"${childGen.value}.isNullAt($i)"),
      value = JavaCode.expression(CodeGenerator.getValue(childGen.value, dataType, i), dataType))
    ev.copy(code =
      code"""
         |${childGen.code}
         |boolean ${ev.isNull} = true;
         |$javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |if (!${childGen.isNull}) {
         |  for (int $i = 0; $i < ${childGen.value}.numElements(); $i ++) {
         |    ${ctx.reassignIfGreater(dataType, ev, item)}
         |  }
         |}
      """.stripMargin)
  }

  override protected def nullSafeEval(input: Any): Any = {
    var max: Any = null
    input.asInstanceOf[ArrayData].foreach(dataType, (_, item) =>
      if (item != null && (max == null || ordering.gt(item, max))) {
        max = item
      }
    )
    max
  }

  override def dataType: DataType = child.dataType match {
    case ArrayType(dt, _) => dt
    case _ => throw new IllegalStateException(s"$prettyName accepts only arrays.")
  }

  override def prettyName: String = "array_max"
}


/**
 * Returns the position of the first occurrence of element in the given array as long.
 * Returns 0 if the given value could not be found in the array. Returns null if either of
 * the arguments are null
 *
 * NOTE: that this is not zero based, but 1-based index. The first element in the array has
 *       index 1.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(array, element) - Returns the (1-based) index of the first element of the array as long.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(3, 2, 1), 1);
       3
  """,
  since = "2.4.0")
case class ArrayPosition(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(right.dataType)

  override def dataType: DataType = LongType

  override def inputTypes: Seq[AbstractDataType] = {
    val elementType = left.dataType match {
      case t: ArrayType => t.elementType
      case _ => AnyDataType
    }
    Seq(ArrayType, elementType)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes() match {
      case f: TypeCheckResult.TypeCheckFailure => f
      case TypeCheckResult.TypeCheckSuccess =>
        TypeUtils.checkForOrderingExpr(right.dataType, s"function $prettyName")
    }
  }

  override def nullSafeEval(arr: Any, value: Any): Any = {
    arr.asInstanceOf[ArrayData].foreach(right.dataType, (i, v) =>
      if (v != null && ordering.equiv(v, value)) {
        return (i + 1).toLong
      }
    )
    0L
  }

  override def prettyName: String = "array_position"

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (arr, value) => {
      val pos = ctx.freshName("arrayPosition")
      val i = ctx.freshName("i")
      val getValue = CodeGenerator.getValue(arr, right.dataType, i)
      s"""
         |int $pos = 0;
         |for (int $i = 0; $i < $arr.numElements(); $i ++) {
         |  if (!$arr.isNullAt($i) && ${ctx.genEqual(right.dataType, value, getValue)}) {
         |    $pos = $i + 1;
         |    break;
         |  }
         |}
         |${ev.value} = (long) $pos;
       """.stripMargin
    })
  }
}

/**
 * Returns the value of index `right` in Array `left` or the value for key `right` in Map `left`.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(array, index) - Returns element of array at given (1-based) index. If index < 0,
      accesses elements from the last to the first. Returns NULL if the index exceeds the length
      of the array.

    _FUNC_(map, key) - Returns value for given key, or NULL if the key is not contained in the map
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), 2);
       2
      > SELECT _FUNC_(map(1, 'a', 2, 'b'), 2);
       "b"
  """,
  since = "2.4.0")
case class ElementAt(left: Expression, right: Expression) extends GetMapValueUtil {

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(left.dataType.asInstanceOf[MapType].keyType)

  override def dataType: DataType = left.dataType match {
    case ArrayType(elementType, _) => elementType
    case MapType(_, valueType, _) => valueType
  }

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(TypeCollection(ArrayType, MapType),
      left.dataType match {
        case _: ArrayType => IntegerType
        case _: MapType => left.dataType.asInstanceOf[MapType].keyType
        case _ => AnyDataType // no match for a wrong 'left' expression type
      }
    )
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes() match {
      case f: TypeCheckResult.TypeCheckFailure => f
      case TypeCheckResult.TypeCheckSuccess if left.dataType.isInstanceOf[MapType] =>
        TypeUtils.checkForOrderingExpr(
          left.dataType.asInstanceOf[MapType].keyType, s"function $prettyName")
      case TypeCheckResult.TypeCheckSuccess => TypeCheckResult.TypeCheckSuccess
    }
  }

  override def nullable: Boolean = true

  override def nullSafeEval(value: Any, ordinal: Any): Any = {
    left.dataType match {
      case _: ArrayType =>
        val array = value.asInstanceOf[ArrayData]
        val index = ordinal.asInstanceOf[Int]
        if (array.numElements() < math.abs(index)) {
          null
        } else {
          val idx = if (index == 0) {
            throw new ArrayIndexOutOfBoundsException("SQL array indices start at 1")
          } else if (index > 0) {
            index - 1
          } else {
            array.numElements() + index
          }
          if (left.dataType.asInstanceOf[ArrayType].containsNull && array.isNullAt(idx)) {
            null
          } else {
            array.get(idx, dataType)
          }
        }
      case _: MapType =>
        getValueEval(value, ordinal, left.dataType.asInstanceOf[MapType].keyType, ordering)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    left.dataType match {
      case _: ArrayType =>
        nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
          val index = ctx.freshName("elementAtIndex")
          val nullCheck = if (left.dataType.asInstanceOf[ArrayType].containsNull) {
            s"""
               |if ($eval1.isNullAt($index)) {
               |  ${ev.isNull} = true;
               |} else
             """.stripMargin
          } else {
            ""
          }
          s"""
             |int $index = (int) $eval2;
             |if ($eval1.numElements() < Math.abs($index)) {
             |  ${ev.isNull} = true;
             |} else {
             |  if ($index == 0) {
             |    throw new ArrayIndexOutOfBoundsException("SQL array indices start at 1");
             |  } else if ($index > 0) {
             |    $index--;
             |  } else {
             |    $index += $eval1.numElements();
             |  }
             |  $nullCheck
             |  {
             |    ${ev.value} = ${CodeGenerator.getValue(eval1, dataType, index)};
             |  }
             |}
           """.stripMargin
        })
      case _: MapType =>
        doGetValueGenCode(ctx, ev, left.dataType.asInstanceOf[MapType])
    }
  }

  override def prettyName: String = "element_at"
}

/**
 * Concatenates multiple input columns together into a single column.
 * The function works with strings, binary and compatible array columns.
 */
@ExpressionDescription(
  usage = "_FUNC_(col1, col2, ..., colN) - Returns the concatenation of col1, col2, ..., colN.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark', 'SQL');
       SparkSQL
      > SELECT _FUNC_(array(1, 2, 3), array(4, 5), array(6));
 |     [1,2,3,4,5,6]
  """)
case class Concat(children: Seq[Expression]) extends Expression {

  private val MAX_ARRAY_LENGTH: Int = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH

  val allowedTypes = Seq(StringType, BinaryType, ArrayType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.isEmpty) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      val childTypes = children.map(_.dataType)
      if (childTypes.exists(tpe => !allowedTypes.exists(_.acceptsType(tpe)))) {
        return TypeCheckResult.TypeCheckFailure(
          s"input to function $prettyName should have been ${StringType.simpleString}," +
            s" ${BinaryType.simpleString} or ${ArrayType.simpleString}, but it's " +
            childTypes.map(_.simpleString).mkString("[", ", ", "]"))
      }
      TypeUtils.checkForSameTypeInputExpr(childTypes, s"function $prettyName")
    }
  }

  override def dataType: DataType = children.map(_.dataType).headOption.getOrElse(StringType)

  lazy val javaType: String = CodeGenerator.javaType(dataType)

  override def nullable: Boolean = children.exists(_.nullable)

  override def foldable: Boolean = children.forall(_.foldable)

  override def eval(input: InternalRow): Any = dataType match {
    case BinaryType =>
      val inputs = children.map(_.eval(input).asInstanceOf[Array[Byte]])
      ByteArray.concat(inputs: _*)
    case StringType =>
      val inputs = children.map(_.eval(input).asInstanceOf[UTF8String])
      UTF8String.concat(inputs : _*)
    case ArrayType(elementType, _) =>
      val inputs = children.toStream.map(_.eval(input))
      if (inputs.contains(null)) {
        null
      } else {
        val arrayData = inputs.map(_.asInstanceOf[ArrayData])
        val numberOfElements = arrayData.foldLeft(0L)((sum, ad) => sum + ad.numElements())
        if (numberOfElements > MAX_ARRAY_LENGTH) {
          throw new RuntimeException(s"Unsuccessful try to concat arrays with $numberOfElements" +
            s" elements due to exceeding the array size limit $MAX_ARRAY_LENGTH.")
        }
        val finalData = new Array[AnyRef](numberOfElements.toInt)
        var position = 0
        for(ad <- arrayData) {
          val arr = ad.toObjectArray(elementType)
          Array.copy(arr, 0, finalData, position, arr.length)
          position += arr.length
        }
        new GenericArrayData(finalData)
      }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evals = children.map(_.genCode(ctx))
    val args = ctx.freshName("args")

    val inputs = evals.zipWithIndex.map { case (eval, index) =>
      s"""
        ${eval.code}
        if (!${eval.isNull}) {
          $args[$index] = ${eval.value};
        }
      """
    }

    val (concatenator, initCode) = dataType match {
      case BinaryType =>
        (classOf[ByteArray].getName, s"byte[][] $args = new byte[${evals.length}][];")
      case StringType =>
        ("UTF8String", s"UTF8String[] $args = new UTF8String[${evals.length}];")
      case ArrayType(elementType, _) =>
        val arrayConcatClass = if (CodeGenerator.isPrimitiveType(elementType)) {
          genCodeForPrimitiveArrays(ctx, elementType)
        } else {
          genCodeForNonPrimitiveArrays(ctx, elementType)
        }
        (arrayConcatClass, s"ArrayData[] $args = new ArrayData[${evals.length}];")
    }
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = inputs,
      funcName = "valueConcat",
      extraArguments = (s"$javaType[]", args) :: Nil)
    ev.copy(code"""
      $initCode
      $codes
      $javaType ${ev.value} = $concatenator.concat($args);
      boolean ${ev.isNull} = ${ev.value} == null;
    """)
  }

  private def genCodeForNumberOfElements(ctx: CodegenContext) : (String, String) = {
    val numElements = ctx.freshName("numElements")
    val code = s"""
        |long $numElements = 0L;
        |for (int z = 0; z < ${children.length}; z++) {
        |  $numElements += args[z].numElements();
        |}
        |if ($numElements > $MAX_ARRAY_LENGTH) {
        |  throw new RuntimeException("Unsuccessful try to concat arrays with " + $numElements +
        |    " elements due to exceeding the array size limit $MAX_ARRAY_LENGTH.");
        |}
      """.stripMargin

    (code, numElements)
  }

  private def nullArgumentProtection() : String = {
    if (nullable) {
      s"""
         |for (int z = 0; z < ${children.length}; z++) {
         |  if (args[z] == null) return null;
         |}
       """.stripMargin
    } else {
      ""
    }
  }

  private def genCodeForPrimitiveArrays(ctx: CodegenContext, elementType: DataType): String = {
    val counter = ctx.freshName("counter")
    val arrayData = ctx.freshName("arrayData")

    val (numElemCode, numElemName) = genCodeForNumberOfElements(ctx)

    val primitiveValueTypeName = CodeGenerator.primitiveTypeName(elementType)

    s"""
       |new Object() {
       |  public ArrayData concat($javaType[] args) {
       |    ${nullArgumentProtection()}
       |    $numElemCode
       |    ${ctx.createUnsafeArray(arrayData, numElemName, elementType, s" $prettyName failed.")}
       |    int $counter = 0;
       |    for (int y = 0; y < ${children.length}; y++) {
       |      for (int z = 0; z < args[y].numElements(); z++) {
       |        if (args[y].isNullAt(z)) {
       |          $arrayData.setNullAt($counter);
       |        } else {
       |          $arrayData.set$primitiveValueTypeName(
       |            $counter,
       |            ${CodeGenerator.getValue(s"args[y]", elementType, "z")}
       |          );
       |        }
       |        $counter++;
       |      }
       |    }
       |    return $arrayData;
       |  }
       |}""".stripMargin.stripPrefix("\n")
  }

  private def genCodeForNonPrimitiveArrays(ctx: CodegenContext, elementType: DataType): String = {
    val genericArrayClass = classOf[GenericArrayData].getName
    val arrayData = ctx.freshName("arrayObjects")
    val counter = ctx.freshName("counter")

    val (numElemCode, numElemName) = genCodeForNumberOfElements(ctx)

    s"""
       |new Object() {
       |  public ArrayData concat($javaType[] args) {
       |    ${nullArgumentProtection()}
       |    $numElemCode
       |    Object[] $arrayData = new Object[(int)$numElemName];
       |    int $counter = 0;
       |    for (int y = 0; y < ${children.length}; y++) {
       |      for (int z = 0; z < args[y].numElements(); z++) {
       |        $arrayData[$counter] = ${CodeGenerator.getValue(s"args[y]", elementType, "z")};
       |        $counter++;
       |      }
       |    }
       |    return new $genericArrayClass($arrayData);
       |  }
       |}""".stripMargin.stripPrefix("\n")
  }

  override def toString: String = s"concat(${children.mkString(", ")})"

  override def sql: String = s"concat(${children.map(_.sql).mkString(", ")})"
}

/**
 * Transforms an array of arrays into a single array.
 */
@ExpressionDescription(
  usage = "_FUNC_(arrayOfArrays) - Transforms an array of arrays into a single array.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(array(1, 2), array(3, 4));
       [1,2,3,4]
  """,
  since = "2.4.0")
case class Flatten(child: Expression) extends UnaryExpression {

  private val MAX_ARRAY_LENGTH = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH

  private lazy val childDataType: ArrayType = child.dataType.asInstanceOf[ArrayType]

  override def nullable: Boolean = child.nullable || childDataType.containsNull

  override def dataType: DataType = childDataType.elementType

  lazy val elementType: DataType = dataType.asInstanceOf[ArrayType].elementType

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case ArrayType(_: ArrayType, _) =>
      TypeCheckResult.TypeCheckSuccess
    case _ =>
      TypeCheckResult.TypeCheckFailure(
        s"The argument should be an array of arrays, " +
        s"but '${child.sql}' is of ${child.dataType.simpleString} type."
      )
  }

  override def nullSafeEval(child: Any): Any = {
    val elements = child.asInstanceOf[ArrayData].toObjectArray(dataType)

    if (elements.contains(null)) {
      null
    } else {
      val arrayData = elements.map(_.asInstanceOf[ArrayData])
      val numberOfElements = arrayData.foldLeft(0L)((sum, e) => sum + e.numElements())
      if (numberOfElements > MAX_ARRAY_LENGTH) {
        throw new RuntimeException("Unsuccessful try to flatten an array of arrays with " +
          s"$numberOfElements elements due to exceeding the array size limit $MAX_ARRAY_LENGTH.")
      }
      val flattenedData = new Array(numberOfElements.toInt)
      var position = 0
      for (ad <- arrayData) {
        val arr = ad.toObjectArray(elementType)
        Array.copy(arr, 0, flattenedData, position, arr.length)
        position += arr.length
      }
      new GenericArrayData(flattenedData)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => {
      val code = if (CodeGenerator.isPrimitiveType(elementType)) {
        genCodeForFlattenOfPrimitiveElements(ctx, c, ev.value)
      } else {
        genCodeForFlattenOfNonPrimitiveElements(ctx, c, ev.value)
      }
      if (childDataType.containsNull) nullElementsProtection(ev, c, code) else code
    })
  }

  private def nullElementsProtection(
      ev: ExprCode,
      childVariableName: String,
      coreLogic: String): String = {
    s"""
    |for (int z = 0; !${ev.isNull} && z < $childVariableName.numElements(); z++) {
    |  ${ev.isNull} |= $childVariableName.isNullAt(z);
    |}
    |if (!${ev.isNull}) {
    |  $coreLogic
    |}
    """.stripMargin
  }

  private def genCodeForNumberOfElements(
      ctx: CodegenContext,
      childVariableName: String) : (String, String) = {
    val variableName = ctx.freshName("numElements")
    val code = s"""
      |long $variableName = 0;
      |for (int z = 0; z < $childVariableName.numElements(); z++) {
      |  $variableName += $childVariableName.getArray(z).numElements();
      |}
      |if ($variableName > $MAX_ARRAY_LENGTH) {
      |  throw new RuntimeException("Unsuccessful try to flatten an array of arrays with " +
      |    $variableName + " elements due to exceeding the array size limit $MAX_ARRAY_LENGTH.");
      |}
      """.stripMargin
    (code, variableName)
  }

  private def genCodeForFlattenOfPrimitiveElements(
      ctx: CodegenContext,
      childVariableName: String,
      arrayDataName: String): String = {
    val counter = ctx.freshName("counter")
    val tempArrayDataName = ctx.freshName("tempArrayData")

    val (numElemCode, numElemName) = genCodeForNumberOfElements(ctx, childVariableName)

    val primitiveValueTypeName = CodeGenerator.primitiveTypeName(elementType)

    s"""
    |$numElemCode
    |${ctx.createUnsafeArray(tempArrayDataName, numElemName, elementType, s" $prettyName failed.")}
    |int $counter = 0;
    |for (int k = 0; k < $childVariableName.numElements(); k++) {
    |  ArrayData arr = $childVariableName.getArray(k);
    |  for (int l = 0; l < arr.numElements(); l++) {
    |   if (arr.isNullAt(l)) {
    |     $tempArrayDataName.setNullAt($counter);
    |   } else {
    |     $tempArrayDataName.set$primitiveValueTypeName(
    |       $counter,
    |       ${CodeGenerator.getValue("arr", elementType, "l")}
    |     );
    |   }
    |   $counter++;
    | }
    |}
    |$arrayDataName = $tempArrayDataName;
    """.stripMargin
  }

  private def genCodeForFlattenOfNonPrimitiveElements(
      ctx: CodegenContext,
      childVariableName: String,
      arrayDataName: String): String = {
    val genericArrayClass = classOf[GenericArrayData].getName
    val arrayName = ctx.freshName("arrayObject")
    val counter = ctx.freshName("counter")
    val (numElemCode, numElemName) = genCodeForNumberOfElements(ctx, childVariableName)

    s"""
    |$numElemCode
    |Object[] $arrayName = new Object[(int)$numElemName];
    |int $counter = 0;
    |for (int k = 0; k < $childVariableName.numElements(); k++) {
    |  ArrayData arr = $childVariableName.getArray(k);
    |  for (int l = 0; l < arr.numElements(); l++) {
    |    $arrayName[$counter] = ${CodeGenerator.getValue("arr", elementType, "l")};
    |    $counter++;
    |  }
    |}
    |$arrayDataName = new $genericArrayClass($arrayName);
    """.stripMargin
  }

  override def prettyName: String = "flatten"
}

/**
 * Returns the array containing the given input value (left) count (right) times.
 */
@ExpressionDescription(
  usage = "_FUNC_(element, count) - Returns the array containing element count times.",
  examples = """
    Examples:
      > SELECT _FUNC_('123', 2);
       ['123', '123']
  """,
  since = "2.4.0")
case class ArrayRepeat(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  private val MAX_ARRAY_LENGTH = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH

  override def dataType: ArrayType = ArrayType(left.dataType, left.nullable)

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegerType)

  override def nullable: Boolean = right.nullable

  override def eval(input: InternalRow): Any = {
    val count = right.eval(input)
    if (count == null) {
      null
    } else {
      if (count.asInstanceOf[Int] > MAX_ARRAY_LENGTH) {
        throw new RuntimeException(s"Unsuccessful try to create array with $count elements " +
          s"due to exceeding the array size limit $MAX_ARRAY_LENGTH.");
      }
      val element = left.eval(input)
      new GenericArrayData(Array.fill(count.asInstanceOf[Int])(element))
    }
  }

  override def prettyName: String = "array_repeat"

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    val element = leftGen.value
    val count = rightGen.value
    val et = dataType.elementType

    val coreLogic = if (CodeGenerator.isPrimitiveType(et)) {
      genCodeForPrimitiveElement(ctx, et, element, count, leftGen.isNull, ev.value)
    } else {
      genCodeForNonPrimitiveElement(ctx, element, count, leftGen.isNull, ev.value)
    }
    val resultCode = nullElementsProtection(ev, rightGen.isNull, coreLogic)

    ev.copy(code =
      code"""
         |boolean ${ev.isNull} = false;
         |${leftGen.code}
         |${rightGen.code}
         |${CodeGenerator.javaType(dataType)} ${ev.value} =
         |  ${CodeGenerator.defaultValue(dataType)};
         |$resultCode
       """.stripMargin)
  }

  private def nullElementsProtection(
      ev: ExprCode,
      rightIsNull: String,
      coreLogic: String): String = {
    if (nullable) {
      s"""
         |if ($rightIsNull) {
         |  ${ev.isNull} = true;
         |} else {
         |  ${coreLogic}
         |}
       """.stripMargin
    } else {
      coreLogic
    }
  }

  private def genCodeForNumberOfElements(ctx: CodegenContext, count: String): (String, String) = {
    val numElements = ctx.freshName("numElements")
    val numElementsCode =
      s"""
         |int $numElements = 0;
         |if ($count > 0) {
         |  $numElements = $count;
         |}
         |if ($numElements > $MAX_ARRAY_LENGTH) {
         |  throw new RuntimeException("Unsuccessful try to create array with " + $numElements +
         |    " elements due to exceeding the array size limit $MAX_ARRAY_LENGTH.");
         |}
       """.stripMargin

    (numElements, numElementsCode)
  }

  private def genCodeForPrimitiveElement(
      ctx: CodegenContext,
      elementType: DataType,
      element: String,
      count: String,
      leftIsNull: String,
      arrayDataName: String): String = {
    val tempArrayDataName = ctx.freshName("tempArrayData")
    val primitiveValueTypeName = CodeGenerator.primitiveTypeName(elementType)
    val errorMessage = s" $prettyName failed."
    val (numElemName, numElemCode) = genCodeForNumberOfElements(ctx, count)

    s"""
       |$numElemCode
       |${ctx.createUnsafeArray(tempArrayDataName, numElemName, elementType, errorMessage)}
       |if (!$leftIsNull) {
       |  for (int k = 0; k < $tempArrayDataName.numElements(); k++) {
       |    $tempArrayDataName.set$primitiveValueTypeName(k, $element);
       |  }
       |} else {
       |  for (int k = 0; k < $tempArrayDataName.numElements(); k++) {
       |    $tempArrayDataName.setNullAt(k);
       |  }
       |}
       |$arrayDataName = $tempArrayDataName;
     """.stripMargin
  }

  private def genCodeForNonPrimitiveElement(
      ctx: CodegenContext,
      element: String,
      count: String,
      leftIsNull: String,
      arrayDataName: String): String = {
    val genericArrayClass = classOf[GenericArrayData].getName
    val arrayName = ctx.freshName("arrayObject")
    val (numElemName, numElemCode) = genCodeForNumberOfElements(ctx, count)

    s"""
       |$numElemCode
       |Object[] $arrayName = new Object[(int)$numElemName];
       |if (!$leftIsNull) {
       |  for (int k = 0; k < $numElemName; k++) {
       |    $arrayName[k] = $element;
       |  }
       |}
       |$arrayDataName = new $genericArrayClass($arrayName);
     """.stripMargin
  }

}

/**
 * Remove all elements that equal to element from the given array
 */
@ExpressionDescription(
  usage = "_FUNC_(array, element) - Remove all elements that equal to element from array.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3, null, 3), 3);
       [1,2,null]
  """, since = "2.4.0")
case class ArrayRemove(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = left.dataType

  override def inputTypes: Seq[AbstractDataType] = {
    val elementType = left.dataType match {
      case t: ArrayType => t.elementType
      case _ => AnyDataType
    }
    Seq(ArrayType, elementType)
  }

  lazy val elementType: DataType = left.dataType.asInstanceOf[ArrayType].elementType

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(right.dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes() match {
      case f: TypeCheckResult.TypeCheckFailure => f
      case TypeCheckResult.TypeCheckSuccess =>
        TypeUtils.checkForOrderingExpr(right.dataType, s"function $prettyName")
    }
  }

  override def nullSafeEval(arr: Any, value: Any): Any = {
    val newArray = new Array[Any](arr.asInstanceOf[ArrayData].numElements())
    var pos = 0
    arr.asInstanceOf[ArrayData].foreach(right.dataType, (i, v) =>
      if (v == null || !ordering.equiv(v, value)) {
        newArray(pos) = v
        pos += 1
      }
    )
    new GenericArrayData(newArray.slice(0, pos))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (arr, value) => {
      val numsToRemove = ctx.freshName("numsToRemove")
      val newArraySize = ctx.freshName("newArraySize")
      val i = ctx.freshName("i")
      val getValue = CodeGenerator.getValue(arr, elementType, i)
      val isEqual = ctx.genEqual(elementType, value, getValue)
      s"""
         |int $numsToRemove = 0;
         |for (int $i = 0; $i < $arr.numElements(); $i ++) {
         |  if (!$arr.isNullAt($i) && $isEqual) {
         |    $numsToRemove = $numsToRemove + 1;
         |  }
         |}
         |int $newArraySize = $arr.numElements() - $numsToRemove;
         |${genCodeForResult(ctx, ev, arr, value, newArraySize)}
       """.stripMargin
    })
  }

  def genCodeForResult(
      ctx: CodegenContext,
      ev: ExprCode,
      inputArray: String,
      value: String,
      newArraySize: String): String = {
    val values = ctx.freshName("values")
    val i = ctx.freshName("i")
    val pos = ctx.freshName("pos")
    val getValue = CodeGenerator.getValue(inputArray, elementType, i)
    val isEqual = ctx.genEqual(elementType, value, getValue)
    if (!CodeGenerator.isPrimitiveType(elementType)) {
      val arrayClass = classOf[GenericArrayData].getName
      s"""
         |int $pos = 0;
         |Object[] $values = new Object[$newArraySize];
         |for (int $i = 0; $i < $inputArray.numElements(); $i ++) {
         |  if ($inputArray.isNullAt($i)) {
         |    $values[$pos] = null;
         |    $pos = $pos + 1;
         |  }
         |  else {
         |    if (!($isEqual)) {
         |      $values[$pos] = $getValue;
         |      $pos = $pos + 1;
         |    }
         |  }
         |}
         |${ev.value} = new $arrayClass($values);
       """.stripMargin
    } else {
      val primitiveValueTypeName = CodeGenerator.primitiveTypeName(elementType)
      s"""
         |${ctx.createUnsafeArray(values, newArraySize, elementType, s" $prettyName failed.")}
         |int $pos = 0;
         |for (int $i = 0; $i < $inputArray.numElements(); $i ++) {
         |  if ($inputArray.isNullAt($i)) {
         |      $values.setNullAt($pos);
         |      $pos = $pos + 1;
         |  }
         |  else {
         |    if (!($isEqual)) {
         |      $values.set$primitiveValueTypeName($pos, $getValue);
         |      $pos = $pos + 1;
         |    }
         |  }
         |}
         |${ev.value} = $values;
       """.stripMargin
    }
  }

  override def prettyName: String = "array_remove"
}
