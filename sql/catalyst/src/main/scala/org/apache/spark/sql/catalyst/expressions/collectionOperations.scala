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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._

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
 * Sorts the input array in ascending / descending order according to the natural ordering of
 * the array elements and returns it.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(array[, ascendingOrder]) - Sorts the input array in ascending or descending order according to the natural ordering of the array elements.",
  examples = """
    Examples:
      > SELECT _FUNC_(array('b', 'd', 'c', 'a'), true);
       ["a","b","c","d"]
  """)
// scalastyle:on line.size.limit
case class SortArray(base: Expression, ascendingOrder: Expression)
  extends BinaryExpression with ExpectsInputTypes with CodegenFallback with OrderSpecified {

  def this(e: Expression) = this(e, Literal(true))

  override def left: Expression = base
  override def right: Expression = ascendingOrder
  override def dataType: DataType = base.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, BooleanType)

  override def checkInputDataTypes(): TypeCheckResult = base.dataType match {
    case ArrayType(dt, _) if TypeUtils.isOrderable(dt) =>
      ascendingOrder match {
        case Literal(_: Boolean, BooleanType) =>
          TypeCheckResult.TypeCheckSuccess
        case _ =>
          TypeCheckResult.TypeCheckFailure(
            "Sort order in second argument requires a boolean literal.")
      }
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
      case _ @ ArrayType(m: MapType, _) => m.interpretedOrdering.asInstanceOf[Ordering[Any]]
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
      case _ @ ArrayType(m: MapType, _) => m.interpretedOrdering.asInstanceOf[Ordering[Any]]
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
      TypeCheckResult.TypeCheckSuccess
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
      } else if (v == value) {
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
      val getValue = ctx.getValue(arr, right.dataType, i)
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
 * This expression orders all maps in an expression's result. This expression enables the use of
 * maps in comparisons and equality operations.
 */
case class OrderMaps(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(ArrayType, MapType, StructType))

  /** Create a data type in which all maps are ordered. */
  private[this] def createDataType(dataType: DataType): DataType = dataType match {
    case StructType(fields) =>
      StructType(fields.map { field =>
        field.copy(dataType = createDataType(field.dataType))
      })
    case ArrayType(elementType, containsNull) =>
      ArrayType(createDataType(elementType), containsNull)
    case MapType(keyType, valueType, valueContainsNull, false) =>
      MapType(
        createDataType(keyType),
        createDataType(valueType),
        valueContainsNull,
        ordered = true)
    case _ =>
      dataType
  }

  override lazy val dataType: DataType = createDataType(child.dataType)

  private[this] val identity = (id: Any) => id

  /**
   * Create a function that transforms a Spark SQL datum to a new datum for which all MapData
   * elements have been ordered.
   */
  private[this] def createTransform(dataType: DataType): Option[Any => Any] = {
    dataType match {
      case m @ MapType(keyType, valueType, _, false) =>
        val keyTransform = createTransform(keyType).getOrElse(identity)
        val valueTransform = createTransform(valueType).getOrElse(identity)
        val ordering = Ordering.Tuple2(m.interpretedKeyOrdering, m.interpretedValueOrdering)
        Option((data: Any) => {
          val input = data.asInstanceOf[MapData]
          val length = input.numElements()
          val buffer = Array.ofDim[(Any, Any)](length)

          // Move the entries into a temporary buffer.
          var i = 0
          val keys = input.keyArray()
          val values = input.valueArray()
          while (i < length) {
            val key = keyTransform(keys.get(i, keyType))
            val value = if (!values.isNullAt(i)) {
              valueTransform(values.get(i, valueType))
            } else {
              null
            }
            buffer(i) = key -> value
            i += 1
          }

          // Sort the buffer.
          java.util.Arrays.sort(buffer, ordering)

          // Recreate the map data.
          i = 0
          val sortedKeys = Array.ofDim[Any](length)
          val sortedValues = Array.ofDim[Any](length)
          while (i < length) {
            sortedKeys(i) = buffer(i)._1
            sortedValues(i) = buffer(i)._2
            i += 1
          }
          ArrayBasedMapData(sortedKeys, sortedValues)
        })
      case ArrayType(dt, _) =>
        createTransform(dt).map { transform =>
          data: Any => {
            val input = data.asInstanceOf[ArrayData]
            val length = input.numElements()
            val output = Array.ofDim[Any](length)
            var i = 0
            while (i < length) {
              if (!input.isNullAt(i)) {
                output(i) = transform(input.get(i, dt))
              }
              i += 1
            }
            new GenericArrayData(output)
          }
        }
      case StructType(fields) =>
        val transformOpts = fields.map { field =>
          createTransform(field.dataType)
        }
        // Only transform a struct if a meaningful transformation has been defined.
        if (transformOpts.exists(_.isDefined)) {
          val transforms = transformOpts.zip(fields).map { case (opt, field) =>
            val dataType = field.dataType
            val transform = opt.getOrElse(identity)
            (input: InternalRow, i: Int) => {
              transform(input.get(i, dataType))
            }
          }
          val length = fields.length
          val tf = (data: Any) => {
            val input = data.asInstanceOf[InternalRow]
            val output = Array.ofDim[Any](length)
            var i = 0
            while (i < length) {
              if (!input.isNullAt(i)) {
                output(i) = transforms(i)(input, i)
              }
              i += 1
            }
            new GenericInternalRow(output)
          }
          Some(tf)
        } else {
          None
        }
      case _ => None
    }
  }

  @transient private[this] lazy val transform = {
    createTransform(child.dataType).getOrElse(identity)
  }

  override protected def nullSafeEval(input: Any): Any = transform(input)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // TODO we should code generate this.
    val tf = ctx.addReferenceObj("transform", transform, classOf[Any => Any].getCanonicalName)
    nullSafeCodeGen(ctx, ev, eval => {
      s"${ev.value} = (${ctx.boxedType(dataType)})$tf.apply($eval);"
    })
  }
}
