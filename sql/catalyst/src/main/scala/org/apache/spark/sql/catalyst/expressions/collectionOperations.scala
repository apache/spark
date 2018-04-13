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
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

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
  extends BinaryExpression with ExpectsInputTypes with CodegenFallback {

  def this(e: Expression) = this(e, Literal(true))

  override def left: Expression = base
  override def right: Expression = ascendingOrder
  override def dataType: DataType = base.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, BooleanType)

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
        s"""
           |boolean ${ev.isNull} = true;
           |UTF8String ${ev.value} = null;
           |$code
         """.stripMargin)
    } else {
      ev.copy(
        s"""
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
