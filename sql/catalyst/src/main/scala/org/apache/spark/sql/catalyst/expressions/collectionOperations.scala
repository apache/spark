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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData, TypeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}

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

  val allowedTypes = Seq(StringType, BinaryType, ArrayType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.isEmpty) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      val childTypes = children.map(_.dataType)
      if (childTypes.exists(tpe => !allowedTypes.exists(_.acceptsType(tpe)))) {
        return TypeCheckResult.TypeCheckFailure(
          s"input to function $prettyName should have been StringType, BinaryType or ArrayType," +
            s" but it's " + childTypes.map(_.simpleString).mkString("[", ", ", "]"))
      }
      TypeUtils.checkForSameTypeInputExpr(childTypes, s"function $prettyName")
    }
  }

  override def dataType: DataType = children.map(_.dataType).headOption.getOrElse(StringType)

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
        val elements = inputs.flatMap(_.asInstanceOf[ArrayData].toObjectArray(elementType))
        new GenericArrayData(elements)
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
          genCodeForPrimitiveArrayConcat(ctx, elementType, evals.length)
        } else {
          genCodeForComplexArrayConcat(ctx, evals.length)
        }
        (arrayConcatClass, s"ArrayData[] $args = new ArrayData[${evals.length}];")
    }
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = inputs,
      funcName = "valueConcat",
      extraArguments = (s"${CodeGenerator.javaType(dataType)}[]", args) :: Nil)
    ev.copy(s"""
      $initCode
      $codes
      ${CodeGenerator.javaType(dataType)} ${ev.value} = $concatenator.concat($args);
      boolean ${ev.isNull} = ${ev.value} == null;
    """)
  }

  private def genCodeForNumberOfElements(
      ctx: CodegenContext,
      argsLength: Int) : (String, String) = {
    val variableName = ctx.freshName("numElements")
    val code = (0 until argsLength)
      .map(idx => s"$variableName += args[$idx].numElements();")
      .foldLeft(s"int $variableName = 0;")((acc, s) => acc + "\n" + s)
    (code, variableName)
  }

  private def nullArgumentProtection(argsLength: Int) : String =
  {
    if (nullable) {
      (0 until argsLength).map(idx => s"if (args[$idx] == null) return null;").mkString("\n")
    } else {
      ""
    }
  }

  private def genCodeForPrimitiveArrayConcat(
      ctx: CodegenContext,
      elementType: DataType,
      argsLength: Int): String = {
    val arrayName = ctx.freshName("array")
    val arraySizeName = ctx.freshName("size")
    val counter = ctx.freshName("counter")
    val arrayDataName = ctx.freshName("arrayData")

    val (numElemCode, numElemName) = genCodeForNumberOfElements(ctx, argsLength)

    val unsafeArraySizeInBytes = s"""
      |int $arraySizeName = UnsafeArrayData.calculateHeaderPortionInBytes($numElemName) +
      |${classOf[ByteArrayMethods].getName}.roundNumberOfBytesToNearestWord(
      |  ${elementType.defaultSize} * $numElemName
      |);
      """.stripMargin
    val baseOffset = Platform.BYTE_ARRAY_OFFSET

    val primitiveValueTypeName = CodeGenerator.primitiveTypeName(elementType)
    val assignments = (0 until argsLength).map { idx =>
      s"""
         |for (int z = 0; z < args[$idx].numElements(); z++) {
         |  if (args[$idx].isNullAt(z)) {
         |    $arrayDataName.setNullAt($counter);
         |  } else {
         |    $arrayDataName.set$primitiveValueTypeName(
         |      $counter,
         |      args[$idx].get$primitiveValueTypeName(z)
         |    );
         |  }
         |  $counter++;
         |}
        """.stripMargin
    }.mkString("\n")

    s"""new Object() {
       |  public ArrayData concat(${CodeGenerator.javaType(dataType)}[] args) {
       |    ${nullArgumentProtection(argsLength)}
       |    $numElemCode
       |    $unsafeArraySizeInBytes
       |    byte[] $arrayName = new byte[$arraySizeName];
       |    UnsafeArrayData $arrayDataName = new UnsafeArrayData();
       |    Platform.putLong($arrayName, $baseOffset, $numElemName);
       |    $arrayDataName.pointTo($arrayName, $baseOffset, $arraySizeName);
       |    int $counter = 0;
       |    $assignments
       |    return $arrayDataName;
       |  }
       |}""".stripMargin
  }

  private def genCodeForComplexArrayConcat(
    ctx: CodegenContext,
    argsLength: Int): String = {
    val genericArrayClass = classOf[GenericArrayData].getName
    val arrayName = ctx.freshName("arrayObject")
    val counter = ctx.freshName("counter")

    val (numElemCode, numElemName) = genCodeForNumberOfElements(ctx, argsLength)

    val assignments = (0 until argsLength).map { idx =>
      s"""
         |for (int z = 0; z < args[$idx].numElements(); z++) {
         |  $arrayName[$counter] = args[$idx].array()[z];
         |  $counter++;
         |}
        """.stripMargin
    }.mkString("\n")

    s"""new Object() {
       |  public ArrayData concat(${CodeGenerator.javaType(dataType)}[] args) {
       |    ${nullArgumentProtection(argsLength)}
       |    $numElemCode
       |    Object[] $arrayName = new Object[$numElemName];
       |    int $counter = 0;
       |    $assignments
       |    return new $genericArrayClass($arrayName);
       |  }
       |}""".stripMargin
  }


  override def toString: String = s"concat(${children.mkString(", ")})"

  override def sql: String = s"concat(${children.map(_.sql).mkString(", ")})"
}
