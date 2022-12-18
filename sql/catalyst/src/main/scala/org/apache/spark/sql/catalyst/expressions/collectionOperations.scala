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

import java.time.{Duration, Period, ZoneId}
import java.util.Comparator

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion, UnresolvedAttribute, UnresolvedSeed}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.ArraySortLike.NullOrder
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.{BinaryLike, SQLQueryContext, UnaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern.{ARRAYS_ZIP, CONCAT, TreePattern}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.errors.{QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SQLOpenHashSet
import org.apache.spark.unsafe.UTF8StringBuilder
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.array.ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH
import org.apache.spark.unsafe.types.{ByteArray, CalendarInterval, UTF8String}

/**
 * Base trait for [[BinaryExpression]]s with two arrays of the same element type and implicit
 * casting.
 */
trait BinaryArrayExpressionWithImplicitCast
  extends BinaryExpression
  with ImplicitCastInputTypes
  with QueryErrorsBase {

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
      case _ =>
        DataTypeMismatch(
          errorSubClass = "BINARY_ARRAY_DIFF_TYPES",
          messageParameters = Map(
            "functionName" -> toSQLId(prettyName),
            "arrayType" -> toSQLType(ArrayType),
            "leftType" -> toSQLType(left.dataType),
            "rightType" -> toSQLType(right.dataType)
          )
        )
    }
  }

  protected def leftArrayElementNullable = left.dataType.asInstanceOf[ArrayType].containsNull
  protected def rightArrayElementNullable = right.dataType.asInstanceOf[ArrayType].containsNull
}


/**
 * Given an array or map, returns total number of elements in it.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the size of an array or a map.
    The function returns null for null input if spark.sql.legacy.sizeOfNull is set to false or
    spark.sql.ansi.enabled is set to true. Otherwise, the function returns -1 for null input.
    With the default settings, the function returns -1 for null input.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array('b', 'd', 'c', 'a'));
       4
      > SELECT _FUNC_(map('a', 1, 'b', 2));
       2
  """,
  since = "1.5.0",
  group = "collection_funcs")
case class Size(child: Expression, legacySizeOfNull: Boolean)
  extends UnaryExpression with ExpectsInputTypes {

  def this(child: Expression) = this(child, SQLConf.get.legacySizeOfNull)

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(ArrayType, MapType))
  override def nullable: Boolean = if (legacySizeOfNull) false else super.nullable

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      if (legacySizeOfNull) -1 else null
    } else child.dataType match {
      case _: ArrayType => value.asInstanceOf[ArrayData].numElements()
      case _: MapType => value.asInstanceOf[MapData].numElements()
      case other => throw QueryExecutionErrors.unsupportedOperandTypeForSizeFunctionError(other)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (legacySizeOfNull) {
      val childGen = child.genCode(ctx)
      ev.copy(code = code"""
      boolean ${ev.isNull} = false;
      ${childGen.code}
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${childGen.isNull} ? -1 :
        (${childGen.value}).numElements();""", isNull = FalseLiteral)
    } else {
      defineCodeGen(ctx, ev, c => s"($c).numElements()")
    }
  }

  override protected def withNewChildInternal(newChild: Expression): Size = copy(child = newChild)
}

object Size {
  def apply(child: Expression): Size = new Size(child)
}


/**
 * Given an array, returns total number of elements in it.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the size of an array. The function returns null for null input.",
  examples = """
    Examples:
      > SELECT _FUNC_(array('b', 'd', 'c', 'a'));
       4
  """,
  since = "3.3.0",
  group = "collection_funcs")
case class ArraySize(child: Expression)
  extends RuntimeReplaceable with ImplicitCastInputTypes with UnaryLike[Expression] {

  override lazy val replacement: Expression = Size(child, legacySizeOfNull = false)

  override def prettyName: String = "array_size"

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  protected def withNewChildInternal(newChild: Expression): ArraySize = copy(child = newChild)
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
  """,
  group = "map_funcs",
  since = "2.0.0")
case class MapKeys(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  override def dataType: DataType = ArrayType(child.dataType.asInstanceOf[MapType].keyType)

  override def nullSafeEval(map: Any): Any = {
    map.asInstanceOf[MapData].keyArray()
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"${ev.value} = ($c).keyArray();")
  }

  override def prettyName: String = "map_keys"

  override protected def withNewChildInternal(newChild: Expression): MapKeys =
    copy(child = newChild)
}


/**
 * Returns an unordered array containing the keys of the map.
 */
@ExpressionDescription(
  usage = "_FUNC_(map, key) - Returns true if the map contains the key.",
  examples = """
    Examples:
      > SELECT _FUNC_(map(1, 'a', 2, 'b'), 1);
       true
      > SELECT _FUNC_(map(1, 'a', 2, 'b'), 3);
       false
  """,
  group = "map_funcs",
  since = "3.3.0")
case class MapContainsKey(left: Expression, right: Expression)
  extends RuntimeReplaceable
  with BinaryLike[Expression]
  with ImplicitCastInputTypes
  with QueryErrorsBase {

  override lazy val replacement: Expression = ArrayContains(MapKeys(left), right)

  override def inputTypes: Seq[AbstractDataType] = {
    (left.dataType, right.dataType) match {
      case (_, NullType) => Seq.empty
      case (MapType(kt, vt, valueContainsNull), dt) =>
        TypeCoercion.findWiderTypeWithoutStringPromotionForTwo(kt, dt) match {
          case Some(widerType) => Seq(MapType(widerType, vt, valueContainsNull), widerType)
          case _ => Seq.empty
        }
      case _ => Seq.empty
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (_, NullType) =>
        DataTypeMismatch(
          errorSubClass = "NULL_TYPE",
          Map("functionName" -> toSQLId(prettyName)))
      case (MapType(kt, _, _), dt) if kt.sameType(dt) =>
        TypeUtils.checkForOrderingExpr(kt, prettyName)
      case _ =>
        DataTypeMismatch(
          errorSubClass = "MAP_FUNCTION_DIFF_TYPES",
          messageParameters = Map(
            "functionName" -> toSQLId(prettyName),
            "dataType" -> toSQLType(MapType),
            "leftType" -> toSQLType(left.dataType),
            "rightType" -> toSQLType(right.dataType)
          )
        )
    }
  }

  override def prettyName: String = "map_contains_key"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression = {
    copy(newLeft, newRight)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(a1, a2, ...) - Returns a merged array of structs in which the N-th struct contains all
    N-th values of input arrays.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), array(2, 3, 4));
       [{"0":1,"1":2},{"0":2,"1":3},{"0":3,"1":4}]
      > SELECT _FUNC_(array(1, 2), array(2, 3), array(3, 4));
       [{"0":1,"1":2,"2":3},{"0":2,"1":3,"2":4}]
  """,
  group = "array_funcs",
  since = "2.4.0")
case class ArraysZip(children: Seq[Expression], names: Seq[Expression])
  extends Expression with ExpectsInputTypes {

  def this(children: Seq[Expression]) = {
    this(
      children,
      children.zipWithIndex.map {
        case (u: UnresolvedAttribute, _) => Literal(u.nameParts.last)
        case (e: NamedExpression, _) if e.resolved => Literal(e.name)
        case (e: NamedExpression, _) => NamePlaceholder
        case (g: GetStructField, _) => Literal(g.extractFieldName)
        case (g: GetArrayStructFields, _) => Literal(g.field.name)
        case (g: GetMapValue, _) => Literal(g.key)
        case (_, idx) => Literal(idx.toString)
      })
  }

  if (children.size != names.size) {
    throw new IllegalArgumentException(
      "The numbers of zipped arrays and field names should be the same")
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(ARRAYS_ZIP)

  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess && names.forall(_.resolved)
  override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.length)(ArrayType)

  @transient override lazy val dataType: DataType = {
    val fields = arrayElementTypes.zip(names).map {
      case (elementType, Literal(name, StringType)) =>
        StructField(name.toString, elementType, nullable = true)
    }
    ArrayType(StructType(fields), containsNull = false)
  }

  override def nullable: Boolean = children.exists(_.nullable)

  @transient private lazy val arrayElementTypes =
    children.map(_.dataType.asInstanceOf[ArrayType].elementType)

  private def genericArrayData = classOf[GenericArrayData].getName

  def emptyInputGenCode(ev: ExprCode): ExprCode = {
    ev.copy(code"""
      |${CodeGenerator.javaType(dataType)} ${ev.value} = new $genericArrayData(new Object[0]);
      |boolean ${ev.isNull} = false;
    """.stripMargin)
  }

  def nonEmptyInputGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val genericInternalRow = classOf[GenericInternalRow].getName
    val arrVals = ctx.freshName("arrVals")
    val biggestCardinality = ctx.freshName("biggestCardinality")

    val currentRow = ctx.freshName("currentRow")
    val j = ctx.freshName("j")
    val i = ctx.freshName("i")
    val args = ctx.freshName("args")

    val evals = children.map(_.genCode(ctx))
    val getValuesAndCardinalities = evals.zipWithIndex.map { case (eval, index) =>
      s"""
        |if ($biggestCardinality != -1) {
        |  ${eval.code}
        |  if (!${eval.isNull}) {
        |    $arrVals[$index] = ${eval.value};
        |    $biggestCardinality = Math.max($biggestCardinality, ${eval.value}.numElements());
        |  } else {
        |    $biggestCardinality = -1;
        |  }
        |}
      """.stripMargin
    }

    val splittedGetValuesAndCardinalities = ctx.splitExpressionsWithCurrentInputs(
      expressions = getValuesAndCardinalities,
      funcName = "getValuesAndCardinalities",
      returnType = "int",
      makeSplitFunction = body =>
        s"""
          |$body
          |return $biggestCardinality;
        """.stripMargin,
      foldFunctions = _.map(funcCall => s"$biggestCardinality = $funcCall;").mkString("\n"),
      extraArguments =
        ("ArrayData[]", arrVals) ::
        ("int", biggestCardinality) :: Nil)

    val getValueForType = arrayElementTypes.zipWithIndex.map { case (eleType, idx) =>
      val g = CodeGenerator.getValue(s"$arrVals[$idx]", eleType, i)
      s"""
        |if ($i < $arrVals[$idx].numElements() && !$arrVals[$idx].isNullAt($i)) {
        |  $currentRow[$idx] = $g;
        |} else {
        |  $currentRow[$idx] = null;
        |}
      """.stripMargin
    }

    val getValueForTypeSplitted = ctx.splitExpressions(
      expressions = getValueForType,
      funcName = "extractValue",
      arguments =
        ("int", i) ::
        ("Object[]", currentRow) ::
        ("ArrayData[]", arrVals) :: Nil)

    val initVariables = s"""
      |ArrayData[] $arrVals = new ArrayData[${children.length}];
      |int $biggestCardinality = 0;
      |${CodeGenerator.javaType(dataType)} ${ev.value} = null;
    """.stripMargin

    ev.copy(code"""
      |$initVariables
      |$splittedGetValuesAndCardinalities
      |boolean ${ev.isNull} = $biggestCardinality == -1;
      |if (!${ev.isNull}) {
      |  Object[] $args = new Object[$biggestCardinality];
      |  for (int $i = 0; $i < $biggestCardinality; $i ++) {
      |    Object[] $currentRow = new Object[${children.length}];
      |    $getValueForTypeSplitted
      |    $args[$i] = new $genericInternalRow($currentRow);
      |  }
      |  ${ev.value} = new $genericArrayData($args);
      |}
    """.stripMargin)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (children.isEmpty) {
      emptyInputGenCode(ev)
    } else {
      nonEmptyInputGenCode(ctx, ev)
    }
  }

  override def eval(input: InternalRow): Any = {
    val inputArrays = children.map(_.eval(input).asInstanceOf[ArrayData])
    if (inputArrays.contains(null)) {
      null
    } else {
      val biggestCardinality = if (inputArrays.isEmpty) {
        0
      } else {
        inputArrays.map(_.numElements()).max
      }

      val result = new Array[InternalRow](biggestCardinality)
      val zippedArrs: Seq[(ArrayData, Int)] = inputArrays.zipWithIndex

      for (i <- 0 until biggestCardinality) {
        val currentLayer: Seq[Object] = zippedArrs.map { case (arr, index) =>
          if (i < arr.numElements() && !arr.isNullAt(i)) {
            arr.get(i, arrayElementTypes(index))
          } else {
            null
          }
        }

        result(i) = InternalRow.apply(currentLayer: _*)
      }
      new GenericArrayData(result)
    }
  }

  override def prettyName: String = "arrays_zip"

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): ArraysZip =
    copy(children = newChildren)
}

object ArraysZip {
  def apply(children: Seq[Expression]): ArraysZip = {
    new ArraysZip(children)
  }
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
  """,
  group = "map_funcs",
  since = "2.0.0")
case class MapValues(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  override def dataType: DataType = ArrayType(child.dataType.asInstanceOf[MapType].valueType)

  override def nullSafeEval(map: Any): Any = {
    map.asInstanceOf[MapData].valueArray()
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"${ev.value} = ($c).valueArray();")
  }

  override def prettyName: String = "map_values"

  override protected def withNewChildInternal(newChild: Expression): MapValues =
    copy(child = newChild)
}

/**
 * Returns an unordered array of all entries in the given map.
 */
@ExpressionDescription(
  usage = "_FUNC_(map) - Returns an unordered array of all entries in the given map.",
  examples = """
    Examples:
      > SELECT _FUNC_(map(1, 'a', 2, 'b'));
       [{"key":1,"value":"a"},{"key":2,"value":"b"}]
  """,
  group = "map_funcs",
  since = "3.0.0")
case class MapEntries(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  @transient private lazy val childDataType: MapType = child.dataType.asInstanceOf[MapType]

  private lazy val internalDataType: DataType = {
    ArrayType(
      StructType(
        StructField("key", childDataType.keyType, false) ::
        StructField("value", childDataType.valueType, childDataType.valueContainsNull) ::
        Nil),
      false)
  }

  override def dataType: DataType = internalDataType

  override protected def nullSafeEval(input: Any): Any = {
    val childMap = input.asInstanceOf[MapData]
    val keys = childMap.keyArray()
    val values = childMap.valueArray()
    val length = childMap.numElements()
    val resultData = new Array[AnyRef](length)
    var i = 0
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
      val arrayData = ctx.freshName("arrayData")
      val numElements = ctx.freshName("numElements")
      val keys = ctx.freshName("keys")
      val values = ctx.freshName("values")
      val isKeyPrimitive = CodeGenerator.isPrimitiveType(childDataType.keyType)
      val isValuePrimitive = CodeGenerator.isPrimitiveType(childDataType.valueType)

      val wordSize = UnsafeRow.WORD_SIZE
      val structSize = UnsafeRow.calculateBitSetWidthInBytes(2) + wordSize * 2
      val (isPrimitive, elementSize) = if (isKeyPrimitive && isValuePrimitive) {
        (true, structSize + wordSize)
      } else {
        (false, -1)
      }

      val allocation =
        s"""
           |ArrayData $arrayData = ArrayData.allocateArrayData(
           |  $elementSize, $numElements, " $prettyName failed.");
         """.stripMargin

      val code = if (isPrimitive) {
        val genCodeForPrimitive = genCodeForPrimitiveElements(
          ctx, arrayData, keys, values, ev.value, numElements, structSize)
        s"""
           |if ($arrayData instanceof UnsafeArrayData) {
           |  $genCodeForPrimitive
           |} else {
           |  ${genCodeForAnyElements(ctx, arrayData, keys, values, ev.value, numElements)}
           |}
         """.stripMargin
      } else {
        s"${genCodeForAnyElements(ctx, arrayData, keys, values, ev.value, numElements)}"
      }

      s"""
         |final int $numElements = $c.numElements();
         |final ArrayData $keys = $c.keyArray();
         |final ArrayData $values = $c.valueArray();
         |$allocation
         |$code
       """.stripMargin
    })
  }

  private def getKey(varName: String, index: String) =
    CodeGenerator.getValue(varName, childDataType.keyType, index)

  private def getValue(varName: String, index: String) =
    CodeGenerator.getValue(varName, childDataType.valueType, index)

  private def genCodeForPrimitiveElements(
      ctx: CodegenContext,
      arrayData: String,
      keys: String,
      values: String,
      resultArrayData: String,
      numElements: String,
      structSize: Int): String = {
    val unsafeArrayData = ctx.freshName("unsafeArrayData")
    val baseObject = ctx.freshName("baseObject")
    val unsafeRow = ctx.freshName("unsafeRow")
    val structsOffset = ctx.freshName("structsOffset")
    val offset = ctx.freshName("offset")
    val z = ctx.freshName("z")
    val calculateHeader = "UnsafeArrayData.calculateHeaderPortionInBytes"

    val baseOffset = "Platform.BYTE_ARRAY_OFFSET"
    val wordSize = UnsafeRow.WORD_SIZE
    val structSizeAsLong = s"${structSize}L"

    val setKey = CodeGenerator.setColumn(unsafeRow, childDataType.keyType, 0, getKey(keys, z))

    val valueAssignmentChecked = CodeGenerator.createArrayAssignment(
      unsafeRow, childDataType.valueType, values, "1", z, childDataType.valueContainsNull)

    s"""
       |UnsafeArrayData $unsafeArrayData = (UnsafeArrayData)$arrayData;
       |Object $baseObject = $unsafeArrayData.getBaseObject();
       |final int $structsOffset = $calculateHeader($numElements) + $numElements * $wordSize;
       |UnsafeRow $unsafeRow = new UnsafeRow(2);
       |for (int $z = 0; $z < $numElements; $z++) {
       |  long $offset = $structsOffset + $z * $structSizeAsLong;
       |  $unsafeArrayData.setLong($z, ($offset << 32) + $structSizeAsLong);
       |  $unsafeRow.pointTo($baseObject, $baseOffset + $offset, $structSize);
       |  $setKey;
       |  $valueAssignmentChecked
       |}
       |$resultArrayData = $arrayData;
     """.stripMargin
  }

  private def genCodeForAnyElements(
      ctx: CodegenContext,
      arrayData: String,
      keys: String,
      values: String,
      resultArrayData: String,
      numElements: String): String = {
    val z = ctx.freshName("z")
    val isValuePrimitive = CodeGenerator.isPrimitiveType(childDataType.valueType)
    val getValueWithCheck = if (childDataType.valueContainsNull && isValuePrimitive) {
      s"$values.isNullAt($z) ? null : (Object)${getValue(values, z)}"
    } else {
      getValue(values, z)
    }

    val rowClass = classOf[GenericInternalRow].getName
    val genericArrayDataClass = classOf[GenericArrayData].getName
    val genericArrayData = ctx.freshName("genericArrayData")
    val rowObject = s"new $rowClass(new Object[]{${getKey(keys, z)}, $getValueWithCheck})"
    s"""
       |$genericArrayDataClass $genericArrayData = ($genericArrayDataClass)$arrayData;
       |for (int $z = 0; $z < $numElements; $z++) {
       |  $genericArrayData.update($z, $rowObject);
       |}
       |$resultArrayData = $arrayData;
     """.stripMargin
  }

  override def prettyName: String = "map_entries"

  override def withNewChildInternal(newChild: Expression): MapEntries = copy(child = newChild)
}

/**
 * Returns the union of all the given maps.
 */
@ExpressionDescription(
  usage = "_FUNC_(map, ...) - Returns the union of all the given maps",
  examples = """
    Examples:
      > SELECT _FUNC_(map(1, 'a', 2, 'b'), map(3, 'c'));
       {1:"a",2:"b",3:"c"}
  """,
  group = "map_funcs",
  since = "2.4.0")
case class MapConcat(children: Seq[Expression])
  extends ComplexTypeMergingExpression
  with QueryErrorsBase {

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.exists(!_.dataType.isInstanceOf[MapType])) {
      DataTypeMismatch(
        errorSubClass = "MAP_CONCAT_DIFF_TYPES",
        messageParameters = Map(
          "functionName" -> toSQLId(prettyName),
          "dataType" -> children.map(_.dataType).map(toSQLType).mkString("[", ", ", "]")
        )
      )
    } else {
      val sameTypeCheck = TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), prettyName)
      if (sameTypeCheck.isFailure) {
        sameTypeCheck
      } else {
        TypeUtils.checkForMapKeyType(dataType.keyType)
      }
    }
  }

  @transient override lazy val dataType: MapType = {
    if (children.isEmpty) {
      MapType(StringType, StringType)
    } else {
      super.dataType.asInstanceOf[MapType]
    }
  }

  override def nullable: Boolean = children.exists(_.nullable)

  private lazy val mapBuilder = new ArrayBasedMapBuilder(dataType.keyType, dataType.valueType)

  override def eval(input: InternalRow): Any = {
    val maps = children.map(_.eval(input).asInstanceOf[MapData])
    if (maps.contains(null)) {
      return null
    }

    for (map <- maps) {
      mapBuilder.putAll(map.keyArray(), map.valueArray())
    }
    mapBuilder.build()
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val mapCodes = children.map(_.genCode(ctx))
    val argsName = ctx.freshName("args")
    val hasNullName = ctx.freshName("hasNull")
    val builderTerm = ctx.addReferenceObj("mapBuilder", mapBuilder)

    val assignments = mapCodes.zip(children.map(_.nullable)).zipWithIndex.map {
      case ((m, true), i) =>
        s"""
           |if (!$hasNullName) {
           |  ${m.code}
           |  if (!${m.isNull}) {
           |    $argsName[$i] = ${m.value};
           |  } else {
           |    $hasNullName = true;
           |  }
           |}
         """.stripMargin
      case ((m, false), i) =>
        s"""
           |if (!$hasNullName) {
           |  ${m.code}
           |  $argsName[$i] = ${m.value};
           |}
         """.stripMargin
    }

    val prepareMaps = ctx.splitExpressionsWithCurrentInputs(
      expressions = assignments,
      funcName = "getMapConcatInputs",
      extraArguments = ("MapData[]", argsName) :: ("boolean", hasNullName) :: Nil,
      returnType = "boolean",
      makeSplitFunction = body =>
        s"""
           |$body
           |return $hasNullName;
        """.stripMargin,
      foldFunctions = _.map(funcCall => s"$hasNullName = $funcCall;").mkString("\n")
    )

    val idxName = ctx.freshName("idx")
    val mapMerge =
      s"""
        |for (int $idxName = 0; $idxName < $argsName.length; $idxName++) {
        |  $builderTerm.putAll($argsName[$idxName].keyArray(), $argsName[$idxName].valueArray());
        |}
        |${ev.value} = $builderTerm.build();
      """.stripMargin

    ev.copy(
      code = code"""
        |MapData[] $argsName = new MapData[${mapCodes.size}];
        |boolean $hasNullName = false;
        |$prepareMaps
        |boolean ${ev.isNull} = $hasNullName;
        |MapData ${ev.value} = null;
        |if (!$hasNullName) {
        |  $mapMerge
        |}
      """.stripMargin)
  }

  override def prettyName: String = "map_concat"

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): MapConcat =
    copy(children = newChildren)
}

/**
 * Returns a map created from the given array of entries.
 */
@ExpressionDescription(
  usage = "_FUNC_(arrayOfEntries) - Returns a map created from the given array of entries.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(struct(1, 'a'), struct(2, 'b')));
       {1:"a",2:"b"}
  """,
  group = "map_funcs",
  since = "2.4.0")
case class MapFromEntries(child: Expression)
  extends UnaryExpression
  with NullIntolerant
  with QueryErrorsBase {

  @transient
  private lazy val dataTypeDetails: Option[(MapType, Boolean, Boolean)] = child.dataType match {
    case ArrayType(
      StructType(Array(
        StructField(_, keyType, keyNullable, _),
        StructField(_, valueType, valueNullable, _))),
      containsNull) => Some((MapType(keyType, valueType, valueNullable), keyNullable, containsNull))
    case _ => None
  }

  @transient private lazy val nullEntries: Boolean = dataTypeDetails.get._3

  override def nullable: Boolean = child.nullable || nullEntries

  @transient override lazy val dataType: MapType = dataTypeDetails.get._1

  override def checkInputDataTypes(): TypeCheckResult = dataTypeDetails match {
    case Some((mapType, _, _)) =>
      TypeUtils.checkForMapKeyType(mapType.keyType)
    case None =>
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> "1",
          "requiredType" -> s"${toSQLType(ArrayType)} of pair ${toSQLType(StructType)}",
          "inputSql" -> toSQLExpr(child),
          "inputType" -> toSQLType(child.dataType)
        )
      )
  }

  private lazy val mapBuilder = new ArrayBasedMapBuilder(dataType.keyType, dataType.valueType)

  override protected def nullSafeEval(input: Any): Any = {
    val entries = input.asInstanceOf[ArrayData]
    val numEntries = entries.numElements()
    var i = 0
    if (nullEntries) {
      while (i < numEntries) {
        if (entries.isNullAt(i)) return null
        i += 1
      }
    }

    i = 0
    while (i < numEntries) {
      mapBuilder.put(entries.getStruct(i, 2))
      i += 1
    }
    mapBuilder.build()
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => {
      val numEntries = ctx.freshName("numEntries")
      val builderTerm = ctx.addReferenceObj("mapBuilder", mapBuilder)
      val i = ctx.freshName("idx")
      ctx.nullArrayElementsSaveExec(nullEntries, ev.isNull, c) {
        s"""
           |final int $numEntries = $c.numElements();
           |for (int $i = 0; $i < $numEntries; $i++) {
           |  $builderTerm.put($c.getStruct($i, 2));
           |}
           |${ev.value} = $builderTerm.build();
         """.stripMargin
      }
    })
  }

  override def prettyName: String = "map_from_entries"

  override protected def withNewChildInternal(newChild: Expression): MapFromEntries =
    copy(child = newChild)
}


/**
 * Common base class for [[SortArray]] and [[ArraySort]].
 */
trait ArraySortLike extends ExpectsInputTypes {
  protected def arrayExpression: Expression

  protected def nullOrder: NullOrder

  @transient private lazy val lt: Comparator[Any] = {
    val ordering = arrayExpression.dataType match {
      case _ @ ArrayType(n: AtomicType, _) => n.ordering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(a: ArrayType, _) => a.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(s: StructType, _) => s.interpretedOrdering.asInstanceOf[Ordering[Any]]
    }

    (o1: Any, o2: Any) => {
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

  @transient private lazy val gt: Comparator[Any] = {
    val ordering = arrayExpression.dataType match {
      case _ @ ArrayType(n: AtomicType, _) => n.ordering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(a: ArrayType, _) => a.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case _ @ ArrayType(s: StructType, _) => s.interpretedOrdering.asInstanceOf[Ordering[Any]]
    }

    (o1: Any, o2: Any) => {
      if (o1 == null && o2 == null) {
        0
      } else if (o1 == null) {
        -nullOrder
      } else if (o2 == null) {
        nullOrder
      } else {
        ordering.compare(o2, o1)
      }
    }
  }

  @transient lazy val elementType: DataType =
    arrayExpression.dataType.asInstanceOf[ArrayType].elementType

  private def resultArrayElementNullable: Boolean =
    arrayExpression.dataType.asInstanceOf[ArrayType].containsNull

  def sortEval(array: Any, ascending: Boolean): Any = {
    val data = array.asInstanceOf[ArrayData].toArray[AnyRef](elementType)
    if (elementType != NullType) {
      java.util.Arrays.sort(data, if (ascending) lt else gt)
    }
    new GenericArrayData(data.asInstanceOf[Array[Any]])
  }

  def sortCodegen(ctx: CodegenContext, ev: ExprCode, base: String, order: String): String = {
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
      val canPerformFastSort = CodeGenerator.isPrimitiveType(elementType) &&
        elementType != BooleanType && !resultArrayElementNullable
      val nonNullPrimitiveAscendingSort = if (canPerformFastSort) {
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
      according to the natural ordering of the array elements. NaN is greater than any non-NaN
      elements for double/float type. Null elements will be placed at the beginning of the returned
      array in ascending order or at the end of the returned array in descending order.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array('b', 'd', null, 'c', 'a'), true);
       [null,"a","b","c","d"]
  """,
  group = "array_funcs",
  since = "1.5.0")
// scalastyle:on line.size.limit
case class SortArray(base: Expression, ascendingOrder: Expression)
  extends BinaryExpression with ArraySortLike with NullIntolerant with QueryErrorsBase {

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
          DataTypeMismatch(
            errorSubClass = "UNEXPECTED_INPUT_TYPE",
            messageParameters = Map(
              "paramIndex" -> "2",
              "requiredType" -> toSQLType(BooleanType),
              "inputSql" -> toSQLExpr(ascendingOrder),
              "inputType" -> toSQLType(ascendingOrder.dataType))
          )
      }
    case ArrayType(dt, _) =>
      DataTypeMismatch(
        errorSubClass = "INVALID_ORDERING_TYPE",
        messageParameters = Map(
          "functionName" -> toSQLId(prettyName),
          "dataType" -> toSQLType(base.dataType)
        )
      )
    case _ =>
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> "1",
          "requiredType" -> toSQLType(ArrayType),
          "inputSql" -> toSQLExpr(base),
          "inputType" -> toSQLType(base.dataType))
      )
  }

  override def nullSafeEval(array: Any, ascending: Any): Any = {
    sortEval(array, ascending.asInstanceOf[Boolean])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (b, order) => sortCodegen(ctx, ev, b, order))
  }

  override def prettyName: String = "sort_array"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): SortArray =
    copy(base = newLeft, ascendingOrder = newRight)
}

/**
 * Returns a random permutation of the given array.
 */
@ExpressionDescription(
  usage = "_FUNC_(array) - Returns a random permutation of the given array.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 20, 3, 5));
       [3,1,5,20]
      > SELECT _FUNC_(array(1, 20, null, 3));
       [20,null,3,1]
  """,
  note = """
    The function is non-deterministic.
  """,
  group = "array_funcs",
  since = "2.4.0")
case class Shuffle(child: Expression, randomSeed: Option[Long] = None)
  extends UnaryExpression with ExpectsInputTypes with Stateful with ExpressionWithRandomSeed {

  def this(child: Expression) = this(child, None)

  override def seedExpression: Expression = randomSeed.map(Literal.apply).getOrElse(UnresolvedSeed)

  override def withNewSeed(seed: Long): Shuffle = copy(randomSeed = Some(seed))

  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess && randomSeed.isDefined

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  override def dataType: DataType = child.dataType

  private def resultArrayElementNullable = dataType.asInstanceOf[ArrayType].containsNull

  @transient lazy val elementType: DataType = dataType.asInstanceOf[ArrayType].elementType

  @transient private[this] var random: RandomIndicesGenerator = _

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    random = RandomIndicesGenerator(randomSeed.get + partitionIndex)
  }

  override protected def evalInternal(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      val source = value.asInstanceOf[ArrayData]
      val numElements = source.numElements()
      val indices = random.getNextIndices(numElements)
      new GenericArrayData(indices.map(source.get(_, elementType)))
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => shuffleArrayCodeGen(ctx, ev, c))
  }

  private def shuffleArrayCodeGen(ctx: CodegenContext, ev: ExprCode, childName: String): String = {
    val randomClass = classOf[RandomIndicesGenerator].getName

    val rand = ctx.addMutableState(randomClass, "rand", forceInline = true)
    ctx.addPartitionInitializationStatement(
      s"$rand = new $randomClass(${randomSeed.get}L + partitionIndex);")

    val numElements = ctx.freshName("numElements")
    val arrayData = ctx.freshName("arrayData")
    val indices = ctx.freshName("indices")
    val i = ctx.freshName("i")

    val initialization = CodeGenerator.createArrayData(
      arrayData, elementType, numElements, s" $prettyName failed.")
    val assignment = CodeGenerator.createArrayAssignment(arrayData, elementType, childName,
      i, s"$indices[$i]", resultArrayElementNullable)

    s"""
       |int $numElements = $childName.numElements();
       |int[] $indices = $rand.getNextIndices($numElements);
       |$initialization
       |for (int $i = 0; $i < $numElements; $i++) {
       |  $assignment
       |}
       |${ev.value} = $arrayData;
     """.stripMargin
  }

  override def freshCopy(): Shuffle = Shuffle(child, randomSeed)

  override def withNewChildInternal(newChild: Expression): Shuffle = copy(child = newChild)
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
       [3,4,1,2]
  """,
  group = "collection_funcs",
  since = "1.5.0",
  note = """
    Reverse logic for arrays is available since 2.4.0.
  """
)
case class Reverse(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  // Input types are utilized by type coercion in ImplicitTypeCasts.
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(StringType, ArrayType))

  override def dataType: DataType = child.dataType

  private def resultArrayElementNullable = dataType.asInstanceOf[ArrayType].containsNull

  override def nullSafeEval(input: Any): Any = doReverse(input)

  @transient private lazy val doReverse: Any => Any = dataType match {
    case ArrayType(elementType, _) =>
      input => {
        val arrayData = input.asInstanceOf[ArrayData]
        new GenericArrayData(arrayData.toObjectArray(elementType).reverse)
      }
    case StringType => _.asInstanceOf[UTF8String].reverse()
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

    val numElements = ctx.freshName("numElements")
    val arrayData = ctx.freshName("arrayData")

    val i = ctx.freshName("i")
    val j = ctx.freshName("j")

    val elementType = dataType.asInstanceOf[ArrayType].elementType
    val initialization = CodeGenerator.createArrayData(
      arrayData, elementType, numElements, s" $prettyName failed.")
    val assignment = CodeGenerator.createArrayAssignment(
      arrayData, elementType, childName, i, j, resultArrayElementNullable)

    s"""
       |final int $numElements = $childName.numElements();
       |$initialization
       |for (int $i = 0; $i < $numElements; $i++) {
       |  int $j = $numElements - $i - 1;
       |  $assignment
       |}
       |${ev.value} = $arrayData;
     """.stripMargin
  }

  override def prettyName: String = "reverse"

  override protected def withNewChildInternal(newChild: Expression): Reverse =
    copy(child = newChild)
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
  """,
  group = "array_funcs",
  since = "1.5.0")
case class ArrayContains(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant with Predicate
  with QueryErrorsBase {

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(right.dataType)

  override def inputTypes: Seq[AbstractDataType] = {
    (left.dataType, right.dataType) match {
      case (_, NullType) => Seq.empty
      case (ArrayType(e1, hasNull), e2) =>
        TypeCoercion.findWiderTypeWithoutStringPromotionForTwo(e1, e2) match {
          case Some(dt) => Seq(ArrayType(dt, hasNull), dt)
          case _ => Seq.empty
        }
      case _ => Seq.empty
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (_, NullType) | (NullType, _) =>
        DataTypeMismatch(
          errorSubClass = "NULL_TYPE",
          messageParameters = Map("functionName" -> toSQLId(prettyName)))
      case (l, _) if !ArrayType.acceptsType(l) =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "1",
            "requiredType" -> toSQLType(ArrayType),
            "inputSql" -> toSQLExpr(left),
            "inputType" -> toSQLType(left.dataType))
        )
      case (ArrayType(e1, _), e2) if e1.sameType(e2) =>
        TypeUtils.checkForOrderingExpr(e2, prettyName)
      case _ =>
        DataTypeMismatch(
          errorSubClass = "ARRAY_FUNCTION_DIFF_TYPES",
          messageParameters = Map(
            "functionName" -> toSQLId(prettyName),
            "dataType" -> toSQLType(ArrayType),
            "leftType" -> toSQLType(left.dataType),
            "rightType" -> toSQLType(right.dataType)
          )
        )
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
      val loopBodyCode = if (nullable) {
        s"""
           |if ($arr.isNullAt($i)) {
           |   ${ev.isNull} = true;
           |} else if (${ctx.genEqual(right.dataType, value, getValue)}) {
           |   ${ev.isNull} = false;
           |   ${ev.value} = true;
           |   break;
           |}
         """.stripMargin
      } else {
        s"""
           |if (${ctx.genEqual(right.dataType, value, getValue)}) {
           |  ${ev.value} = true;
           |  break;
           |}
         """.stripMargin
      }
      s"""
         |for (int $i = 0; $i < $arr.numElements(); $i ++) {
         |  $loopBodyCode
         |}
       """.stripMargin
    })
  }

  override def prettyName: String = "array_contains"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): ArrayContains =
    copy(left = newLeft, right = newRight)
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
  """,
  group = "array_funcs",
  since = "2.4.0")
// scalastyle:off line.size.limit
case class ArraysOverlap(left: Expression, right: Expression)
  extends BinaryArrayExpressionWithImplicitCast with NullIntolerant with Predicate {

  override def checkInputDataTypes(): TypeCheckResult = super.checkInputDataTypes() match {
    case TypeCheckResult.TypeCheckSuccess =>
      TypeUtils.checkForOrderingExpr(elementType, prettyName)
    case failure => failure
  }

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(elementType)

  @transient private lazy val doEvaluation = if (TypeUtils.typeWithProperEquals(elementType)) {
    fastEval _
  } else {
    bruteForceEval _
  }

  override def nullable: Boolean = {
    left.nullable || right.nullable || leftArrayElementNullable || rightArrayElementNullable
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
      val smallestSet = new java.util.HashSet[Any]()
      smaller.foreach(elementType, (_, v) =>
        if (v == null) {
          hasNull = true
        } else {
          smallestSet.add(v)
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
      val comparisonCode = if (TypeUtils.typeWithProperEquals(elementType)) {
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
    val setIsNullCode = if (nullable) s"${ev.isNull} = false;" else ""
    val elementIsInSetCode = nullSafeElementCodegen(
      bigger,
      i,
      s"""
         |if ($set.contains($getFromBigger)) {
         |  $setIsNullCode
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
    val setIsNullCode = if (nullable) s"${ev.isNull} = false;" else ""
    val compareValues = nullSafeElementCodegen(
      smaller,
      j,
      s"""
         |if (${ctx.genEqual(elementType, getFromSmaller, getFromBigger)}) {
         |  $setIsNullCode
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

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): ArraysOverlap =
    copy(left = newLeft, right = newRight)
}

/**
 * Slices an array according to the requested start index and length
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(x, start, length) - Subsets array x starting from index start (array indices start at 1, or starting from the end if start is negative) with the specified length.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3, 4), 2, 2);
       [2,3]
      > SELECT _FUNC_(array(1, 2, 3, 4), -2, 2);
       [3,4]
  """,
  group = "array_funcs",
  since = "2.4.0")
// scalastyle:on line.size.limit
case class Slice(x: Expression, start: Expression, length: Expression)
  extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def dataType: DataType = x.dataType

  private def resultArrayElementNullable = dataType.asInstanceOf[ArrayType].containsNull

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, IntegerType, IntegerType)

  override def first: Expression = x
  override def second: Expression = start
  override def third: Expression = length

  @transient private lazy val elementType: DataType = x.dataType.asInstanceOf[ArrayType].elementType

  override def nullSafeEval(xVal: Any, startVal: Any, lengthVal: Any): Any = {
    val startInt = startVal.asInstanceOf[Int]
    val lengthInt = lengthVal.asInstanceOf[Int]
    val arr = xVal.asInstanceOf[ArrayData]
    val startIndex = if (startInt == 0) {
      throw QueryExecutionErrors.unexpectedValueForStartInFunctionError(prettyName)
    } else if (startInt < 0) {
      startInt + arr.numElements()
    } else {
      startInt - 1
    }
    if (lengthInt < 0) {
      throw QueryExecutionErrors.unexpectedValueForLengthInFunctionError(prettyName)
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
         |  throw QueryExecutionErrors.unexpectedValueForStartInFunctionError("$prettyName");
         |} else if ($start < 0) {
         |  $startIdx = $start + $x.numElements();
         |} else {
         |  // arrays in SQL are 1-based instead of 0-based
         |  $startIdx = $start - 1;
         |}
         |if ($length < 0) {
         |  throw QueryExecutionErrors.unexpectedValueForLengthInFunctionError("$prettyName");
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
    val genericArrayData = classOf[GenericArrayData].getName

    val allocation = CodeGenerator.createArrayData(
      values, elementType, resLength, s" $prettyName failed.")
    val assignment = CodeGenerator.createArrayAssignment(values, elementType, inputArray,
      i, s"$i + $startIdx", resultArrayElementNullable)

    s"""
       |if ($startIdx < 0 || $startIdx >= $inputArray.numElements()) {
       |  ${ev.value} = new $genericArrayData(new Object[0]);
       |} else {
       |  $allocation
       |  for (int $i = 0; $i < $resLength; $i ++) {
       |    $assignment
       |  }
       |  ${ev.value} = $values;
       |}
     """.stripMargin
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): Slice =
    copy(x = newFirst, start = newSecond, length = newThird)
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
  """,
  group = "array_funcs",
  since = "2.4.0")
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

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    if (nullReplacement.isDefined) {
      copy(
        array = newChildren(0),
        delimiter = newChildren(1),
        nullReplacement = Some(newChildren(2)))
    } else {
      copy(array = newChildren(0), delimiter = newChildren(1))
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

  override def prettyName: String = "array_join"
}

/**
 * Returns the minimum value in the array.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(array) - Returns the minimum value in the array. NaN is greater than
    any non-NaN elements for double/float type. NULL elements are skipped.""",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 20, null, 3));
       1
  """,
  group = "array_funcs",
  since = "2.4.0")
case class ArrayMin(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  @transient private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val typeCheckResult = super.checkInputDataTypes()
    if (typeCheckResult.isSuccess) {
      TypeUtils.checkForOrderingExpr(dataType, prettyName)
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

  @transient override lazy val dataType: DataType = child.dataType match {
    case ArrayType(dt, _) => dt
    case _ => throw new IllegalStateException(s"$prettyName accepts only arrays.")
  }

  override def prettyName: String = "array_min"

  override protected def withNewChildInternal(newChild: Expression): ArrayMin =
    copy(child = newChild)
}

/**
 * Returns the maximum value in the array.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(array) - Returns the maximum value in the array. NaN is greater than
    any non-NaN elements for double/float type. NULL elements are skipped.""",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 20, null, 3));
       20
  """,
  group = "array_funcs",
  since = "2.4.0")
case class ArrayMax(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  @transient private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val typeCheckResult = super.checkInputDataTypes()
    if (typeCheckResult.isSuccess) {
      TypeUtils.checkForOrderingExpr(dataType, prettyName)
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

  @transient override lazy val dataType: DataType = child.dataType match {
    case ArrayType(dt, _) => dt
    case _ => throw new IllegalStateException(s"$prettyName accepts only arrays.")
  }

  override def prettyName: String = "array_max"

  override protected def withNewChildInternal(newChild: Expression): ArrayMax =
    copy(child = newChild)
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
  group = "array_funcs",
  since = "2.4.0")
case class ArrayPosition(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant with QueryErrorsBase {

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(right.dataType)

  override def dataType: DataType = LongType

  override def inputTypes: Seq[AbstractDataType] = {
    (left.dataType, right.dataType) match {
      case (ArrayType(e1, hasNull), e2) =>
        TypeCoercion.findTightestCommonType(e1, e2) match {
          case Some(dt) => Seq(ArrayType(dt, hasNull), dt)
          case _ => Seq.empty
        }
      case _ => Seq.empty
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (NullType, _) | (_, NullType) =>
        DataTypeMismatch(
          errorSubClass = "NULL_TYPE",
          Map("functionName" -> toSQLId(prettyName)))
      case (t, _) if !ArrayType.acceptsType(t) =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "1",
            "requiredType" -> toSQLType(ArrayType),
            "inputSql" -> toSQLExpr(left),
            "inputType" -> toSQLType(left.dataType))
        )
      case (ArrayType(e1, _), e2) if e1.sameType(e2) =>
        TypeUtils.checkForOrderingExpr(e2, prettyName)
      case _ =>
        DataTypeMismatch(
          errorSubClass = "ARRAY_FUNCTION_DIFF_TYPES",
          messageParameters = Map(
            "functionName" -> toSQLId(prettyName),
            "dataType" -> toSQLType(ArrayType),
            "leftType" -> toSQLType(left.dataType),
            "rightType" -> toSQLType(right.dataType)
          )
        )
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

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): ArrayPosition =
    copy(left = newLeft, right = newRight)
}

/**
 * Returns the value of index `right` in Array `left`. If the index points outside of the array
 * boundaries, then this function returns NULL.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(array, index) - Returns element of array at given (0-based) index. If the index points
     outside of the array boundaries, then this function returns NULL.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), 0);
       1
      > SELECT _FUNC_(array(1, 2, 3), 3);
       NULL
      > SELECT _FUNC_(array(1, 2, 3), -1);
       NULL
  """,
  since = "3.4.0",
  group = "array_funcs")
case class Get(
    left: Expression,
    right: Expression,
    replacement: Expression) extends RuntimeReplaceable with InheritAnalysisRules {

  def this(left: Expression, right: Expression) =
    this(left, right, GetArrayItem(left, right, failOnError = false))

  override def prettyName: String = "get"

  override def parameters: Seq[Expression] = Seq(left, right)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    this.copy(replacement = newChild)
}

/**
 * Returns the value of index `right` in Array `left` or the value for key `right` in Map `left`.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(array, index) - Returns element of array at given (1-based) index. If Index is 0,
      Spark will throw an error. If index < 0, accesses elements from the last to the first.
      The function returns NULL if the index exceeds the length of the array and
      `spark.sql.ansi.enabled` is set to false.
      If `spark.sql.ansi.enabled` is set to true, it throws ArrayIndexOutOfBoundsException
      for invalid indices.

    _FUNC_(map, key) - Returns value for given key. The function returns NULL if the key is not
       contained in the map.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), 2);
       2
      > SELECT _FUNC_(map(1, 'a', 2, 'b'), 2);
       b
  """,
  since = "2.4.0",
  group = "map_funcs")
case class ElementAt(
    left: Expression,
    right: Expression,
    // The value to return if index is out of bound
    defaultValueOutOfBound: Option[Literal] = None,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends GetMapValueUtil with GetArrayItemUtil with NullIntolerant with SupportQueryContext
  with QueryErrorsBase {

  def this(left: Expression, right: Expression) = this(left, right, None, SQLConf.get.ansiEnabled)

  @transient private lazy val mapKeyType = left.dataType.asInstanceOf[MapType].keyType

  @transient private lazy val mapValueContainsNull =
    left.dataType.asInstanceOf[MapType].valueContainsNull

  @transient private lazy val arrayElementNullable =
    left.dataType.asInstanceOf[ArrayType].containsNull

  @transient private lazy val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(mapKeyType)

  @transient override lazy val dataType: DataType = left.dataType match {
    case ArrayType(elementType, _) => elementType
    case MapType(_, valueType, _) => valueType
  }

  override def inputTypes: Seq[AbstractDataType] = {
    (left.dataType, right.dataType) match {
      case (arr: ArrayType, e2: IntegralType) if (e2 != LongType) =>
        Seq(arr, IntegerType)
      case (MapType(keyType, valueType, hasNull), e2) =>
        TypeCoercion.findTightestCommonType(keyType, e2) match {
          case Some(dt) => Seq(MapType(dt, valueType, hasNull), dt)
          case _ => Seq.empty
        }
      case (l, r) => Seq.empty

    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (_: ArrayType, e2) if e2 != IntegerType =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "2",
            "requiredType" -> toSQLType(IntegerType),
            "inputSql" -> toSQLExpr(right),
            "inputType" -> toSQLType(right.dataType))
        )
      case (MapType(e1, _, _), e2) if (!e2.sameType(e1)) =>
        DataTypeMismatch(
          errorSubClass = "MAP_FUNCTION_DIFF_TYPES",
          messageParameters = Map(
            "functionName" -> toSQLId(prettyName),
            "dataType" -> toSQLType(MapType),
            "leftType" -> toSQLType(left.dataType),
            "rightType" -> toSQLType(right.dataType)
          )
        )
      case (e1, _) if (!e1.isInstanceOf[MapType] && !e1.isInstanceOf[ArrayType]) =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "1",
            "requiredType" -> toSQLType(TypeCollection(ArrayType, MapType)),
            "inputSql" -> toSQLExpr(left),
            "inputType" -> toSQLType(left.dataType))
        )
      case _ => TypeCheckResult.TypeCheckSuccess
    }
  }

  private def nullability(elements: Seq[Expression], ordinal: Int): Boolean = {
    if (ordinal == 0) {
      false
    } else if (elements.length < math.abs(ordinal)) {
      !failOnError
    } else {
      if (ordinal < 0) {
        elements(elements.length + ordinal).nullable
      } else {
        elements(ordinal - 1).nullable
      }
    }
  }

  override def nullable: Boolean = left.dataType match {
    case _: ArrayType =>
      computeNullabilityFromArray(left, right, failOnError, nullability)
    case _: MapType => true
  }

  override def nullSafeEval(value: Any, ordinal: Any): Any = doElementAt(value, ordinal)

  @transient private lazy val doElementAt: (Any, Any) => Any = left.dataType match {
    case _: ArrayType =>
      (value, ordinal) => {
        val array = value.asInstanceOf[ArrayData]
        val index = ordinal.asInstanceOf[Int]
        if (array.numElements() < math.abs(index)) {
          if (failOnError) {
            throw QueryExecutionErrors.invalidElementAtIndexError(
              index, array.numElements(), getContextOrNull())
          } else {
            defaultValueOutOfBound match {
              case Some(value) => value.eval()
              case None => null
            }
          }
        } else {
          val idx = if (index == 0) {
            throw QueryExecutionErrors.elementAtByIndexZeroError(getContextOrNull())
          } else if (index > 0) {
            index - 1
          } else {
            array.numElements() + index
          }
          if (arrayElementNullable && array.isNullAt(idx)) {
            null
          } else {
            array.get(idx, dataType)
          }
        }
      }
    case _: MapType =>
      (value, ordinal) => getValueEval(value, ordinal, mapKeyType, ordering)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    left.dataType match {
      case _: ArrayType =>
        nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
          val index = ctx.freshName("elementAtIndex")
          val nullCheck = if (arrayElementNullable) {
            s"""
               |if ($eval1.isNullAt($index)) {
               |  ${ev.isNull} = true;
               |} else
             """.stripMargin
          } else {
            ""
          }
          val errorContext = getContextOrNullCode(ctx)
          val indexOutOfBoundBranch = if (failOnError) {
            // scalastyle:off line.size.limit
            s"throw QueryExecutionErrors.invalidElementAtIndexError($index, $eval1.numElements(), $errorContext);"
            // scalastyle:on line.size.limit
          } else {
            defaultValueOutOfBound match {
              case Some(value) =>
                val defaultValueEval = value.genCode(ctx)
                s"""
                  ${defaultValueEval.code}
                  ${ev.isNull} = ${defaultValueEval.isNull};
                  ${ev.value} = ${defaultValueEval.value};
                """.stripMargin
              case None => s"${ev.isNull} = true;"
            }
          }

          s"""
             |int $index = (int) $eval2;
             |if ($eval1.numElements() < Math.abs($index)) {
             |  $indexOutOfBoundBranch
             |} else {
             |  if ($index == 0) {
             |    throw QueryExecutionErrors.elementAtByIndexZeroError($errorContext);
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

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): ElementAt = copy(left = newLeft, right = newRight)

  override def initQueryContext(): Option[SQLQueryContext] = {
    if (failOnError && left.resolved && left.dataType.isInstanceOf[ArrayType]) {
      Some(origin.context)
    } else {
      None
    }
  }
}

/**
 * Returns the value of index `right` in Array `left` or the value for key `right` in Map `left`.
 * The function is identical to the function `element_at`, except that it returns `NULL` result
 * instead of throwing an exception on array's index out of bound or map's key not found when
 * `spark.sql.ansi.enabled` is true.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(array, index) - Returns element of array at given (1-based) index. If Index is 0,
      Spark will throw an error. If index < 0, accesses elements from the last to the first.
      The function always returns NULL if the index exceeds the length of the array.

    _FUNC_(map, key) - Returns value for given key. The function always returns NULL
      if the key is not contained in the map.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), 2);
       2
      > SELECT _FUNC_(map(1, 'a', 2, 'b'), 2);
       b
  """,
  since = "3.3.0",
  group = "map_funcs")
case class TryElementAt(left: Expression, right: Expression, replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {
  def this(left: Expression, right: Expression) =
    this(left, right, ElementAt(left, right, None, failOnError = false))

  override def prettyName: String = "try_element_at"

  override def parameters: Seq[Expression] = Seq(left, right)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    this.copy(replacement = newChild)
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
       [1,2,3,4,5,6]
  """,
  note = """
    Concat logic for arrays is available since 2.4.0.
  """,
  group = "collection_funcs",
  since = "1.5.0")
case class Concat(children: Seq[Expression]) extends ComplexTypeMergingExpression
  with QueryErrorsBase {

  private def allowedTypes: Seq[AbstractDataType] = Seq(StringType, BinaryType, ArrayType)

  final override val nodePatterns: Seq[TreePattern] = Seq(CONCAT)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.isEmpty) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      val dataTypeMismatch = children.zipWithIndex.collectFirst {
        case (e, idx) if !allowedTypes.exists(_.acceptsType(e.dataType)) =>
          DataTypeMismatch(
            errorSubClass = "UNEXPECTED_INPUT_TYPE",
            messageParameters = Map(
              "paramIndex" -> (idx + 1).toString,
              "requiredType" -> toSQLType(TypeCollection(allowedTypes: _*)),
              "inputSql" -> toSQLExpr(e),
              "inputType" -> toSQLType(e.dataType))
          )
      }
      dataTypeMismatch match {
        case Some(mismatch) => mismatch
        case _ => TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), prettyName)
      }
    }
  }

  @transient override lazy val dataType: DataType = {
    if (children.isEmpty) {
      StringType
    } else {
      super.dataType
    }
  }

  private def resultArrayElementNullable = dataType.asInstanceOf[ArrayType].containsNull

  private def javaType: String = CodeGenerator.javaType(dataType)

  override def nullable: Boolean = children.exists(_.nullable)

  override def foldable: Boolean = children.forall(_.foldable)

  override def eval(input: InternalRow): Any = doConcat(input)

  @transient private lazy val doConcat: InternalRow => Any = dataType match {
    case BinaryType =>
      input => {
        val inputs = children.map(_.eval(input).asInstanceOf[Array[Byte]])
        ByteArray.concat(inputs: _*)
      }
    case StringType =>
      input => {
        val inputs = children.map(_.eval(input).asInstanceOf[UTF8String])
        UTF8String.concat(inputs: _*)
      }
    case ArrayType(elementType, _) =>
      input => {
        val inputs = children.toStream.map(_.eval(input))
        if (inputs.contains(null)) {
          null
        } else {
          val arrayData = inputs.map(_.asInstanceOf[ArrayData])
          val numberOfElements = arrayData.foldLeft(0L)((sum, ad) => sum + ad.numElements())
          if (numberOfElements > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
            throw QueryExecutionErrors.concatArraysWithElementsExceedLimitError(numberOfElements)
          }
          val finalData = new Array[AnyRef](numberOfElements.toInt)
          var position = 0
          for (ad <- arrayData) {
            val arr = ad.toObjectArray(elementType)
            Array.copy(arr, 0, finalData, position, arr.length)
            position += arr.length
          }
          new GenericArrayData(finalData)
        }
      }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evals = children.map(_.genCode(ctx))
    val args = ctx.freshName("args")
    val hasNull = ctx.freshName("hasNull")

    val inputs = evals.zip(children.map(_.nullable)).zipWithIndex.map {
      case ((eval, true), index) =>
        s"""
           |if (!$hasNull) {
           |  ${eval.code}
           |  if (!${eval.isNull}) {
           |    $args[$index] = ${eval.value};
           |  } else {
           |    $hasNull = true;
           |  }
           |}
         """.stripMargin
      case ((eval, false), index) =>
        s"""
           |if (!$hasNull) {
           |  ${eval.code}
           |  $args[$index] = ${eval.value};
           |}
         """.stripMargin
    }

    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = inputs,
      funcName = "valueConcat",
      extraArguments = (s"$javaType[]", args) :: ("boolean", hasNull) :: Nil,
      returnType = "boolean",
      makeSplitFunction = body =>
        s"""
           |$body
           |return $hasNull;
         """.stripMargin,
      foldFunctions = _.map(funcCall => s"$hasNull = $funcCall;").mkString("\n")
    )

    val (concat, initCode) = dataType match {
      case BinaryType =>
        (s"${classOf[ByteArray].getName}.concat", s"byte[][] $args = new byte[${evals.length}][];")
      case StringType =>
        ("UTF8String.concat", s"UTF8String[] $args = new UTF8String[${evals.length}];")
      case ArrayType(elementType, containsNull) =>
        val concat = genCodeForArrays(ctx, elementType, containsNull)
        (concat, s"ArrayData[] $args = new ArrayData[${evals.length}];")
    }

    ev.copy(code =
      code"""
         |boolean $hasNull = false;
         |$initCode
         |$codes
         |$javaType ${ev.value} = null;
         |if (!$hasNull) {
         |  ${ev.value} = $concat($args);
         |}
         |boolean ${ev.isNull} = ${ev.value} == null;
       """.stripMargin)
  }

  private def genCodeForNumberOfElements(ctx: CodegenContext) : (String, String) = {
    val numElements = ctx.freshName("numElements")
    val z = ctx.freshName("z")
    val code = s"""
        |long $numElements = 0L;
        |for (int $z = 0; $z < ${children.length}; $z++) {
        |  $numElements += args[$z].numElements();
        |}
      """.stripMargin

    (code, numElements)
  }

  private def genCodeForArrays(
      ctx: CodegenContext,
      elementType: DataType,
      checkForNull: Boolean): String = {
    val counter = ctx.freshName("counter")
    val arrayData = ctx.freshName("arrayData")
    val y = ctx.freshName("y")
    val z = ctx.freshName("z")

    val (numElemCode, numElemName) = genCodeForNumberOfElements(ctx)

    val initialization = CodeGenerator.createArrayData(
      arrayData, elementType, numElemName, s" $prettyName failed.")
    val assignment = CodeGenerator.createArrayAssignment(
      arrayData, elementType, s"args[$y]", counter, z, resultArrayElementNullable)

    val concat = ctx.freshName("concat")
    val concatDef =
      s"""
         |private ArrayData $concat(ArrayData[] args) {
         |  $numElemCode
         |  $initialization
         |  int $counter = 0;
         |  for (int $y = 0; $y < ${children.length}; $y++) {
         |    for (int $z = 0; $z < args[$y].numElements(); $z++) {
         |      $assignment
         |      $counter++;
         |    }
         |  }
         |  return $arrayData;
         |}
       """.stripMargin

    ctx.addNewFunction(concat, concatDef)
  }

  override def toString: String = s"concat(${children.mkString(", ")})"

  override def sql: String = s"concat(${children.map(_.sql).mkString(", ")})"

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Concat =
    copy(children = newChildren)
}

/**
 * Transforms an array of arrays into a single array.
 */
@ExpressionDescription(
  usage = "_FUNC_(arrayOfArrays) - Transforms an array of arrays into a single array.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(array(1, 2), array(3, 4)));
       [1,2,3,4]
  """,
  group = "array_funcs",
  since = "2.4.0")
case class Flatten(child: Expression) extends UnaryExpression with NullIntolerant
  with QueryErrorsBase {

  private def childDataType: ArrayType = child.dataType.asInstanceOf[ArrayType]

  override def nullable: Boolean = child.nullable || childDataType.containsNull

  @transient override lazy val dataType: DataType = childDataType.elementType

  private def resultArrayElementNullable = dataType.asInstanceOf[ArrayType].containsNull

  @transient private lazy val elementType: DataType = dataType.asInstanceOf[ArrayType].elementType

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case ArrayType(_: ArrayType, _) =>
      TypeCheckResult.TypeCheckSuccess
    case _ =>
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> "1",
          "requiredType" -> s"${toSQLType(ArrayType)} of ${toSQLType(ArrayType)}",
          "inputSql" -> toSQLExpr(child),
          "inputType" -> toSQLType(child.dataType))
      )
  }

  override def nullSafeEval(child: Any): Any = {
    val elements = child.asInstanceOf[ArrayData].toObjectArray(dataType)

    if (elements.contains(null)) {
      null
    } else {
      val arrayData = elements.map(_.asInstanceOf[ArrayData])
      val numberOfElements = arrayData.foldLeft(0L)((sum, e) => sum + e.numElements())
      if (numberOfElements > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
        throw QueryExecutionErrors.flattenArraysWithElementsExceedLimitError(numberOfElements)
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
      val code = genCodeForFlatten(ctx, c, ev.value)
      ctx.nullArrayElementsSaveExec(childDataType.containsNull, ev.isNull, c)(code)
    })
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
      """.stripMargin
    (code, variableName)
  }

  private def genCodeForFlatten(
      ctx: CodegenContext,
      childVariableName: String,
      arrayDataName: String): String = {
    val counter = ctx.freshName("counter")
    val tempArrayDataName = ctx.freshName("tempArrayData")
    val k = ctx.freshName("k")
    val l = ctx.freshName("l")
    val arr = ctx.freshName("arr")

    val (numElemCode, numElemName) = genCodeForNumberOfElements(ctx, childVariableName)

    val allocation = CodeGenerator.createArrayData(
      tempArrayDataName, elementType, numElemName, s" $prettyName failed.")
    val assignment = CodeGenerator.createArrayAssignment(
      tempArrayDataName, elementType, arr, counter, l, resultArrayElementNullable)

    s"""
    |$numElemCode
    |$allocation
    |int $counter = 0;
    |for (int $k = 0; $k < $childVariableName.numElements(); $k++) {
    |  ArrayData $arr = $childVariableName.getArray($k);
    |  for (int $l = 0; $l < $arr.numElements(); $l++) {
    |   $assignment
    |   $counter++;
    | }
    |}
    |$arrayDataName = $tempArrayDataName;
    """.stripMargin
  }

  override def prettyName: String = "flatten"

  override protected def withNewChildInternal(newChild: Expression): Flatten =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = """
    _FUNC_(start, stop, step) - Generates an array of elements from start to stop (inclusive),
      incrementing by step. The type of the returned elements is the same as the type of argument
      expressions.

      Supported types are: byte, short, integer, long, date, timestamp.

      The start and stop expressions must resolve to the same type.
      If start and stop expressions resolve to the 'date' or 'timestamp' type
      then the step expression must resolve to the 'interval' or 'year-month interval' or
      'day-time interval' type, otherwise to the same type as the start and stop expressions.
  """,
  arguments = """
    Arguments:
      * start - an expression. The start of the range.
      * stop - an expression. The end the range (inclusive).
      * step - an optional expression. The step of the range.
          By default step is 1 if start is less than or equal to stop, otherwise -1.
          For the temporal sequences it's 1 day and -1 day respectively.
          If start is greater than stop then the step must be negative, and vice versa.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(1, 5);
       [1,2,3,4,5]
      > SELECT _FUNC_(5, 1);
       [5,4,3,2,1]
      > SELECT _FUNC_(to_date('2018-01-01'), to_date('2018-03-01'), interval 1 month);
       [2018-01-01,2018-02-01,2018-03-01]
      > SELECT _FUNC_(to_date('2018-01-01'), to_date('2018-03-01'), interval '0-1' year to month);
       [2018-01-01,2018-02-01,2018-03-01]
  """,
  group = "array_funcs",
  since = "2.4.0"
)
case class Sequence(
    start: Expression,
    stop: Expression,
    stepOpt: Option[Expression],
    timeZoneId: Option[String] = None)
  extends Expression
  with TimeZoneAwareExpression
  with QueryErrorsBase {

  import Sequence._

  def this(start: Expression, stop: Expression) =
    this(start, stop, None, None)

  def this(start: Expression, stop: Expression, step: Expression) =
    this(start, stop, Some(step), None)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Some(timeZoneId))

  override def children: Seq[Expression] = Seq(start, stop) ++ stepOpt

  override def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): TimeZoneAwareExpression = {
    if (stepOpt.isDefined) {
      copy(start = newChildren(0), stop = newChildren(1), stepOpt = Some(newChildren(2)))
    } else {
      copy(start = newChildren(0), stop = newChildren(1))
    }
  }

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def dataType: ArrayType = ArrayType(start.dataType, containsNull = false)

  override def checkInputDataTypes(): TypeCheckResult = {
    val startType = start.dataType
    def stepType = stepOpt.get.dataType
    val typesCorrect =
      startType.sameType(stop.dataType) &&
        (startType match {
          case TimestampType | TimestampNTZType =>
            stepOpt.isEmpty || CalendarIntervalType.acceptsType(stepType) ||
              YearMonthIntervalType.acceptsType(stepType) ||
              DayTimeIntervalType.acceptsType(stepType)
          case DateType =>
            stepOpt.isEmpty || CalendarIntervalType.acceptsType(stepType) ||
              YearMonthIntervalType.acceptsType(stepType) ||
              DayTimeIntervalType.acceptsType(stepType)
          case _: IntegralType =>
            stepOpt.isEmpty || stepType.sameType(startType)
          case _ => false
        })

    if (typesCorrect) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      DataTypeMismatch(
        errorSubClass = "SEQUENCE_WRONG_INPUT_TYPES",
        messageParameters = Map(
          "functionName" -> toSQLId(prettyName),
          "startType" -> toSQLType(TypeCollection(TimestampType, TimestampNTZType, DateType)),
          "stepType" -> toSQLType(
            TypeCollection(CalendarIntervalType, YearMonthIntervalType, DayTimeIntervalType)),
          "otherStartType" -> toSQLType(IntegralType)
        )
      )
    }
  }

  private def isNotIntervalType(expr: Expression) = expr.dataType match {
    case CalendarIntervalType | _: AnsiIntervalType => false
    case _ => true
  }

  def coercibleChildren: Seq[Expression] = children.filter(isNotIntervalType)

  def castChildrenTo(widerType: DataType): Expression = Sequence(
    Cast(start, widerType),
    Cast(stop, widerType),
    stepOpt.map(step => if (isNotIntervalType(step)) Cast(step, widerType) else step),
    timeZoneId)

  @transient private lazy val impl: InternalSequence = dataType.elementType match {
    case iType: IntegralType =>
      type T = iType.InternalType
      val ct = ClassTag[T](iType.tag.mirror.runtimeClass(iType.tag.tpe))
      new IntegralSequenceImpl(iType)(ct, iType.integral)

    case TimestampType | TimestampNTZType =>
      if (stepOpt.isEmpty || CalendarIntervalType.acceptsType(stepOpt.get.dataType)) {
        new TemporalSequenceImpl[Long](LongType, start.dataType, 1, identity, zoneId)
      } else if (YearMonthIntervalType.acceptsType(stepOpt.get.dataType)) {
        new PeriodSequenceImpl[Long](LongType, start.dataType, 1, identity, zoneId)
      } else {
        new DurationSequenceImpl[Long](LongType, start.dataType, 1, identity, zoneId)
      }

    case DateType =>
      if (stepOpt.isEmpty || CalendarIntervalType.acceptsType(stepOpt.get.dataType)) {
        new TemporalSequenceImpl[Int](IntegerType, start.dataType, MICROS_PER_DAY, _.toInt, zoneId)
      } else if (YearMonthIntervalType.acceptsType(stepOpt.get.dataType)) {
        new PeriodSequenceImpl[Int](IntegerType, start.dataType, MICROS_PER_DAY, _.toInt, zoneId)
      } else {
        new DurationSequenceImpl[Int](IntegerType, start.dataType, MICROS_PER_DAY, _.toInt, zoneId)
      }
  }

  override def eval(input: InternalRow): Any = {
    val startVal = start.eval(input)
    if (startVal == null) return null
    val stopVal = stop.eval(input)
    if (stopVal == null) return null
    val stepVal = stepOpt.map(_.eval(input)).getOrElse(impl.defaultStep(startVal, stopVal))
    if (stepVal == null) return null

    ArrayData.toArrayData(impl.eval(startVal, stopVal, stepVal))
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val startGen = start.genCode(ctx)
    val stopGen = stop.genCode(ctx)
    val stepGen = stepOpt.map(_.genCode(ctx)).getOrElse(
      impl.defaultStep.genCode(ctx, startGen, stopGen))

    val resultType = CodeGenerator.javaType(dataType)
    val resultCode = {
      val arr = ctx.freshName("arr")
      val arrElemType = CodeGenerator.javaType(dataType.elementType)
      s"""
         |final $arrElemType[] $arr = null;
         |${impl.genCode(ctx, startGen.value, stopGen.value, stepGen.value, arr, arrElemType)}
         |${ev.value} = UnsafeArrayData.fromPrimitiveArray($arr);
       """.stripMargin
    }

    if (nullable) {
      val nullSafeEval =
        startGen.code + ctx.nullSafeExec(start.nullable, startGen.isNull) {
          stopGen.code + ctx.nullSafeExec(stop.nullable, stopGen.isNull) {
            stepGen.code + ctx.nullSafeExec(stepOpt.exists(_.nullable), stepGen.isNull) {
              s"""
                 |${ev.isNull} = false;
                 |$resultCode
               """.stripMargin
            }
          }
        }
      ev.copy(code =
        code"""
           |boolean ${ev.isNull} = true;
           |$resultType ${ev.value} = null;
           |$nullSafeEval
         """.stripMargin)

    } else {
      ev.copy(code =
        code"""
           |${startGen.code}
           |${stopGen.code}
           |${stepGen.code}
           |$resultType ${ev.value} = null;
           |$resultCode
         """.stripMargin,
        isNull = FalseLiteral)
    }
  }
}

object Sequence {

  private type LessThanOrEqualFn = (Any, Any) => Boolean

  private class DefaultStep(lteq: LessThanOrEqualFn, stepType: DataType, one: Any) {
    private val negativeOne = UnaryMinus(Literal(one)).eval()

    def apply(start: Any, stop: Any): Any = {
      if (lteq(start, stop)) one else negativeOne
    }

    def genCode(ctx: CodegenContext, startGen: ExprCode, stopGen: ExprCode): ExprCode = {
      val Seq(oneVal, negativeOneVal) = Seq(one, negativeOne).map(Literal(_).genCode(ctx).value)
      ExprCode.forNonNullValue(JavaCode.expression(
        s"${startGen.value} <= ${stopGen.value} ? $oneVal : $negativeOneVal",
        stepType))
    }
  }

  private trait InternalSequence {
    def eval(start: Any, stop: Any, step: Any): Any

    def genCode(
        ctx: CodegenContext,
        start: String,
        stop: String,
        step: String,
        arr: String,
        elemType: String): String

    val defaultStep: DefaultStep
  }

  private class IntegralSequenceImpl[T: ClassTag]
    (elemType: IntegralType)(implicit num: Integral[T]) extends InternalSequence {

    override val defaultStep: DefaultStep = new DefaultStep(
      (elemType.ordering.lteq _).asInstanceOf[LessThanOrEqualFn],
      elemType,
      num.one)

    override def eval(input1: Any, input2: Any, input3: Any): Array[T] = {
      import num._

      val start = input1.asInstanceOf[T]
      val stop = input2.asInstanceOf[T]
      val step = input3.asInstanceOf[T]

      var i: Int = getSequenceLength(start, stop, step, step)
      val arr = new Array[T](i)
      while (i > 0) {
        i -= 1
        arr(i) = start + step * num.fromInt(i)
      }
      arr
    }

    override def genCode(
        ctx: CodegenContext,
        start: String,
        stop: String,
        step: String,
        arr: String,
        elemType: String): String = {
      val i = ctx.freshName("i")
      s"""
         |${genSequenceLengthCode(ctx, start, stop, step, step, i)}
         |$arr = new $elemType[$i];
         |while ($i > 0) {
         |  $i--;
         |  $arr[$i] = ($elemType) ($start + $step * $i);
         |}
         """.stripMargin
    }
  }

  private class PeriodSequenceImpl[T: ClassTag]
      (dt: IntegralType, outerDataType: DataType, scale: Long, fromLong: Long => T, zoneId: ZoneId)
      (implicit num: Integral[T])
    extends InternalSequenceBase(dt, outerDataType, scale, fromLong, zoneId) {

    override val defaultStep: DefaultStep = new DefaultStep(
      (dt.ordering.lteq _).asInstanceOf[LessThanOrEqualFn],
      YearMonthIntervalType(),
      Period.of(0, 1, 0))

    val intervalType: DataType = YearMonthIntervalType()

    def splitStep(input: Any): (Int, Int, Long) = {
      (input.asInstanceOf[Int], 0, 0)
    }

    def stepSplitCode(
        stepMonths: String, stepDays: String, stepMicros: String, step: String): String = {
      s"""
         |final int $stepMonths = $step;
         |final int $stepDays = 0;
         |final long $stepMicros = 0L;
       """.stripMargin
    }
  }

  private class DurationSequenceImpl[T: ClassTag]
      (dt: IntegralType, outerDataType: DataType, scale: Long, fromLong: Long => T, zoneId: ZoneId)
      (implicit num: Integral[T])
    extends InternalSequenceBase(dt, outerDataType, scale, fromLong, zoneId) {

    override val defaultStep: DefaultStep = new DefaultStep(
      (dt.ordering.lteq _).asInstanceOf[LessThanOrEqualFn],
      DayTimeIntervalType(),
      Duration.ofDays(1))

    val intervalType: DataType = DayTimeIntervalType()

    def splitStep(input: Any): (Int, Int, Long) = {
      val duration = input.asInstanceOf[Long]
      val days = IntervalUtils.getDays(duration)
      val micros = duration - days * MICROS_PER_DAY
      (0, days, micros)
    }

    def stepSplitCode(
        stepMonths: String, stepDays: String, stepMicros: String, step: String): String = {
      s"""
         |final int $stepMonths = 0;
         |final int $stepDays =
         |  (int) org.apache.spark.sql.catalyst.util.IntervalUtils.getDays($step);
         |final long $stepMicros = $step - $stepDays * ${MICROS_PER_DAY}L;
       """.stripMargin
    }
  }

  private class TemporalSequenceImpl[T: ClassTag]
      (dt: IntegralType, outerDataType: DataType, scale: Long, fromLong: Long => T, zoneId: ZoneId)
      (implicit num: Integral[T])
    extends InternalSequenceBase(dt, outerDataType, scale, fromLong, zoneId) {

    override val defaultStep: DefaultStep = new DefaultStep(
      (dt.ordering.lteq _).asInstanceOf[LessThanOrEqualFn],
      CalendarIntervalType,
      new CalendarInterval(0, 1, 0))

    val intervalType: DataType = CalendarIntervalType

    def splitStep(input: Any): (Int, Int, Long) = {
      val step = input.asInstanceOf[CalendarInterval]
      (step.months, step.days, step.microseconds)
    }

    def stepSplitCode(
        stepMonths: String, stepDays: String, stepMicros: String, step: String): String = {
      s"""
         |final int $stepMonths = $step.months;
         |final int $stepDays = $step.days;
         |final long $stepMicros = $step.microseconds;
       """.stripMargin
    }
  }

  private abstract class InternalSequenceBase[T: ClassTag]
      (dt: IntegralType, outerDataType: DataType, scale: Long, fromLong: Long => T, zoneId: ZoneId)
      (implicit num: Integral[T]) extends InternalSequence {

    val defaultStep: DefaultStep

    private val backedSequenceImpl = new IntegralSequenceImpl[T](dt)
    // We choose a minimum days(28) in one month to calculate the `intervalStepInMicros`
    // in order to make sure the estimated array length is long enough
    private val microsPerMonth = 28 * MICROS_PER_DAY

    protected val intervalType: DataType

    protected def splitStep(input: Any): (Int, Int, Long)

    private val addInterval: (Long, Int, Int, Long, ZoneId) => Long = outerDataType match {
      case TimestampType | DateType => timestampAddInterval
      case TimestampNTZType => timestampNTZAddInterval
    }

    private def toMicros(value: Long, scale: Long): Long = {
      if (scale == MICROS_PER_DAY) {
        daysToMicros(value.toInt, zoneId)
      } else {
        value * scale
      }
    }

    private def fromMicros(value: Long, scale: Long): Long = {
      if (scale == MICROS_PER_DAY) {
        microsToDays(value, zoneId).toLong
      } else {
        value / scale
      }
    }

    override def eval(input1: Any, input2: Any, input3: Any): Array[T] = {
      val start = input1.asInstanceOf[T]
      val stop = input2.asInstanceOf[T]
      val (stepMonths, stepDays, stepMicros) = splitStep(input3)

      if (scale == MICROS_PER_DAY && stepMonths == 0 && stepDays == 0) {
        throw new IllegalArgumentException(s"sequence step must be an ${intervalType.typeName}" +
          " of day granularity if start and end values are dates")
      }

      if (stepMonths == 0 && stepMicros == 0 && scale == MICROS_PER_DAY) {
        // Adding pure days to date start/end
        backedSequenceImpl.eval(start, stop, fromLong(stepDays))

      } else if (stepMonths == 0 && stepDays == 0 && scale == 1) {
        // Adding pure microseconds to timestamp start/end
        backedSequenceImpl.eval(start, stop, fromLong(stepMicros))

      } else {
        // To estimate the resulted array length we need to make assumptions
        // about a month length in days and a day length in microseconds
        val intervalStepInMicros =
          stepMicros + stepMonths * microsPerMonth + stepDays * MICROS_PER_DAY

        val startMicros: Long = toMicros(num.toLong(start), scale)
        val stopMicros: Long = toMicros(num.toLong(stop), scale)

        val estimatedArrayLength =
          getSequenceLength(startMicros, stopMicros, input3, intervalStepInMicros)

        val stepSign = if (intervalStepInMicros > 0) +1 else -1
        val exclusiveItem = stopMicros + stepSign
        var arr = new Array[T](estimatedArrayLength)
        var t = startMicros
        var i = 0

        while (t < exclusiveItem ^ stepSign < 0) {
          val result = fromMicros(t, scale)
          // if we've underestimated the size of the array, due to crossing a DST
          // "spring forward" without a corresponding "fall back", make a copy
          // that's larger by 1
          if (i == arr.length) {
            arr = arr.padTo(i + 1, fromLong(0L))
          }
          arr(i) = fromLong(result)
          i += 1
          t = addInterval(startMicros, i * stepMonths, i * stepDays, i * stepMicros, zoneId)
        }

        // truncate array to the correct length
        if (arr.length == i) arr else arr.slice(0, i)
      }
    }

    protected def stepSplitCode(
         stepMonths: String, stepDays: String, stepMicros: String, step: String): String

    private val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")

    private val addIntervalCode = outerDataType match {
      case TimestampType | DateType => s"$dtu.timestampAddInterval"
      case TimestampNTZType => s"$dtu.timestampNTZAddInterval"
    }

    private val daysToMicrosCode = s"$dtu.daysToMicros"
    private val microsToDaysCode = s"$dtu.microsToDays"

    override def genCode(
        ctx: CodegenContext,
        start: String,
        stop: String,
        step: String,
        arr: String,
        elemType: String): String = {
      val stepMonths = ctx.freshName("stepMonths")
      val stepDays = ctx.freshName("stepDays")
      val stepMicros = ctx.freshName("stepMicros")
      val stepScaled = ctx.freshName("stepScaled")
      val intervalInMicros = ctx.freshName("intervalInMicros")
      val startMicros = ctx.freshName("startMicros")
      val stopMicros = ctx.freshName("stopMicros")
      val arrLength = ctx.freshName("arrLength")
      val stepSign = ctx.freshName("stepSign")
      val exclusiveItem = ctx.freshName("exclusiveItem")
      val t = ctx.freshName("t")
      val i = ctx.freshName("i")
      val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)

      val sequenceLengthCode =
        s"""
           |final long $intervalInMicros =
           |  $stepMicros + $stepMonths * ${microsPerMonth}L + $stepDays * ${MICROS_PER_DAY}L;
           |${genSequenceLengthCode(
              ctx, startMicros, stopMicros, step, intervalInMicros, arrLength)}
         """.stripMargin

      val check = if (scale == MICROS_PER_DAY) {
        s"""
           |if ($stepMonths == 0 && $stepDays == 0) {
           |  throw new IllegalArgumentException(
           |    "sequence step must be an ${intervalType.typeName} " +
           |    "of day granularity if start and end values are dates");
           |}
         """.stripMargin
        } else {
          ""
        }

      val stepSplits = stepSplitCode(stepMonths, stepDays, stepMicros, step)

      val toMicrosCode = if (scale == MICROS_PER_DAY) {
        s"""
          |  final long $startMicros = $daysToMicrosCode((int) $start, $zid);
          |  final long $stopMicros = $daysToMicrosCode((int) $stop, $zid);
          |""".stripMargin
      } else {
        s"""
          |  final long $startMicros = $start * ${scale}L;
          |  final long $stopMicros = $stop * ${scale}L;
          |""".stripMargin
      }

      val fromMicrosCode = if (scale == MICROS_PER_DAY) {
        s"($elemType) $microsToDaysCode($t, $zid)"
      } else {
        s"($elemType) ($t / ${scale}L)"
      }

      s"""
         |$stepSplits
         |
         |$check
         |
         |if ($stepMonths == 0 && $stepMicros == 0 && ${scale}L == ${MICROS_PER_DAY}L) {
         |  ${backedSequenceImpl.genCode(ctx, start, stop, stepDays, arr, elemType)};
         |
         |} else if ($stepMonths == 0 && $stepDays == 0 && ${scale}L == 1) {
         |  ${backedSequenceImpl.genCode(ctx, start, stop, stepMicros, arr, elemType)};
         |} else {
         |  $toMicrosCode
         |
         |  $sequenceLengthCode
         |
         |  final int $stepSign = $intervalInMicros > 0 ? +1 : -1;
         |  final long $exclusiveItem = $stopMicros + $stepSign;
         |
         |  $arr = new $elemType[$arrLength];
         |  long $t = $startMicros;
         |  int $i = 0;
         |
         |  while ($t < $exclusiveItem ^ $stepSign < 0) {
         |    if ($i == $arr.length) {
         |      $arr = java.util.Arrays.copyOf($arr, $i + 1);
         |    }
         |    $arr[$i] = $fromMicrosCode;
         |    $i += 1;
         |    $t = $addIntervalCode(
         |       $startMicros, $i * $stepMonths, $i * $stepDays, $i * $stepMicros, $zid);
         |  }
         |
         |  if ($arr.length > $i) {
         |    $arr = java.util.Arrays.copyOf($arr, $i);
         |  }
         |}
         """.stripMargin
    }
  }

  private def getSequenceLength[U](start: U, stop: U, step: Any, estimatedStep: U)
      (implicit num: Integral[U]): Int = {
    import num._
    require(
      (estimatedStep > num.zero && start <= stop)
        || (estimatedStep < num.zero && start >= stop)
        || (estimatedStep == num.zero && start == stop),
      s"Illegal sequence boundaries: $start to $stop by $step")

    val len = if (start == stop) 1L else 1L + (stop.toLong - start.toLong) / estimatedStep.toLong

    require(
      len <= MAX_ROUNDED_ARRAY_LENGTH,
      s"Too long sequence: $len. Should be <= $MAX_ROUNDED_ARRAY_LENGTH")

    len.toInt
  }

  private def genSequenceLengthCode(
      ctx: CodegenContext,
      start: String,
      stop: String,
      step: String,
      estimatedStep: String,
      len: String): String = {
    val longLen = ctx.freshName("longLen")
    s"""
       |if (!(($estimatedStep > 0 && $start <= $stop) ||
       |  ($estimatedStep < 0 && $start >= $stop) ||
       |  ($estimatedStep == 0 && $start == $stop))) {
       |  throw new IllegalArgumentException(
       |    "Illegal sequence boundaries: " + $start + " to " + $stop + " by " + $step);
       |}
       |long $longLen = $stop == $start ? 1L : 1L + ((long) $stop - $start) / $estimatedStep;
       |if ($longLen > $MAX_ROUNDED_ARRAY_LENGTH) {
       |  throw new IllegalArgumentException(
       |    "Too long sequence: " + $longLen + ". Should be <= $MAX_ROUNDED_ARRAY_LENGTH");
       |}
       |int $len = (int) $longLen;
       """.stripMargin
  }
}

/**
 * Returns the array containing the given input value (left) count (right) times.
 */
@ExpressionDescription(
  usage = "_FUNC_(element, count) - Returns the array containing element count times.",
  examples = """
    Examples:
      > SELECT _FUNC_('123', 2);
       ["123","123"]
  """,
  group = "array_funcs",
  since = "2.4.0")
case class ArrayRepeat(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def dataType: ArrayType = ArrayType(left.dataType, left.nullable)

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegerType)

  override def nullable: Boolean = right.nullable

  override def eval(input: InternalRow): Any = {
    val count = right.eval(input)
    if (count == null) {
      null
    } else {
      if (count.asInstanceOf[Int] > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
        throw QueryExecutionErrors.createArrayWithElementsExceedLimitError(count)
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

    val coreLogic = genCodeForElement(ctx, et, element, count, leftGen.isNull, ev.value)
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
       """.stripMargin

    (numElements, numElementsCode)
  }

  private def genCodeForElement(
      ctx: CodegenContext,
      elementType: DataType,
      element: String,
      count: String,
      leftIsNull: String,
      arrayDataName: String): String = {
    val tempArrayDataName = ctx.freshName("tempArrayData")
    val k = ctx.freshName("k")
    val (numElemName, numElemCode) = genCodeForNumberOfElements(ctx, count)

    val allocation = CodeGenerator.createArrayData(
      tempArrayDataName, elementType, numElemName, s" $prettyName failed.")
    val assignment =
      CodeGenerator.setArrayElement(tempArrayDataName, elementType, k, element)

    s"""
       |$numElemCode
       |$allocation
       |if (!$leftIsNull) {
       |  for (int $k = 0; $k < $tempArrayDataName.numElements(); $k++) {
       |    $assignment
       |  }
       |} else {
       |  for (int $k = 0; $k < $tempArrayDataName.numElements(); $k++) {
       |    $tempArrayDataName.setNullAt($k);
       |  }
       |}
       |$arrayDataName = $tempArrayDataName;
     """.stripMargin
  }

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): ArrayRepeat = copy(left = newLeft, right = newRight)
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
  """,
  group = "array_funcs",
  since = "2.4.0")
case class ArrayRemove(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant with QueryErrorsBase {

  override def dataType: DataType = left.dataType

  override def inputTypes: Seq[AbstractDataType] = {
    (left.dataType, right.dataType) match {
      case (ArrayType(e1, hasNull), e2) =>
        TypeCoercion.findTightestCommonType(e1, e2) match {
          case Some(dt) => Seq(ArrayType(dt, hasNull), dt)
          case _ => Seq.empty
        }
      case _ => Seq.empty
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (ArrayType(e1, _), e2) if e1.sameType(e2) =>
        TypeUtils.checkForOrderingExpr(e2, prettyName)
      case _ =>
        DataTypeMismatch(
          errorSubClass = "ARRAY_FUNCTION_DIFF_TYPES",
          messageParameters = Map(
            "functionName" -> toSQLId(prettyName),
            "dataType" -> toSQLType(ArrayType),
            "leftType" -> toSQLType(left.dataType),
            "rightType" -> toSQLType(right.dataType)
          )
        )
    }
  }

  private def elementType: DataType = left.dataType.asInstanceOf[ArrayType].elementType

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(right.dataType)

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

    val allocation = CodeGenerator.createArrayData(
      values, elementType, newArraySize, s" $prettyName failed.")
    val assignment = CodeGenerator.createArrayAssignment(
      values, elementType, inputArray, pos, i, false)

    s"""
       |$allocation
       |int $pos = 0;
       |for (int $i = 0; $i < $inputArray.numElements(); $i ++) {
       |  if ($inputArray.isNullAt($i)) {
       |    $values.setNullAt($pos);
       |    $pos = $pos + 1;
       |  }
       |  else {
       |    if (!($isEqual)) {
       |      $assignment
       |      $pos = $pos + 1;
       |    }
       |  }
       |}
       |${ev.value} = $values;
     """.stripMargin
  }

  override def prettyName: String = "array_remove"

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): ArrayRemove = copy(left = newLeft, right = newRight)
}

/**
 * Will become common base class for [[ArrayDistinct]], [[ArrayUnion]], [[ArrayIntersect]],
 * and [[ArrayExcept]].
 */
trait ArraySetLike {
  protected def dt: DataType
  protected def et: DataType

  @transient protected lazy val canUseSpecializedHashSet = et match {
    case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType => true
    case _ => false
  }

  @transient protected lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(et)

  protected def resultArrayElementNullable = dt.asInstanceOf[ArrayType].containsNull

  protected def genGetValue(array: String, i: String): String =
    CodeGenerator.getValue(array, et, i)

  @transient protected lazy val (hsPostFix, hsTypeName) = {
    val ptName = CodeGenerator.primitiveTypeName(et)
    et match {
      // we cast byte/short to int when writing to the hash set.
      case ByteType | ShortType | IntegerType => ("$mcI$sp", "Int")
      case LongType => ("$mcJ$sp", ptName)
      case FloatType => ("$mcF$sp", ptName)
      case DoubleType => ("$mcD$sp", ptName)
    }
  }

  // we cast byte/short to int when writing to the hash set.
  @transient protected lazy val hsValueCast = et match {
    case ByteType | ShortType => "(int) "
    case _ => ""
  }

  // When hitting a null value, put a null holder in the ArrayBuilder. Finally we will
  // convert ArrayBuilder to ArrayData and setNull on the slot with null holder.
  @transient protected lazy val nullValueHolder = et match {
    case ByteType => "(byte) 0"
    case ShortType => "(short) 0"
    case LongType => "0L"
    case FloatType => "0.0f"
    case DoubleType => "0.0"
    case _ => "0"
  }

  protected def withResultArrayNullCheck(
      body: String,
      value: String,
      nullElementIndex: String): String = {
    if (resultArrayElementNullable) {
      s"""
         |$body
         |if ($nullElementIndex >= 0) {
         |  // result has null element
         |  $value.setNullAt($nullElementIndex);
         |}
       """.stripMargin
    } else {
      body
    }
  }

  def buildResultArray(
      builder: String,
      value : String,
      size : String,
      nullElementIndex : String): String = withResultArrayNullCheck(
    s"""
       |if ($size > ${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}) {
       |  throw QueryExecutionErrors.createArrayWithElementsExceedLimitError($size);
       |}
       |
       |if (!UnsafeArrayData.shouldUseGenericArrayData(${et.defaultSize}, $size)) {
       |  $value = UnsafeArrayData.fromPrimitiveArray($builder.result());
       |} else {
       |  $value = new ${classOf[GenericArrayData].getName}($builder.result());
       |}
     """.stripMargin, value, nullElementIndex)

}


/**
 * Removes duplicate values from the array.
 */
@ExpressionDescription(
  usage = "_FUNC_(array) - Removes duplicate values from the array.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3, null, 3));
       [1,2,3,null]
  """,
  group = "array_funcs",
  since = "2.4.0")
case class ArrayDistinct(child: Expression)
  extends UnaryExpression with ArraySetLike with ExpectsInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  override def dataType: DataType = child.dataType

  @transient private lazy val elementType: DataType = dataType.asInstanceOf[ArrayType].elementType

  override protected def dt: DataType = dataType
  override protected def et: DataType = elementType

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes() match {
      case f if f.isFailure => f
      case TypeCheckResult.TypeCheckSuccess =>
        TypeUtils.checkForOrderingExpr(elementType, prettyName)
    }
  }

  override def nullSafeEval(array: Any): Any = {
    val data = array.asInstanceOf[ArrayData]
    doEvaluation(data)
  }

  @transient private lazy val doEvaluation = if (TypeUtils.typeWithProperEquals(elementType)) {
    (array: ArrayData) =>
      val arrayBuffer = new scala.collection.mutable.ArrayBuffer[Any]
      val hs = new SQLOpenHashSet[Any]()
      val withNaNCheckFunc = SQLOpenHashSet.withNaNCheckFunc(elementType, hs,
        (value: Any) =>
          if (!hs.contains(value)) {
            if (arrayBuffer.size > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
              ArrayBinaryLike.throwUnionLengthOverflowException(arrayBuffer.size)
            }
            arrayBuffer += value
            hs.add(value)
          },
        (valueNaN: Any) => arrayBuffer += valueNaN)
      val withNullCheckFunc = SQLOpenHashSet.withNullCheckFunc(elementType, hs,
        (value: Any) => withNaNCheckFunc(value),
        () => arrayBuffer += null)
      var i = 0
      while (i < array.numElements()) {
        withNullCheckFunc(array, i)
        i += 1
      }
      new GenericArrayData(arrayBuffer)
  } else {
    (data: ArrayData) => {
      val array = data.toArray[AnyRef](elementType)
      val arrayBuffer = new scala.collection.mutable.ArrayBuffer[AnyRef]
      var alreadyStoredNull = false
      for (i <- array.indices) {
        if (array(i) != null) {
          var found = false
          var j = 0
          while (!found && j < arrayBuffer.size) {
            val va = arrayBuffer(j)
            found = (va != null) && ordering.equiv(va, array(i))
            j += 1
          }
          if (!found) {
            arrayBuffer += array(i)
          }
        } else {
          // De-duplicate the null values.
          if (!alreadyStoredNull) {
            arrayBuffer += array(i)
            alreadyStoredNull = true
          }
        }
      }
      new GenericArrayData(arrayBuffer)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val i = ctx.freshName("i")
    val value = ctx.freshName("value")
    val size = ctx.freshName("size")

    if (canUseSpecializedHashSet) {
      val jt = CodeGenerator.javaType(elementType)
      val ptName = CodeGenerator.primitiveTypeName(jt)

      nullSafeCodeGen(ctx, ev, (array) => {
        val nullElementIndex = ctx.freshName("nullElementIndex")
        val builder = ctx.freshName("builder")
        val openHashSet = classOf[SQLOpenHashSet[_]].getName
        val classTag = s"scala.reflect.ClassTag$$.MODULE$$.$hsTypeName()"
        val hashSet = ctx.freshName("hashSet")
        val arrayBuilder = classOf[mutable.ArrayBuilder[_]].getName
        val arrayBuilderClass = s"$arrayBuilder$$of$ptName"

        // Only need to track null element index when array's element is nullable.
        val declareNullTrackVariables = if (resultArrayElementNullable) {
          s"""
             |int $nullElementIndex = -1;
           """.stripMargin
        } else {
          ""
        }

        val body =
          s"""
             |if (!$hashSet.contains($hsValueCast$value)) {
             |  if (++$size > ${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}) {
             |    break;
             |  }
             |  $hashSet.add$hsPostFix($hsValueCast$value);
             |  $builder.$$plus$$eq($value);
             |}
           """.stripMargin

        val withNaNCheckCodeGenerator =
          (array: String, index: String) =>
              s"$jt $value = ${genGetValue(array, index)};" +
                SQLOpenHashSet.withNaNCheckCode(elementType, value, hashSet, body,
                  (valueNaN: String) =>
                    s"""
                       |$size++;
                       |$builder.$$plus$$eq($valueNaN);
                     """.stripMargin)

        val processArray = SQLOpenHashSet.withNullCheckCode(
          resultArrayElementNullable,
          resultArrayElementNullable,
          array, i, hashSet, withNaNCheckCodeGenerator,
          s"""
             |$nullElementIndex = $size;
             |$size++;
             |$builder.$$plus$$eq($nullValueHolder);
           """.stripMargin)

        s"""
           |$openHashSet $hashSet = new $openHashSet$hsPostFix($classTag);
           |$declareNullTrackVariables
           |$arrayBuilderClass $builder = new $arrayBuilderClass();
           |int $size = 0;
           |for (int $i = 0; $i < $array.numElements(); $i++) {
           |  $processArray
           |}
           |${buildResultArray(builder, ev.value, size, nullElementIndex)}
         """.stripMargin
      })
    } else {
      nullSafeCodeGen(ctx, ev, (array) => {
        val expr = ctx.addReferenceObj("arrayDistinctExpr", this)
        s"${ev.value} = (ArrayData)$expr.nullSafeEval($array);"
      })
    }
  }

  override def prettyName: String = "array_distinct"

  override protected def withNewChildInternal(newChild: Expression): ArrayDistinct =
    copy(child = newChild)
}

/**
 * Will become common base class for [[ArrayUnion]], [[ArrayIntersect]], and [[ArrayExcept]].
 */
trait ArrayBinaryLike
  extends BinaryArrayExpressionWithImplicitCast with ArraySetLike with NullIntolerant {
  override protected def dt: DataType = dataType
  override protected def et: DataType = elementType

  override def checkInputDataTypes(): TypeCheckResult = {
    val typeCheckResult = super.checkInputDataTypes()
    if (typeCheckResult.isSuccess) {
      TypeUtils.checkForOrderingExpr(dataType.asInstanceOf[ArrayType].elementType, prettyName)
    } else {
      typeCheckResult
    }
  }
}

object ArrayBinaryLike {
  def throwUnionLengthOverflowException(length: Int): Unit = {
    throw QueryExecutionErrors.unionArrayWithElementsExceedLimitError(length)
  }
}


/**
 * Returns an array of the elements in the union of x and y, without duplicates
 */
@ExpressionDescription(
  usage = """
    _FUNC_(array1, array2) - Returns an array of the elements in the union of array1 and array2,
      without duplicates.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), array(1, 3, 5));
       [1,2,3,5]
  """,
  group = "array_funcs",
  since = "2.4.0")
case class ArrayUnion(left: Expression, right: Expression) extends ArrayBinaryLike
  with ComplexTypeMergingExpression {

  @transient lazy val evalUnion: (ArrayData, ArrayData) => ArrayData = {
    if (TypeUtils.typeWithProperEquals(elementType)) {
      (array1, array2) =>
        val arrayBuffer = new scala.collection.mutable.ArrayBuffer[Any]
        val hs = new SQLOpenHashSet[Any]()
        val withNaNCheckFunc = SQLOpenHashSet.withNaNCheckFunc(elementType, hs,
          (value: Any) =>
            if (!hs.contains(value)) {
              if (arrayBuffer.size > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
                ArrayBinaryLike.throwUnionLengthOverflowException(arrayBuffer.size)
              }
              arrayBuffer += value
              hs.add(value)
            },
          (valueNaN: Any) => arrayBuffer += valueNaN)
        val withNullCheckFunc = SQLOpenHashSet.withNullCheckFunc(elementType, hs,
          (value: Any) => withNaNCheckFunc(value),
          () => arrayBuffer += null
        )
        Seq(array1, array2).foreach { array =>
          var i = 0
          while (i < array.numElements()) {
            withNullCheckFunc(array, i)
            i += 1
          }
        }
        new GenericArrayData(arrayBuffer)
    } else {
      (array1, array2) =>
        val arrayBuffer = new scala.collection.mutable.ArrayBuffer[Any]
        var alreadyIncludeNull = false
        Seq(array1, array2).foreach(_.foreach(elementType, (_, elem) => {
          var found = false
          if (elem == null) {
            if (alreadyIncludeNull) {
              found = true
            } else {
              alreadyIncludeNull = true
            }
          } else {
            // check elem is already stored in arrayBuffer or not?
            var j = 0
            while (!found && j < arrayBuffer.size) {
              val va = arrayBuffer(j)
              if (va != null && ordering.equiv(va, elem)) {
                found = true
              }
              j = j + 1
            }
          }
          if (!found) {
            if (arrayBuffer.length > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
              ArrayBinaryLike.throwUnionLengthOverflowException(arrayBuffer.length)
            }
            arrayBuffer += elem
          }
        }))
        new GenericArrayData(arrayBuffer)
    }
  }

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val array1 = input1.asInstanceOf[ArrayData]
    val array2 = input2.asInstanceOf[ArrayData]

    evalUnion(array1, array2)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val i = ctx.freshName("i")
    val value = ctx.freshName("value")
    val size = ctx.freshName("size")
    if (canUseSpecializedHashSet) {
      val jt = CodeGenerator.javaType(elementType)
      val ptName = CodeGenerator.primitiveTypeName(jt)

      nullSafeCodeGen(ctx, ev, (array1, array2) => {
        val nullElementIndex = ctx.freshName("nullElementIndex")
        val builder = ctx.freshName("builder")
        val array = ctx.freshName("array")
        val arrays = ctx.freshName("arrays")
        val arrayDataIdx = ctx.freshName("arrayDataIdx")
        val openHashSet = classOf[SQLOpenHashSet[_]].getName
        val classTag = s"scala.reflect.ClassTag$$.MODULE$$.$hsTypeName()"
        val hashSet = ctx.freshName("hashSet")
        val arrayBuilder = classOf[mutable.ArrayBuilder[_]].getName
        val arrayBuilderClass = s"$arrayBuilder$$of$ptName"

        val body =
          s"""
             |if (!$hashSet.contains($hsValueCast$value)) {
             |  if (++$size > ${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}) {
             |    break;
             |  }
             |  $hashSet.add$hsPostFix($hsValueCast$value);
             |  $builder.$$plus$$eq($value);
             |}
           """.stripMargin

        val withNaNCheckCodeGenerator =
          (array: String, index: String) =>
            s"$jt $value = ${genGetValue(array, index)};" +
            SQLOpenHashSet.withNaNCheckCode(elementType, value, hashSet, body,
              (valueNaN: String) =>
                s"""
                   |$size++;
                   |$builder.$$plus$$eq($valueNaN);
                     """.stripMargin)

        val processArray = SQLOpenHashSet.withNullCheckCode(
          resultArrayElementNullable,
          resultArrayElementNullable,
          array, i, hashSet, withNaNCheckCodeGenerator,
          s"""
             |$nullElementIndex = $size;
             |$size++;
             |$builder.$$plus$$eq($nullValueHolder);
           """.stripMargin)

        // Only need to track null element index when result array's element is nullable.
        val declareNullTrackVariables = if (resultArrayElementNullable) {
          s"""
             |int $nullElementIndex = -1;
           """.stripMargin
        } else {
          ""
        }

        s"""
           |$openHashSet $hashSet = new $openHashSet$hsPostFix($classTag);
           |$declareNullTrackVariables
           |int $size = 0;
           |$arrayBuilderClass $builder = new $arrayBuilderClass();
           |ArrayData[] $arrays = new ArrayData[]{$array1, $array2};
           |for (int $arrayDataIdx = 0; $arrayDataIdx < 2; $arrayDataIdx++) {
           |  ArrayData $array = $arrays[$arrayDataIdx];
           |  for (int $i = 0; $i < $array.numElements(); $i++) {
           |    $processArray
           |  }
           |}
           |${buildResultArray(builder, ev.value, size, nullElementIndex)}
         """.stripMargin
      })
    } else {
      nullSafeCodeGen(ctx, ev, (array1, array2) => {
        val expr = ctx.addReferenceObj("arrayUnionExpr", this)
        s"${ev.value} = (ArrayData)$expr.nullSafeEval($array1, $array2);"
      })
    }
  }

  override def prettyName: String = "array_union"

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): ArrayUnion = copy(left = newLeft, right = newRight)
}

object ArrayUnion {
  def unionOrdering(
      array1: ArrayData,
      array2: ArrayData,
      elementType: DataType,
      ordering: Ordering[Any]): ArrayData = {
    val arrayBuffer = new scala.collection.mutable.ArrayBuffer[Any]
    var alreadyIncludeNull = false
    Seq(array1, array2).foreach(_.foreach(elementType, (_, elem) => {
      var found = false
      if (elem == null) {
        if (alreadyIncludeNull) {
          found = true
        } else {
          alreadyIncludeNull = true
        }
      } else {
        // check elem is already stored in arrayBuffer or not?
        var j = 0
        while (!found && j < arrayBuffer.size) {
          val va = arrayBuffer(j)
          if (va != null && ordering.equiv(va, elem)) {
            found = true
          }
          j = j + 1
        }
      }
      if (!found) {
        if (arrayBuffer.length > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
          ArrayBinaryLike.throwUnionLengthOverflowException(arrayBuffer.length)
        }
        arrayBuffer += elem
      }
    }))
    new GenericArrayData(arrayBuffer)
  }
}

/**
 * Returns an array of the elements in the intersect of x and y, without duplicates
 */
@ExpressionDescription(
  usage = """
  _FUNC_(array1, array2) - Returns an array of the elements in the intersection of array1 and
    array2, without duplicates.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), array(1, 3, 5));
       [1,3]
  """,
  group = "array_funcs",
  since = "2.4.0")
case class ArrayIntersect(left: Expression, right: Expression) extends ArrayBinaryLike
  with ComplexTypeMergingExpression {

  private lazy val internalDataType: DataType = {
    dataTypeCheck
    ArrayType(elementType, leftArrayElementNullable && rightArrayElementNullable)
  }

  override def dataType: DataType = internalDataType

  @transient lazy val evalIntersect: (ArrayData, ArrayData) => ArrayData = {
    if (TypeUtils.typeWithProperEquals(elementType)) {
      (array1, array2) =>
        if (array1.numElements() != 0 && array2.numElements() != 0) {
          val hs = new SQLOpenHashSet[Any]
          val hsResult = new SQLOpenHashSet[Any]
          val arrayBuffer = new scala.collection.mutable.ArrayBuffer[Any]
          val withArray2NaNCheckFunc = SQLOpenHashSet.withNaNCheckFunc(elementType, hs,
            (value: Any) => hs.add(value),
            (valueNaN: Any) => {} )
          val withArray2NullCheckFunc = SQLOpenHashSet.withNullCheckFunc(elementType, hs,
            (value: Any) => withArray2NaNCheckFunc(value),
            () => {}
          )
          val withArray1NaNCheckFunc = SQLOpenHashSet.withNaNCheckFunc(elementType, hsResult,
            (value: Any) =>
              if (hs.contains(value) && !hsResult.contains(value)) {
                arrayBuffer += value
                hsResult.add(value)
              },
            (valueNaN: Any) =>
              if (hs.containsNaN()) {
                arrayBuffer += valueNaN
              })
          val withArray1NullCheckFunc = SQLOpenHashSet.withNullCheckFunc(elementType, hsResult,
            (value: Any) => withArray1NaNCheckFunc(value),
            () =>
              if (hs.containsNull()) {
                arrayBuffer += null
              }
          )

          var i = 0
          while (i < array2.numElements()) {
            withArray2NullCheckFunc(array2, i)
            i += 1
          }
          i = 0
          while (i < array1.numElements()) {
            withArray1NullCheckFunc(array1, i)
            i += 1
          }
          new GenericArrayData(arrayBuffer)
        } else {
          new GenericArrayData(Array.emptyObjectArray)
        }
    } else {
      (array1, array2) =>
        if (array1.numElements() != 0 && array2.numElements() != 0) {
          val arrayBuffer = new scala.collection.mutable.ArrayBuffer[Any]
          var alreadySeenNull = false
          var i = 0
          while (i < array1.numElements()) {
            var found = false
            val elem1 = array1.get(i, elementType)
            if (array1.isNullAt(i)) {
              if (!alreadySeenNull) {
                var j = 0
                while (!found && j < array2.numElements()) {
                  found = array2.isNullAt(j)
                  j += 1
                }
                // array2 is scanned only once for null element
                alreadySeenNull = true
              }
            } else {
              var j = 0
              while (!found && j < array2.numElements()) {
                if (!array2.isNullAt(j)) {
                  val elem2 = array2.get(j, elementType)
                  if (ordering.equiv(elem1, elem2)) {
                    // check whether elem1 is already stored in arrayBuffer
                    var foundArrayBuffer = false
                    var k = 0
                    while (!foundArrayBuffer && k < arrayBuffer.size) {
                      val va = arrayBuffer(k)
                      foundArrayBuffer = (va != null) && ordering.equiv(va, elem1)
                      k += 1
                    }
                    found = !foundArrayBuffer
                  }
                }
                j += 1
              }
            }
            if (found) {
              arrayBuffer += elem1
            }
            i += 1
          }
          new GenericArrayData(arrayBuffer)
        } else {
          new GenericArrayData(Array.emptyObjectArray)
        }
    }
  }

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val array1 = input1.asInstanceOf[ArrayData]
    val array2 = input2.asInstanceOf[ArrayData]

    evalIntersect(array1, array2)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val i = ctx.freshName("i")
    val value = ctx.freshName("value")
    val size = ctx.freshName("size")
    if (canUseSpecializedHashSet) {
      val jt = CodeGenerator.javaType(elementType)
      val ptName = CodeGenerator.primitiveTypeName(jt)

      nullSafeCodeGen(ctx, ev, (array1, array2) => {
        val nullElementIndex = ctx.freshName("nullElementIndex")
        val builder = ctx.freshName("builder")
        val openHashSet = classOf[SQLOpenHashSet[_]].getName
        val classTag = s"scala.reflect.ClassTag$$.MODULE$$.$hsTypeName()"
        val hashSet = ctx.freshName("hashSet")
        val hashSetResult = ctx.freshName("hashSetResult")
        val arrayBuilder = classOf[mutable.ArrayBuilder[_]].getName
        val arrayBuilderClass = s"$arrayBuilder$$of$ptName"

        val withArray2NaNCheckCodeGenerator =
          (array: String, index: String) =>
            s"$jt $value = ${genGetValue(array, index)};" +
              SQLOpenHashSet.withNaNCheckCode(elementType, value, hashSet,
                s"$hashSet.add$hsPostFix($hsValueCast$value);",
                (valueNaN: String) => "")

        val writeArray2ToHashSet = SQLOpenHashSet.withNullCheckCode(
          rightArrayElementNullable, leftArrayElementNullable,
          array2, i, hashSet, withArray2NaNCheckCodeGenerator, "")

        val body =
          s"""
             |if ($hashSet.contains($hsValueCast$value) &&
             |    !$hashSetResult.contains($hsValueCast$value)) {
             |  if (++$size > ${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}) {
             |    break;
             |  }
             |  $hashSetResult.add$hsPostFix($hsValueCast$value);
             |  $builder.$$plus$$eq($value);
             |}
           """.stripMargin

        val withArray1NaNCheckCodeGenerator =
          (array: String, index: String) =>
            s"$jt $value = ${genGetValue(array, index)};" +
              SQLOpenHashSet.withNaNCheckCode(elementType, value, hashSetResult, body,
                (valueNaN: Any) =>
                  s"""
                     |if ($hashSet.containsNaN()) {
                     |  ++$size;
                     |  $builder.$$plus$$eq($valueNaN);
                     |}
                 """.stripMargin)

        val processArray1 = SQLOpenHashSet.withNullCheckCode(
          leftArrayElementNullable, rightArrayElementNullable,
          array1, i, hashSetResult, withArray1NaNCheckCodeGenerator,
          s"""
             |if ($hashSet.containsNull()) {
             |  $nullElementIndex = $size;
             |  $size++;
             |  $builder.$$plus$$eq($nullValueHolder);
             |}
           """.stripMargin)

        // Only need to track null element index when result array's element is nullable.
        val declareNullTrackVariables = if (resultArrayElementNullable) {
          s"""
             |int $nullElementIndex = -1;
           """.stripMargin
        } else {
          ""
        }

        s"""
           |$openHashSet $hashSet = new $openHashSet$hsPostFix($classTag);
           |$openHashSet $hashSetResult = new $openHashSet$hsPostFix($classTag);
           |$declareNullTrackVariables
           |for (int $i = 0; $i < $array2.numElements(); $i++) {
           |  $writeArray2ToHashSet
           |}
           |$arrayBuilderClass $builder = new $arrayBuilderClass();
           |int $size = 0;
           |for (int $i = 0; $i < $array1.numElements(); $i++) {
           |  $processArray1
           |}
           |${buildResultArray(builder, ev.value, size, nullElementIndex)}
         """.stripMargin
      })
    } else {
      nullSafeCodeGen(ctx, ev, (array1, array2) => {
        val expr = ctx.addReferenceObj("arrayIntersectExpr", this)
        s"${ev.value} = (ArrayData)$expr.nullSafeEval($array1, $array2);"
      })
    }
  }

  override def prettyName: String = "array_intersect"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): ArrayIntersect =
    copy(left = newLeft, right = newRight)
}

/**
 * Returns an array of the elements in the intersect of x and y, without duplicates
 */
@ExpressionDescription(
  usage = """
  _FUNC_(array1, array2) - Returns an array of the elements in array1 but not in array2,
    without duplicates.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), array(1, 3, 5));
       [2]
  """,
  group = "array_funcs",
  since = "2.4.0")
case class ArrayExcept(left: Expression, right: Expression) extends ArrayBinaryLike
  with ComplexTypeMergingExpression {

  private lazy val internalDataType: DataType = {
    dataTypeCheck
    left.dataType
  }

  override def dataType: DataType = internalDataType

  @transient lazy val evalExcept: (ArrayData, ArrayData) => ArrayData = {
    if (TypeUtils.typeWithProperEquals(elementType)) {
      (array1, array2) =>
        val hs = new SQLOpenHashSet[Any]
        val arrayBuffer = new scala.collection.mutable.ArrayBuffer[Any]
        val withArray2NaNCheckFunc = SQLOpenHashSet.withNaNCheckFunc(elementType, hs,
          (value: Any) => hs.add(value),
          (valueNaN: Any) => {})
        val withArray2NullCheckFunc = SQLOpenHashSet.withNullCheckFunc(elementType, hs,
          (value: Any) => withArray2NaNCheckFunc(value),
          () => {}
        )
        val withArray1NaNCheckFunc = SQLOpenHashSet.withNaNCheckFunc(elementType, hs,
          (value: Any) =>
            if (!hs.contains(value)) {
              arrayBuffer += value
              hs.add(value)
            },
          (valueNaN: Any) => arrayBuffer += valueNaN)
        val withArray1NullCheckFunc = SQLOpenHashSet.withNullCheckFunc(elementType, hs,
          (value: Any) => withArray1NaNCheckFunc(value),
          () => arrayBuffer += null
        )
        var i = 0
        while (i < array2.numElements()) {
          withArray2NullCheckFunc(array2, i)
          i += 1
        }
        i = 0
        while (i < array1.numElements()) {
          withArray1NullCheckFunc(array1, i)
          i += 1
        }
        new GenericArrayData(arrayBuffer)
    } else {
      (array1, array2) =>
        val arrayBuffer = new scala.collection.mutable.ArrayBuffer[Any]
        var scannedNullElements = false
        var i = 0
        while (i < array1.numElements()) {
          var found = false
          val elem1 = array1.get(i, elementType)
          if (elem1 == null) {
            if (!scannedNullElements) {
              var j = 0
              while (!found && j < array2.numElements()) {
                found = array2.isNullAt(j)
                j += 1
              }
              // array2 is scanned only once for null element
              scannedNullElements = true
            } else {
              found = true
            }
          } else {
            var j = 0
            while (!found && j < array2.numElements()) {
              val elem2 = array2.get(j, elementType)
              if (elem2 != null) {
                found = ordering.equiv(elem1, elem2)
              }
              j += 1
            }
            if (!found) {
              // check whether elem1 is already stored in arrayBuffer
              var k = 0
              while (!found && k < arrayBuffer.size) {
                val va = arrayBuffer(k)
                found = (va != null) && ordering.equiv(va, elem1)
                k += 1
              }
            }
          }
          if (!found) {
            arrayBuffer += elem1
          }
          i += 1
        }
        new GenericArrayData(arrayBuffer)
    }
  }

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val array1 = input1.asInstanceOf[ArrayData]
    val array2 = input2.asInstanceOf[ArrayData]

    evalExcept(array1, array2)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val i = ctx.freshName("i")
    val value = ctx.freshName("value")
    val size = ctx.freshName("size")
    if (canUseSpecializedHashSet) {
      val jt = CodeGenerator.javaType(elementType)
      val ptName = CodeGenerator.primitiveTypeName(jt)

      nullSafeCodeGen(ctx, ev, (array1, array2) => {
        val nullElementIndex = ctx.freshName("nullElementIndex")
        val builder = ctx.freshName("builder")
        val openHashSet = classOf[SQLOpenHashSet[_]].getName
        val classTag = s"scala.reflect.ClassTag$$.MODULE$$.$hsTypeName()"
        val hashSet = ctx.freshName("hashSet")
        val arrayBuilder = classOf[mutable.ArrayBuilder[_]].getName
        val arrayBuilderClass = s"$arrayBuilder$$of$ptName"

        val withArray2NaNCheckCodeGenerator =
          (array: String, index: String) =>
            s"$jt $value = ${genGetValue(array, i)};" +
              SQLOpenHashSet.withNaNCheckCode(elementType, value, hashSet,
                s"$hashSet.add$hsPostFix($hsValueCast$value);",
                (valueNaN: Any) => "")

        val writeArray2ToHashSet = SQLOpenHashSet.withNullCheckCode(
          rightArrayElementNullable, leftArrayElementNullable,
          array2, i, hashSet, withArray2NaNCheckCodeGenerator, "")

        val body =
          s"""
             |if (!$hashSet.contains($hsValueCast$value)) {
             |  if (++$size > ${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}) {
             |    break;
             |  }
             |  $hashSet.add$hsPostFix($hsValueCast$value);
             |  $builder.$$plus$$eq($value);
             |}
           """.stripMargin

        val withArray1NaNCheckCodeGenerator =
          (array: String, index: String) =>
            s"$jt $value = ${genGetValue(array, index)};" +
              SQLOpenHashSet.withNaNCheckCode(elementType, value, hashSet, body,
                (valueNaN: String) =>
                  s"""
                     |$size++;
                     |$builder.$$plus$$eq($valueNaN);
                 """.stripMargin)

        val processArray1 = SQLOpenHashSet.withNullCheckCode(
          leftArrayElementNullable,
          leftArrayElementNullable,
          array1, i, hashSet, withArray1NaNCheckCodeGenerator,
          s"""
             |$nullElementIndex = $size;
             |$size++;
             |$builder.$$plus$$eq($nullValueHolder);
           """.stripMargin)

        // Only need to track null element index when array1's element is nullable.
        val declareNullTrackVariables = if (leftArrayElementNullable) {
          s"""
             |int $nullElementIndex = -1;
           """.stripMargin
        } else {
          ""
        }

        s"""
           |$openHashSet $hashSet = new $openHashSet$hsPostFix($classTag);
           |$declareNullTrackVariables
           |for (int $i = 0; $i < $array2.numElements(); $i++) {
           |  $writeArray2ToHashSet
           |}
           |$arrayBuilderClass $builder = new $arrayBuilderClass();
           |int $size = 0;
           |for (int $i = 0; $i < $array1.numElements(); $i++) {
           |  $processArray1
           |}
           |${buildResultArray(builder, ev.value, size, nullElementIndex)}
         """.stripMargin
      })
    } else {
      nullSafeCodeGen(ctx, ev, (array1, array2) => {
        val expr = ctx.addReferenceObj("arrayExceptExpr", this)
        s"${ev.value} = (ArrayData)$expr.nullSafeEval($array1, $array2);"
      })
    }
  }

  override def prettyName: String = "array_except"

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): ArrayExcept = copy(left = newLeft, right = newRight)
}
