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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Returns an Array containing the evaluation of all children expressions.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, ...) - Returns an array with the given elements.",
  examples = """
    Examples:
      > SELECT _FUNC_(1, 2, 3);
       [1,2,3]
  """)
case class CreateArray(children: Seq[Expression]) extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult = {
    TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), s"function $prettyName")
  }

  override def dataType: ArrayType = {
    ArrayType(
      TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(children.map(_.dataType))
        .getOrElse(StringType),
      containsNull = children.exists(_.nullable))
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    new GenericArrayData(children.map(_.eval(input)).toArray)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val et = dataType.elementType
    val (allocation, assigns, arrayData) =
      GenArrayData.genCodeToCreateArrayData(ctx, et, children, "createArray")
    ev.copy(
      code = code"${allocation}${assigns}",
      value = JavaCode.variable(arrayData, dataType),
      isNull = FalseLiteral)
  }

  override def prettyName: String = "array"
}

private [sql] object GenArrayData {
  /**
   * Return Java code pieces based on DataType and array size to allocate ArrayData class
   *
   * @param ctx a [[CodegenContext]]
   * @param elementType data type of underlying array elements
   * @param elementsExpr concatenated set of [[Expression]] for each element of an underlying array
   * @param functionName string to include in the error message
   * @return (array allocation, concatenated assignments to each array elements, arrayData name)
   */
  def genCodeToCreateArrayData(
      ctx: CodegenContext,
      elementType: DataType,
      elementsExpr: Seq[Expression],
      functionName: String): (String, String, String) = {
    val arrayDataName = ctx.freshName("arrayData")
    val numElements = s"${elementsExpr.length}L"

    val initialization = CodeGenerator.createArrayData(
      arrayDataName, elementType, numElements, s" $functionName failed.")

    val assignments = elementsExpr.zipWithIndex.map { case (expr, i) =>
      val eval = expr.genCode(ctx)
      val setArrayElement = CodeGenerator.setArrayElement(
        arrayDataName, elementType, i.toString, eval.value)

      val assignment = if (!expr.nullable) {
        setArrayElement
      } else {
        s"""
           |if (${eval.isNull}) {
           |  $arrayDataName.setNullAt($i);
           |} else {
           |  $setArrayElement
           |}
         """.stripMargin
      }
      s"""
         |${eval.code}
         |$assignment
       """.stripMargin
    }
    val assignmentString = ctx.splitExpressionsWithCurrentInputs(
      expressions = assignments,
      funcName = "apply",
      extraArguments = ("ArrayData", arrayDataName) :: Nil)

    (initialization, assignmentString, arrayDataName)
  }
}

/**
 * Returns a catalyst Map containing the evaluation of all children expressions as keys and values.
 * The children are a flatted sequence of kv pairs, e.g. (key1, value1, key2, value2, ...)
 */
@ExpressionDescription(
  usage = "_FUNC_(key0, value0, key1, value1, ...) - Creates a map with the given key/value pairs.",
  examples = """
    Examples:
      > SELECT _FUNC_(1.0, '2', 3.0, '4');
       {1.0:"2",3.0:"4"}
  """)
case class CreateMap(children: Seq[Expression]) extends Expression {
  lazy val keys = children.indices.filter(_ % 2 == 0).map(children)
  lazy val values = children.indices.filter(_ % 2 != 0).map(children)

  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size % 2 != 0) {
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName expects a positive even number of arguments.")
    } else if (!TypeCoercion.haveSameType(keys.map(_.dataType))) {
      TypeCheckResult.TypeCheckFailure(
        "The given keys of function map should all be the same type, but they are " +
          keys.map(_.dataType.catalogString).mkString("[", ", ", "]"))
    } else if (!TypeCoercion.haveSameType(values.map(_.dataType))) {
      TypeCheckResult.TypeCheckFailure(
        "The given values of function map should all be the same type, but they are " +
          values.map(_.dataType.catalogString).mkString("[", ", ", "]"))
    } else {
      TypeUtils.checkForMapKeyType(dataType.keyType)
    }
  }

  override lazy val dataType: MapType = {
    MapType(
      keyType = TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(keys.map(_.dataType))
        .getOrElse(StringType),
      valueType = TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(values.map(_.dataType))
        .getOrElse(StringType),
      valueContainsNull = values.exists(_.nullable))
  }

  override def nullable: Boolean = false

  private lazy val mapBuilder = new ArrayBasedMapBuilder(dataType.keyType, dataType.valueType)

  override def eval(input: InternalRow): Any = {
    var i = 0
    while (i < keys.length) {
      mapBuilder.put(keys(i).eval(input), values(i).eval(input))
      i += 1
    }
    mapBuilder.build()
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val MapType(keyDt, valueDt, _) = dataType
    val (allocationKeyData, assignKeys, keyArrayData) =
      GenArrayData.genCodeToCreateArrayData(ctx, keyDt, keys, "createMap")
    val (allocationValueData, assignValues, valueArrayData) =
      GenArrayData.genCodeToCreateArrayData(ctx, valueDt, values, "createMap")
    val builderTerm = ctx.addReferenceObj("mapBuilder", mapBuilder)
    val code =
      code"""
       $allocationKeyData
       $assignKeys
       $allocationValueData
       $assignValues
       final MapData ${ev.value} = $builderTerm.from($keyArrayData, $valueArrayData);
      """
    ev.copy(code = code, isNull = FalseLiteral)
  }

  override def prettyName: String = "map"
}

/**
 * Returns a catalyst Map containing the two arrays in children expressions as keys and values.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(keys, values) - Creates a map with a pair of the given key/value arrays. All elements
      in keys should not be null""",
  examples = """
    Examples:
      > SELECT _FUNC_(array(1.0, 3.0), array('2', '4'));
       {1.0:"2",3.0:"4"}
  """, since = "2.4.0")
case class MapFromArrays(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, ArrayType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      val keyType = left.dataType.asInstanceOf[ArrayType].elementType
      TypeUtils.checkForMapKeyType(keyType)
    }
  }

  override def dataType: MapType = {
    MapType(
      keyType = left.dataType.asInstanceOf[ArrayType].elementType,
      valueType = right.dataType.asInstanceOf[ArrayType].elementType,
      valueContainsNull = right.dataType.asInstanceOf[ArrayType].containsNull)
  }

  private lazy val mapBuilder = new ArrayBasedMapBuilder(dataType.keyType, dataType.valueType)

  override def nullSafeEval(keyArray: Any, valueArray: Any): Any = {
    val keyArrayData = keyArray.asInstanceOf[ArrayData]
    val valueArrayData = valueArray.asInstanceOf[ArrayData]
    mapBuilder.from(keyArrayData.copy(), valueArrayData.copy())
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (keyArrayData, valueArrayData) => {
      val builderTerm = ctx.addReferenceObj("mapBuilder", mapBuilder)
      s"${ev.value} = $builderTerm.from($keyArrayData.copy(), $valueArrayData.copy());"
    })
  }

  override def prettyName: String = "map_from_arrays"
}

/**
 * An expression representing a not yet available attribute name. This expression is unevaluable
 * and as its name suggests it is a temporary place holder until we're able to determine the
 * actual attribute name.
 */
case object NamePlaceholder extends LeafExpression with Unevaluable {
  override lazy val resolved: Boolean = false
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = StringType
  override def prettyName: String = "NamePlaceholder"
  override def toString: String = prettyName
}

/**
 * Returns a Row containing the evaluation of all children expressions.
 */
object CreateStruct extends FunctionBuilder {
  def apply(children: Seq[Expression]): CreateNamedStruct = {
    CreateNamedStruct(children.zipWithIndex.flatMap {
      case (e: NamedExpression, _) if e.resolved => Seq(Literal(e.name), e)
      case (e: NamedExpression, _) => Seq(NamePlaceholder, e)
      case (e, index) => Seq(Literal(s"col${index + 1}"), e)
    })
  }

  /**
   * Entry to use in the function registry.
   */
  val registryEntry: (String, (ExpressionInfo, FunctionBuilder)) = {
    val info: ExpressionInfo = new ExpressionInfo(
      "org.apache.spark.sql.catalyst.expressions.NamedStruct",
      null,
      "struct",
      "_FUNC_(col1, col2, col3, ...) - Creates a struct with the given field values.",
      "",
      "",
      "",
      "",
      "")
    ("struct", (info, this))
  }
}

/**
 * Common base class for both [[CreateNamedStruct]] and [[CreateNamedStructUnsafe]].
 */
trait CreateNamedStructLike extends Expression {
  lazy val (nameExprs, valExprs) = children.grouped(2).map {
    case Seq(name, value) => (name, value)
  }.toList.unzip

  lazy val names = nameExprs.map(_.eval(EmptyRow))

  override def nullable: Boolean = false

  override def foldable: Boolean = valExprs.forall(_.foldable)

  override lazy val dataType: StructType = {
    val fields = names.zip(valExprs).map {
      case (name, expr) =>
        val metadata = expr match {
          case ne: NamedExpression => ne.metadata
          case _ => Metadata.empty
        }
        StructField(name.toString, expr.dataType, expr.nullable, metadata)
    }
    StructType(fields)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size % 2 != 0) {
      TypeCheckResult.TypeCheckFailure(s"$prettyName expects an even number of arguments.")
    } else {
      val invalidNames = nameExprs.filterNot(e => e.foldable && e.dataType == StringType)
      if (invalidNames.nonEmpty) {
        TypeCheckResult.TypeCheckFailure(
          s"Only foldable ${StringType.catalogString} expressions are allowed to appear at odd" +
            s" position, got: ${invalidNames.mkString(",")}")
      } else if (!names.contains(null)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure("Field name should not be null")
      }
    }
  }

  /**
   * Returns Aliased [[Expression]]s that could be used to construct a flattened version of this
   * StructType.
   */
  def flatten: Seq[NamedExpression] = valExprs.zip(names).map {
    case (v, n) => Alias(v, n.toString)()
  }

  override def eval(input: InternalRow): Any = {
    InternalRow(valExprs.map(_.eval(input)): _*)
  }
}

/**
 * Creates a struct with the given field names and values
 *
 * @param children Seq(name1, val1, name2, val2, ...)
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(name1, val1, name2, val2, ...) - Creates a struct with the given field names and values.",
  examples = """
    Examples:
      > SELECT _FUNC_("a", 1, "b", 2, "c", 3);
       {"a":1,"b":2,"c":3}
  """)
// scalastyle:on line.size.limit
case class CreateNamedStruct(children: Seq[Expression]) extends CreateNamedStructLike {

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val rowClass = classOf[GenericInternalRow].getName
    val values = ctx.freshName("values")
    val valCodes = valExprs.zipWithIndex.map { case (e, i) =>
      val eval = e.genCode(ctx)
      s"""
         |${eval.code}
         |if (${eval.isNull}) {
         |  $values[$i] = null;
         |} else {
         |  $values[$i] = ${eval.value};
         |}
       """.stripMargin
    }
    val valuesCode = ctx.splitExpressionsWithCurrentInputs(
      expressions = valCodes,
      funcName = "createNamedStruct",
      extraArguments = "Object[]" -> values :: Nil)

    ev.copy(code =
      code"""
         |Object[] $values = new Object[${valExprs.size}];
         |$valuesCode
         |final InternalRow ${ev.value} = new $rowClass($values);
         |$values = null;
       """.stripMargin, isNull = FalseLiteral)
  }

  override def prettyName: String = "named_struct"
}

/**
 * Creates a struct with the given field names and values. This is a variant that returns
 * UnsafeRow directly. The unsafe projection operator replaces [[CreateStruct]] with
 * this expression automatically at runtime.
 *
 * @param children Seq(name1, val1, name2, val2, ...)
 */
case class CreateNamedStructUnsafe(children: Seq[Expression]) extends CreateNamedStructLike {
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = GenerateUnsafeProjection.createCode(ctx, valExprs)
    ExprCode(code = eval.code, isNull = FalseLiteral, value = eval.value)
  }

  override def prettyName: String = "named_struct_unsafe"
}

/**
 * Creates a map after splitting the input text into key/value pairs using delimiters
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(text[, pairDelim[, keyValueDelim]]) - Creates a map after splitting the text into key/value pairs using delimiters. Default delimiters are ',' for `pairDelim` and ':' for `keyValueDelim`.",
  examples = """
    Examples:
      > SELECT _FUNC_('a:1,b:2,c:3', ',', ':');
       map("a":"1","b":"2","c":"3")
      > SELECT _FUNC_('a');
       map("a":null)
  """)
// scalastyle:on line.size.limit
case class StringToMap(text: Expression, pairDelim: Expression, keyValueDelim: Expression)
  extends TernaryExpression with CodegenFallback with ExpectsInputTypes {

  def this(child: Expression, pairDelim: Expression) = {
    this(child, pairDelim, Literal(":"))
  }

  def this(child: Expression) = {
    this(child, Literal(","), Literal(":"))
  }

  override def children: Seq[Expression] = Seq(text, pairDelim, keyValueDelim)

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, StringType)

  override def dataType: DataType = MapType(StringType, StringType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (Seq(pairDelim, keyValueDelim).exists(! _.foldable)) {
      TypeCheckResult.TypeCheckFailure(s"$prettyName's delimiters must be foldable.")
    } else {
      super.checkInputDataTypes()
    }
  }

  private lazy val mapBuilder = new ArrayBasedMapBuilder(StringType, StringType)

  override def nullSafeEval(
      inputString: Any,
      stringDelimiter: Any,
      keyValueDelimiter: Any): Any = {
    val keyValues =
      inputString.asInstanceOf[UTF8String].split(stringDelimiter.asInstanceOf[UTF8String], -1)
    val keyValueDelimiterUTF8String = keyValueDelimiter.asInstanceOf[UTF8String]

    var i = 0
    while (i < keyValues.length) {
      val keyValueArray = keyValues(i).split(keyValueDelimiterUTF8String, 2)
      val key = keyValueArray(0)
      val value = if (keyValueArray.length < 2) null else keyValueArray(1)
      mapBuilder.put(key, value)
      i += 1
    }
    mapBuilder.build()
  }

  override def prettyName: String = "str_to_map"
}
