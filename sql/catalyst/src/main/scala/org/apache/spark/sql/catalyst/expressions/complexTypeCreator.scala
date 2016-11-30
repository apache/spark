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
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, TypeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Returns an Array containing the evaluation of all children expressions.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, ...) - Returns an array with the given elements.",
  extended = """
    Examples:
      > SELECT _FUNC_(1, 2, 3);
       [1,2,3]
  """)
case class CreateArray(children: Seq[Expression]) extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), "function array")

  override def dataType: DataType = {
    ArrayType(
      children.headOption.map(_.dataType).getOrElse(NullType),
      containsNull = children.exists(_.nullable))
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    new GenericArrayData(children.map(_.eval(input)).toArray)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val arrayClass = classOf[GenericArrayData].getName
    val values = ctx.freshName("values")
    ctx.addMutableState("Object[]", values, s"this.$values = null;")

    ev.copy(code = s"""
      this.$values = new Object[${children.size}];""" +
      ctx.splitExpressions(
        ctx.INPUT_ROW,
        children.zipWithIndex.map { case (e, i) =>
          val eval = e.genCode(ctx)
          eval.code + s"""
            if (${eval.isNull}) {
              $values[$i] = null;
            } else {
              $values[$i] = ${eval.value};
            }
           """
        }) +
      s"""
        final ArrayData ${ev.value} = new $arrayClass($values);
        this.$values = null;
      """, isNull = "false")
  }

  override def prettyName: String = "array"
}

/**
 * Returns a catalyst Map containing the evaluation of all children expressions as keys and values.
 * The children are a flatted sequence of kv pairs, e.g. (key1, value1, key2, value2, ...)
 */
@ExpressionDescription(
  usage = "_FUNC_(key0, value0, key1, value1, ...) - Creates a map with the given key/value pairs.",
  extended = """
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
      TypeCheckResult.TypeCheckFailure(s"$prettyName expects a positive even number of arguments.")
    } else if (keys.map(_.dataType).distinct.length > 1) {
      TypeCheckResult.TypeCheckFailure("The given keys of function map should all be the same " +
        "type, but they are " + keys.map(_.dataType.simpleString).mkString("[", ", ", "]"))
    } else if (values.map(_.dataType).distinct.length > 1) {
      TypeCheckResult.TypeCheckFailure("The given values of function map should all be the same " +
        "type, but they are " + values.map(_.dataType.simpleString).mkString("[", ", ", "]"))
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = {
    MapType(
      keyType = keys.headOption.map(_.dataType).getOrElse(NullType),
      valueType = values.headOption.map(_.dataType).getOrElse(NullType),
      valueContainsNull = values.exists(_.nullable))
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val keyArray = keys.map(_.eval(input)).toArray
    if (keyArray.contains(null)) {
      throw new RuntimeException("Cannot use null as map key!")
    }
    val valueArray = values.map(_.eval(input)).toArray
    new ArrayBasedMapData(new GenericArrayData(keyArray), new GenericArrayData(valueArray))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val arrayClass = classOf[GenericArrayData].getName
    val mapClass = classOf[ArrayBasedMapData].getName
    val keyArray = ctx.freshName("keyArray")
    val valueArray = ctx.freshName("valueArray")
    ctx.addMutableState("Object[]", keyArray, s"this.$keyArray = null;")
    ctx.addMutableState("Object[]", valueArray, s"this.$valueArray = null;")

    val keyData = s"new $arrayClass($keyArray)"
    val valueData = s"new $arrayClass($valueArray)"
    ev.copy(code = s"""
      $keyArray = new Object[${keys.size}];
      $valueArray = new Object[${values.size}];""" +
      ctx.splitExpressions(
        ctx.INPUT_ROW,
        keys.zipWithIndex.map { case (key, i) =>
          val eval = key.genCode(ctx)
          s"""
            ${eval.code}
            if (${eval.isNull}) {
              throw new RuntimeException("Cannot use null as map key!");
            } else {
              $keyArray[$i] = ${eval.value};
            }
          """
        }) +
      ctx.splitExpressions(
        ctx.INPUT_ROW,
        values.zipWithIndex.map { case (value, i) =>
          val eval = value.genCode(ctx)
          s"""
            ${eval.code}
            if (${eval.isNull}) {
              $valueArray[$i] = null;
            } else {
              $valueArray[$i] = ${eval.value};
            }
          """
        }) +
      s"""
        final MapData ${ev.value} = new $mapClass($keyData, $valueData);
        this.$keyArray = null;
        this.$valueArray = null;
      """, isNull = "false")
  }

  override def prettyName: String = "map"
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
          "Only foldable StringType expressions are allowed to appear at odd position, got:" +
          s" ${invalidNames.mkString(",")}")
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
  extended = """
    Examples:
      > SELECT _FUNC_("a", 1, "b", 2, "c", 3);
       {"a":1,"b":2,"c":3}
  """)
// scalastyle:on line.size.limit
case class CreateNamedStruct(children: Seq[Expression]) extends CreateNamedStructLike {

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val rowClass = classOf[GenericInternalRow].getName
    val values = ctx.freshName("values")
    ctx.addMutableState("Object[]", values, s"this.$values = null;")

    ev.copy(code = s"""
      $values = new Object[${valExprs.size}];""" +
      ctx.splitExpressions(
        ctx.INPUT_ROW,
        valExprs.zipWithIndex.map { case (e, i) =>
          val eval = e.genCode(ctx)
          eval.code + s"""
          if (${eval.isNull}) {
            $values[$i] = null;
          } else {
            $values[$i] = ${eval.value};
          }"""
        }) +
      s"""
        final InternalRow ${ev.value} = new $rowClass($values);
        this.$values = null;
      """, isNull = "false")
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
    ExprCode(code = eval.code, isNull = "false", value = eval.value)
  }

  override def prettyName: String = "named_struct_unsafe"
}

/**
 * Creates a map after splitting the input text into key/value pairs using delimiters
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(text[, pairDelim[, keyValueDelim]]) - Creates a map after splitting the text into key/value pairs using delimiters. Default delimiters are ',' for `pairDelim` and ':' for `keyValueDelim`.",
  extended = """
    Examples:
      > SELECT _FUNC_('a:1,b:2,c:3', ',', ':');
       map("a":"1","b":"2","c":"3")
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

  override def dataType: DataType = MapType(StringType, StringType, valueContainsNull = false)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (Seq(pairDelim, keyValueDelim).exists(! _.foldable)) {
      TypeCheckResult.TypeCheckFailure(s"$prettyName's delimiters must be foldable.")
    } else {
      super.checkInputDataTypes()
    }
  }

  override def nullSafeEval(
      inputString: Any,
      stringDelimiter: Any,
      keyValueDelimiter: Any): Any = {
    val keyValues =
      inputString.asInstanceOf[UTF8String].split(stringDelimiter.asInstanceOf[UTF8String], -1)

    val iterator = new Iterator[(UTF8String, UTF8String)] {
      var index = 0
      val keyValueDelimiterUTF8String = keyValueDelimiter.asInstanceOf[UTF8String]

      override def hasNext: Boolean = {
        keyValues.length > index
      }

      override def next(): (UTF8String, UTF8String) = {
        val keyValueArray = keyValues(index).split(keyValueDelimiterUTF8String, 2)
        index += 1
        (keyValueArray(0), if (keyValueArray.length < 2) null else keyValueArray(1))
      }
    }
    ArrayBasedMapData(iterator, keyValues.size, identity, identity)
  }

  override def prettyName: String = "str_to_map"
}
