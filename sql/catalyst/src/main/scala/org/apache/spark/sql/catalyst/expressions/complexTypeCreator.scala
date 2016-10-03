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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, TypeUtils}
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Returns an Array containing the evaluation of all children expressions.
 */
@ExpressionDescription(
  usage = "_FUNC_(n0, ...) - Returns an array with the given elements.")
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
      final boolean ${ev.isNull} = false;
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
      """)
  }

  override def prettyName: String = "array"
}

/**
 * Returns a catalyst Map containing the evaluation of all children expressions as keys and values.
 * The children are a flatted sequence of kv pairs, e.g. (key1, value1, key2, value2, ...)
 */
@ExpressionDescription(
  usage = "_FUNC_(key0, value0, key1, value1...) - Creates a map with the given key/value pairs.")
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
      final boolean ${ev.isNull} = false;
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
      """)
  }

  override def prettyName: String = "map"
}

/**
 * Common trait for [[CreateStruct]] and [[CreateStructUnsafe]].
 */
trait CreateStructLike extends Expression {
  val names: Seq[String]

  override lazy val resolved: Boolean = childrenResolved && names.size == children.size

  override def foldable: Boolean = children.forall(_.foldable)

  override lazy val dataType: StructType = {
    val fields = children.zip(names).map { case (child, name) =>
      val metadata = child match {
        case ne: NamedExpression => ne.metadata
        case _ => Metadata.empty
      }
      StructField(name, child.dataType, child.nullable, metadata)
    }
    StructType(fields)
  }

  lazy val flatten: Seq[NamedExpression] = children.zip(names).map { case (child, name) =>
    Alias(child, name)()
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    InternalRow(children.map(_.eval(input)): _*)
  }

  override def sql: String = {
    // Note that we always create SQL for a 'named_struct' here. By doing this we guarantee that
    // the names of the fields in the structure will always be the same, even if we modify the
    // naming algorithm used by CreateStruct.
    val namesAndChildren = children.zip(names).flatMap { case (child, name) =>
      Seq(Literal(name), child)
    }
    namesAndChildren.map(_.sql).mkString("named_struct(", ", ", ")")
  }
}

/**
 * Returns a Row containing the evaluation of all children expressions.
 */
@ExpressionDescription(
  usage = "_FUNC_(col1, col2, col3, ...) - Creates a struct with the given field values.")
case class CreateStruct(children: Seq[Expression], names: Seq[String]) extends CreateStructLike {
  def this(children: Seq[Expression]) = this(children, CreateStruct.generateNames(children))

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val rowClass = classOf[GenericInternalRow].getName
    val values = ctx.freshName("values")
    ctx.addMutableState("Object[]", values, s"this.$values = null;")

    ev.copy(code = s"""
      boolean ${ev.isNull} = false;
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
            }"""
        }) +
      s"""
        final InternalRow ${ev.value} = new $rowClass($values);
        this.$values = null;
      """)
  }

  override def prettyName: String = "struct"
}

object CreateStruct {
  /**
   * Construct a [[CreateStruct]] using unnamed expressions.
   */
  def apply(children: Seq[Expression]): CreateStruct = new CreateStruct(children)

  /**
   * Generate names for the given expressions. This method will only generate names when all the
   * expressions in the sequence are resolved. Names are generated using an expressions name (if
   * it is a [[NamedExpression]] or using the index of that expression.
   */
  def generateNames(children: Seq[Expression]): Seq[String] = {
    if (!children.forall(_.resolved)) Seq.empty
    else {
      children.zipWithIndex.map {
        case (ne: NamedExpression, _) => ne.name
        case (_, idx) => s"col${idx + 1}"
      }
    }
  }

  /**
   * Create a [[CreateStruct]] using custom names from a sequence of name value pairs.
   */
  def withNameValuePairs(nameChildPairs: Seq[(String, Expression)]): CreateStruct = {
    val (names, children) = nameChildPairs.unzip
    CreateStruct(children, names)
  }

  /**
   * Construct a [[CreateStruct]] using custom names. The input should adhere to the following
   * pattern: name_1, value_1, name_2, value_2, ...
   */
  def withFlatExpressions(namesAndChildren: Seq[Expression]): CreateStruct = {
    if (namesAndChildren.size % 2 != 0) {
      throw new AnalysisException("An even number of arguments is expected")
    }
    val (children, names) = namesAndChildren.grouped(2).toSeq.map {
      case Seq(n, child) =>
        if (!n.foldable || n.dataType != StringType) {
          throw new AnalysisException(
            s"Only foldable StringType expressions are allowed to appear at odd position, got: $n")
        }
        val name = n.eval(EmptyRow)
        if (name == null) {
          throw new AnalysisException("Field name should not be null")
        }
        (child, name.toString)
    }.unzip
    CreateStruct(children, names)
  }

  /**
   * Registration used to add the 'named_struct' to the
   * [[org.apache.spark.sql.catalyst.analysis.FunctionRegistry]]
   */
  val named_struct: (String, (ExpressionInfo, FunctionBuilder)) = {
    val info = new ExpressionInfo(
      classOf[CreateStruct].getCanonicalName,
      "named_struct",
      "_FUNC_(name1, val1, name2, val2, ...) - "
        + "Creates a struct with the given field names and values.",
      "")
    ("named_struct", (info, withFlatExpressions))
  }
}

/**
 * Returns a Row containing the evaluation of all children expressions. This is a variant that
 * returns UnsafeRow directly. The unsafe projection operator replaces [[CreateStruct]] with
 * this expression automatically at runtime.
 */
case class CreateStructUnsafe(
    children: Seq[Expression],
    names: Seq[String])
  extends CreateStructLike {
  def this(children: Seq[Expression]) = this(children, CreateStruct.generateNames(children))

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = GenerateUnsafeProjection.createCode(ctx, children)
    ExprCode(code = eval.code, isNull = eval.isNull, value = eval.value)
  }

  override def prettyName: String = "struct_unsafe"
}

/**
 * Creates a map after splitting the input text into key/value pairs using delimiters
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(text[, pairDelim, keyValueDelim]) - Creates a map after splitting the text into key/value pairs using delimiters. Default delimiters are ',' for pairDelim and ':' for keyValueDelim.",
  extended = """ > SELECT _FUNC_('a:1,b:2,c:3',',',':');\n map("a":"1","b":"2","c":"3") """)
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

  override def nullSafeEval(str: Any, delim1: Any, delim2: Any): Any = {
    val array = str.asInstanceOf[UTF8String]
      .split(delim1.asInstanceOf[UTF8String], -1)
      .map { kv =>
        val arr = kv.split(delim2.asInstanceOf[UTF8String], 2)
        if (arr.length < 2) {
          Array(arr(0), null)
        } else {
          arr
        }
      }
    ArrayBasedMapData(array.map(_ (0)), array.map(_ (1)))
  }

  override def prettyName: String = "str_to_map"
}
