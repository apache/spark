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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.{InternalRow, collation}
import org.apache.spark.sql.catalyst.analysis.{Resolver, TypeCheckResult, TypeCoercion, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.{FUNC_ALIAS, FunctionBuilder}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.trees.{LeafLike, UnaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeNonCSAICollation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

/**
 * Trait to indicate the expression does not throw an exception by itself when they are evaluated.
 * For example, UDFs, [[AssertTrue]], etc can throw an exception when they are executed.
 * In such case, it is necessary to call [[Expression.eval]], and the optimization rule should
 * not ignore it.
 *
 * This trait can be used in an optimization rule such as
 * [[org.apache.spark.sql.catalyst.optimizer.ConstantFolding]] to fold the expressions that
 * do not need to execute, for example, `size(array(c0, c1, c2))`.
 */
trait NoThrow

/**
 * Returns an Array containing the evaluation of all children expressions.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, ...) - Returns an array with the given elements.",
  examples = """
    Examples:
      > SELECT _FUNC_(1, 2, 3);
       [1,2,3]
  """,
  since = "1.1.0",
  group = "array_funcs")
case class CreateArray(children: Seq[Expression], useStringTypeWhenEmpty: Boolean)
  extends Expression with NoThrow {

  def this(children: Seq[Expression]) = {
    this(children, SQLConf.get.getConf(SQLConf.LEGACY_CREATE_EMPTY_COLLECTION_USING_STRING_TYPE))
  }

  override def foldable: Boolean = children.forall(_.foldable)

  override def stringArgs: Iterator[Any] = super.stringArgs.take(1)

  override def checkInputDataTypes(): TypeCheckResult = {
    TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), prettyName)
  }

  private val defaultElementType: DataType = {
    if (useStringTypeWhenEmpty) {
      SQLConf.get.defaultStringType
    } else {
      NullType
    }
  }

  override def dataType: ArrayType = {
    ArrayType(
      TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(children.map(_.dataType))
        .getOrElse(defaultElementType),
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

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): CreateArray =
    copy(children = newChildren)
}

object CreateArray {
  def apply(children: Seq[Expression]): CreateArray = {
    new CreateArray(children)
  }
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
  """,
  since = "2.0.0",
  group = "map_funcs")
case class CreateMap(children: Seq[Expression], useStringTypeWhenEmpty: Boolean)
  extends Expression with NoThrow {

  def this(children: Seq[Expression]) = {
    this(children, SQLConf.get.getConf(SQLConf.LEGACY_CREATE_EMPTY_COLLECTION_USING_STRING_TYPE))
  }

  lazy val keys = children.indices.filter(_ % 2 == 0).map(children)
  lazy val values = children.indices.filter(_ % 2 != 0).map(children)

  private val defaultElementType: DataType = {
    if (useStringTypeWhenEmpty) {
      SQLConf.get.defaultStringType
    } else {
      NullType
    }
  }

  override def foldable: Boolean = children.forall(_.foldable)

  override def stringArgs: Iterator[Any] = super.stringArgs.take(1)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size % 2 != 0) {
      throw QueryCompilationErrors.wrongNumArgsError(
        toSQLId(prettyName), Seq("2n (n > 0)"), children.length
      )
    } else if (!TypeCoercion.haveSameType(keys.map(_.dataType))) {
      DataTypeMismatch(
        errorSubClass = "CREATE_MAP_KEY_DIFF_TYPES",
        messageParameters = Map(
          "functionName" -> toSQLId(prettyName),
          "dataType" -> keys.map(key => toSQLType(key.dataType)).mkString("[", ", ", "]")
        )
      )
    } else if (!TypeCoercion.haveSameType(values.map(_.dataType))) {
      DataTypeMismatch(
        errorSubClass = "CREATE_MAP_VALUE_DIFF_TYPES",
        messageParameters = Map(
          "functionName" -> toSQLId(prettyName),
          "dataType" -> values.map(value => toSQLType(value.dataType)).mkString("[", ", ", "]")
        )
      )
    } else {
      TypeUtils.checkForMapKeyType(dataType.keyType)
    }
  }

  override lazy val dataType: MapType = {
    MapType(
      keyType = TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(keys.map(_.dataType))
        .getOrElse(defaultElementType),
      valueType = TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(values.map(_.dataType))
        .getOrElse(defaultElementType),
      valueContainsNull = values.exists(_.nullable))
  }

  override def nullable: Boolean = false

  private lazy val mapBuilder = new ArrayBasedMapBuilder(dataType.keyType, dataType.valueType)

  override def stateful: Boolean = true

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

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): CreateMap =
    copy(children = newChildren)
}

object CreateMap {
  def apply(children: Seq[Expression]): CreateMap = {
    new CreateMap(children)
  }
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
  """,
  since = "2.4.0",
  group = "map_funcs")
case class MapFromArrays(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes with NullIntolerant {

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

  override def stateful: Boolean = true

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

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): MapFromArrays =
    copy(left = newLeft, right = newRight)
}

/**
 * An expression representing a not yet available attribute name. This expression is unevaluable
 * and as its name suggests it is a temporary place holder until we're able to determine the
 * actual attribute name.
 */
case object NamePlaceholder extends LeafExpression with Unevaluable {
  override lazy val resolved: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = SQLConf.get.defaultStringType
  override def prettyName: String = "NamePlaceholder"
  override def toString: String = prettyName
}

/**
 * Returns a Row containing the evaluation of all children expressions.
 */
object CreateStruct {
  /**
   * Returns a named struct with generated names or using the names when available.
   * It should not be used for `struct` expressions or functions explicitly called
   * by users.
   */
  def apply(children: Seq[Expression]): CreateNamedStruct = {
    CreateNamedStruct(children.zipWithIndex.flatMap {
      // For multi-part column name like `struct(a.b.c)`, it may be resolved into:
      //   1. Attribute if `a.b.c` is simply a qualified column name.
      //   2. GetStructField if `a.b` refers to a struct-type column.
      //   3. GetArrayStructFields if `a.b` refers to a array-of-struct-type column.
      //   4. GetMapValue if `a.b` refers to a map-type column.
      // We should always use the last part of the column name (`c` in the above example) as the
      // alias name inside CreateNamedStruct.
      case (u: UnresolvedAttribute, _) => Seq(Literal(u.nameParts.last), u)
      case (u @ UnresolvedExtractValue(_, e: Literal), _) if e.dataType.isInstanceOf[StringType] =>
        Seq(e, u)
      case (a: Alias, _) => Seq(Literal(a.name), a)
      case (e: NamedExpression, _) if e.resolved => Seq(Literal(e.name), e)
      case (e: NamedExpression, _) => Seq(NamePlaceholder, e)
      case (g @ GetStructField(_, _, Some(name)), _) => Seq(Literal(name), g)
      case (e, index) => Seq(Literal(s"col${index + 1}"), e)
    })
  }

  /**
   * Returns a named struct with a pretty SQL name. It will show the pretty SQL string
   * in its output column name as if `struct(...)` was called. Should be
   * used for `struct` expressions or functions explicitly called by users.
   */
  def create(children: Seq[Expression]): CreateNamedStruct = {
    val expr = CreateStruct(children)
    expr.setTagValue(FUNC_ALIAS, "struct")
    expr
  }

  /**
   * Entry to use in the function registry.
   */
  val registryEntry: (String, (ExpressionInfo, FunctionBuilder)) = {
    val info: ExpressionInfo = new ExpressionInfo(
      classOf[CreateNamedStruct].getCanonicalName,
      null,
      "struct",
      "_FUNC_(col1, col2, col3, ...) - Creates a struct with the given field values.",
      "",
      """
        |    Examples:
        |      > SELECT _FUNC_(1, 2, 3);
        |       {"col1":1,"col2":2,"col3":3}
        |  """.stripMargin,
      "",
      "struct_funcs",
      "1.4.0",
      "",
      "built-in")
    ("struct", (info, this.create))
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
  """,
  since = "1.5.0",
  group = "struct_funcs")
// scalastyle:on line.size.limit
case class CreateNamedStruct(children: Seq[Expression]) extends Expression with NoThrow {
  lazy val (nameExprs, valExprs) = children.grouped(2).map {
    case Seq(name, value) => (name, value)
  }.toList.unzip

  lazy val names = nameExprs.map(_.eval(EmptyRow))

  override def nullable: Boolean = false

  override def foldable: Boolean = valExprs.forall(_.foldable)

  final override val nodePatterns: Seq[TreePattern] = Seq(CREATE_NAMED_STRUCT)

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
      throw QueryCompilationErrors.wrongNumArgsError(
        toSQLId(prettyName), Seq("2n (n > 0)"), children.length
      )
    } else {
      val invalidNames = nameExprs.filterNot(e => e.foldable && e.dataType.isInstanceOf[StringType])
      if (invalidNames.nonEmpty) {
        DataTypeMismatch(
          errorSubClass = "CREATE_NAMED_STRUCT_WITHOUT_FOLDABLE_STRING",
          messageParameters = Map(
            "inputExprs" -> invalidNames.map(toSQLExpr(_)).mkString("[", ", ", "]")
          )
        )
      } else if (!names.contains(null)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_NULL",
          messageParameters = Map(
            "exprName" -> nameExprs.map(toSQLExpr).mkString("[", ", ", "]")
          )
        )
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

  // There is an alias set at `CreateStruct.create`. If there is an alias,
  // this is the struct function explicitly called by a user and we should
  // respect it in the SQL string as `struct(...)`.
  override def prettyName: String = getTagValue(FUNC_ALIAS).getOrElse("named_struct")

  override def sql: String = getTagValue(FUNC_ALIAS).map { alias =>
    val childrenSQL = children.indices.filter(_ % 2 == 1).map(children(_).sql).mkString(", ")
    s"$alias($childrenSQL)"
  }.getOrElse(super.sql)

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): CreateNamedStruct = copy(children = newChildren)
}

/**
 * Creates a map after splitting the input text into key/value pairs using delimiters
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(text[, pairDelim[, keyValueDelim]]) - Creates a map after splitting the text into key/value pairs using delimiters. Default delimiters are ',' for `pairDelim` and ':' for `keyValueDelim`. Both `pairDelim` and `keyValueDelim` are treated as regular expressions.",
  examples = """
    Examples:
      > SELECT _FUNC_('a:1,b:2,c:3', ',', ':');
       {"a":"1","b":"2","c":"3"}
      > SELECT _FUNC_('a');
       {"a":null}
  """,
  since = "2.0.1",
  group = "map_funcs")
// scalastyle:on line.size.limit
case class StringToMap(text: Expression, pairDelim: Expression, keyValueDelim: Expression)
  extends TernaryExpression with ExpectsInputTypes with NullIntolerant {

  def this(child: Expression, pairDelim: Expression) = {
    this(child, pairDelim, Literal(":"))
  }

  def this(child: Expression) = {
    this(child, Literal(","), Literal(":"))
  }

  override def stateful: Boolean = true

  override def first: Expression = text
  override def second: Expression = pairDelim
  override def third: Expression = keyValueDelim

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeNonCSAICollation, StringTypeNonCSAICollation, StringTypeNonCSAICollation)

  override def dataType: DataType = MapType(first.dataType, first.dataType)

  private lazy val mapBuilder = new ArrayBasedMapBuilder(first.dataType, first.dataType)

  private final lazy val collationId: Int = text.dataType.asInstanceOf[StringType].collationId

  override def nullSafeEval(
      inputString: Any,
      stringDelimiter: Any,
      keyValueDelimiter: Any): Any = {
    val keyValues = collation.CollationAwareUTF8String.splitSQL(inputString.asInstanceOf[UTF8String],
      stringDelimiter.asInstanceOf[UTF8String], -1, collationId)
    val keyValueDelimiterUTF8String = keyValueDelimiter.asInstanceOf[UTF8String]

    var i = 0
    while (i < keyValues.length) {
      val keyValueArray = collation.CollationAwareUTF8String.splitSQL(
        keyValues(i), keyValueDelimiterUTF8String, 2, collationId)
      val key = keyValueArray(0)
      val value = if (keyValueArray.length < 2) null else keyValueArray(1)
      mapBuilder.put(key, value)
      i += 1
    }
    mapBuilder.build()
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val builderTerm = ctx.addReferenceObj("mapBuilder", mapBuilder)
    val keyValues = ctx.freshName("kvs")

    nullSafeCodeGen(ctx, ev, (text, pd, kvd) =>
      s"""
         |UTF8String[] $keyValues = CollationAwareUTF8String.splitSQL($text, $pd, -1, $collationId);
         |for(UTF8String kvEntry: $keyValues) {
         |  UTF8String[] kv = CollationAwareUTF8String.splitSQL(kvEntry, $kvd, 2, $collationId);
         |  $builderTerm.put(kv[0], kv.length == 2 ? kv[1] : null);
         |}
         |${ev.value} = $builderTerm.build();
         |""".stripMargin
    )
  }

  override def prettyName: String = "str_to_map"

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = copy(
    text = newFirst,
    pairDelim = newSecond,
    keyValueDelim = newThird
  )
}

/**
 * Represents an operation to be applied to the fields of a struct.
 */
trait StructFieldsOperation extends Expression with Unevaluable {

  val resolver: Resolver = SQLConf.get.resolver

  override def dataType: DataType = throw SparkException.internalError(
    "StructFieldsOperation.dataType should not be called.")

  override def nullable: Boolean = throw SparkException.internalError(
    "StructFieldsOperation.nullable should not be called.")

  /**
   * Returns an updated list of StructFields and Expressions that will ultimately be used
   * as the fields argument for [[StructType]] and as the children argument for
   * [[CreateNamedStruct]] respectively inside of [[UpdateFields]].
   */
  def apply(values: Seq[(StructField, Expression)]): Seq[(StructField, Expression)]
}

/**
 * Add or replace a field by name.
 *
 * We extend [[Unevaluable]] here to ensure that [[UpdateFields]] can include it as part of its
 * children, and thereby enable the analyzer to resolve and transform valExpr as necessary.
 */
case class WithField(name: String, valExpr: Expression)
  extends StructFieldsOperation with UnaryLike[Expression] {

  override def apply(values: Seq[(StructField, Expression)]): Seq[(StructField, Expression)] = {
    val newFieldExpr = (StructField(name, valExpr.dataType, valExpr.nullable), valExpr)
    val result = ArrayBuffer.empty[(StructField, Expression)]
    var hasMatch = false
    for (existingFieldExpr @ (existingField, _) <- values) {
      if (resolver(existingField.name, name)) {
        hasMatch = true
        result += newFieldExpr
      } else {
        result += existingFieldExpr
      }
    }
    if (!hasMatch) result += newFieldExpr
    result.toSeq
  }

  override def child: Expression = valExpr

  override def prettyName: String = "WithField"

  override protected def withNewChildInternal(newChild: Expression): WithField =
    copy(valExpr = newChild)
}

/**
 * Drop a field by name.
 */
case class DropField(name: String) extends StructFieldsOperation with LeafLike[Expression] {
  override def apply(values: Seq[(StructField, Expression)]): Seq[(StructField, Expression)] =
    values.filterNot { case (field, _) => resolver(field.name, name) }
}

/**
 * Updates fields in a struct.
 */
case class UpdateFields(structExpr: Expression, fieldOps: Seq[StructFieldsOperation])
  extends Unevaluable {

  final override val nodePatterns: Seq[TreePattern] = Seq(UPDATE_FIELDS)

  override def checkInputDataTypes(): TypeCheckResult = {
    val dataType = structExpr.dataType
    if (!dataType.isInstanceOf[StructType]) {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> toSQLType(StructType),
          "inputSql" -> toSQLExpr(structExpr),
          "inputType" -> toSQLType(structExpr.dataType))
      )
    } else if (newExprs.isEmpty) {
      DataTypeMismatch(
        errorSubClass = "CANNOT_DROP_ALL_FIELDS",
        messageParameters = Map.empty
      )
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def children: Seq[Expression] = structExpr +: fieldOps.collect {
    case e: Expression => e
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)

  override def dataType: StructType = StructType(newFields)

  override def nullable: Boolean = structExpr.nullable

  override def prettyName: String = "update_fields"

  private lazy val newFieldExprs: Seq[(StructField, Expression)] = {
    def getFieldExpr(i: Int): Expression = structExpr match {
      case c: CreateNamedStruct => c.valExprs(i)
      case _ => GetStructField(structExpr, i)
    }
    val fieldsWithIndex = structExpr.dataType.asInstanceOf[StructType].fields.zipWithIndex
    val existingFieldExprs: Seq[(StructField, Expression)] =
      fieldsWithIndex.map { case (field, i) => (field, getFieldExpr(i)) }.toImmutableArraySeq
    fieldOps.foldLeft(existingFieldExprs)((exprs, op) => op(exprs))
  }

  private lazy val newFields: Seq[StructField] = newFieldExprs.map(_._1)

  lazy val newExprs: Seq[Expression] = newFieldExprs.map(_._2)

  lazy val evalExpr: Expression = {
    val createNamedStructExpr = CreateNamedStruct(newFieldExprs.flatMap {
      case (field, expr) => Seq(Literal(field.name), expr)
    })

    if (structExpr.nullable) {
      If(IsNull(structExpr), Literal(null, dataType), createNamedStructExpr)
    } else {
      createNamedStructExpr
    }
  }
}

object UpdateFields {
  private def nameParts(fieldName: String): Seq[String] = {
    require(fieldName != null, "fieldName cannot be null")

    if (fieldName.isEmpty) {
      fieldName :: Nil
    } else {
      CatalystSqlParser.parseMultipartIdentifier(fieldName)
    }
  }

  /**
   * Adds/replaces field of `StructType` into `col` expression by name.
   */
  def apply(col: Expression, fieldName: String, expr: Expression): UpdateFields = {
    updateFieldsHelper(col, nameParts(fieldName), name => WithField(name, expr))
  }

  /**
   * Drops fields of `StructType` in `col` expression by name.
   */
  def apply(col: Expression, fieldName: String): UpdateFields = {
    updateFieldsHelper(col, nameParts(fieldName), name => DropField(name))
  }

  private def updateFieldsHelper(
      structExpr: Expression,
      namePartsRemaining: Seq[String],
      valueFunc: String => StructFieldsOperation) : UpdateFields = {
    val fieldName = namePartsRemaining.head
    if (namePartsRemaining.length == 1) {
      UpdateFields(structExpr, valueFunc(fieldName) :: Nil)
    } else {
      val newStruct = if (structExpr.resolved) {
        val resolver = SQLConf.get.resolver
        ExtractValue(structExpr, Literal(fieldName), resolver)
      } else {
        UnresolvedExtractValue(structExpr, Literal(fieldName))
      }

      val newValue = updateFieldsHelper(
        structExpr = newStruct,
        namePartsRemaining = namePartsRemaining.tail,
        valueFunc = valueFunc)
      UpdateFields(structExpr, WithField(fieldName, newValue) :: Nil)
    }
  }
}
