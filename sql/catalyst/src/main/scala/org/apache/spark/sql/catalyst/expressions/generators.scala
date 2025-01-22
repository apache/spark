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

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, GeneratorBuilder, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter}
import org.apache.spark.sql.catalyst.trees.TreePattern.{GENERATOR, TreePattern}
import org.apache.spark.sql.catalyst.util.{ArrayData, CollationFactory, MapData}
import org.apache.spark.sql.catalyst.util.SQLKeywordUtils._
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * An expression that produces zero or more rows given a single input row.
 *
 * Generators produce multiple output rows instead of a single value like other expressions,
 * and thus they must have a schema to associate with the rows that are output.
 *
 * However, unlike row producing relational operators, which are either leaves or determine their
 * output schema functionally from their input, generators can contain other expressions that
 * might result in their modification by rules.  This structure means that they might be copied
 * multiple times after first determining their output schema. If a new output schema is created for
 * each copy references up the tree might be rendered invalid. As a result generators must
 * instead define a function `makeOutput` which is called only once when the schema is first
 * requested.  The attributes produced by this function will be automatically copied anytime rules
 * result in changes to the Generator or its children.
 */
trait Generator extends Expression {

  override def dataType: DataType = ArrayType(elementSchema)

  override def foldable: Boolean = false

  override def nullable: Boolean = false

  protected override val nodePatterns: Seq[TreePattern] = Seq(GENERATOR)

  /**
   * The output element schema.
   */
  def elementSchema: StructType

  /** Should be implemented by child classes to perform specific Generators. */
  override def eval(input: InternalRow): IterableOnce[InternalRow]

  /**
   * Notifies that there are no more rows to process, clean up code, and additional
   * rows can be made here.
   */
  def terminate(): IterableOnce[InternalRow] = Nil

  /**
   * Check if this generator supports code generation.
   */
  def supportCodegen: Boolean = !isInstanceOf[CodegenFallback]
}

/**
 * A collection producing [[Generator]]. This trait provides a different path for code generation,
 * by allowing code generation to return either an [[ArrayData]] or a [[MapData]] object.
 */
trait CollectionGenerator extends Generator {
  /** The position of an element within the collection should also be returned. */
  def position: Boolean

  /** Rows will be inlined during generation. */
  def inline: Boolean

  /** The type of the returned collection object. */
  def collectionType: DataType = dataType
}

/**
 * A generator that produces its output using the provided lambda function.
 */
case class UserDefinedGenerator(
    elementSchema: StructType,
    function: Row => IterableOnce[InternalRow],
    children: Seq[Expression])
  extends Generator with CodegenFallback {

  @transient private[this] var inputRow: InterpretedProjection = _
  @transient private[this] var convertToScala: (InternalRow) => Row = _

  private def initializeConverters(): Unit = {
    inputRow = new InterpretedProjection(children)
    convertToScala = {
      val inputSchema = StructType(children.map { e =>
        StructField(e.simpleString(SQLConf.get.maxToStringFields), e.dataType, nullable = true)
      })
      CatalystTypeConverters.createToScalaConverter(inputSchema)
    }.asInstanceOf[InternalRow => Row]
  }

  override def eval(input: InternalRow): IterableOnce[InternalRow] = {
    if (inputRow == null) {
      initializeConverters()
    }
    // Convert the objects into Scala Type before calling function, we need schema to support UDT
    function(convertToScala(inputRow(input)))
  }

  override def toString: String = s"UserDefinedGenerator(${children.mkString(",")})"

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): UserDefinedGenerator = copy(children = newChildren)
}

/**
 * Separate v1, ..., vk into n rows. Each row will have k/n columns. n must be constant.
 * {{{
 *   SELECT stack(2, 1, 2, 3) ->
 *   1      2
 *   3      NULL
 * }}}
 */
// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = "_FUNC_(n, expr1, ..., exprk) - Separates `expr1`, ..., `exprk` into `n` rows. Uses column names col0, col1, etc. by default unless specified otherwise.",
  examples = """
    Examples:
      > SELECT _FUNC_(2, 1, 2, 3);
       1	2
       3	NULL
  """,
  since = "2.0.0",
  group = "generator_funcs")
// scalastyle:on line.size.limit line.contains.tab
case class Stack(children: Seq[Expression]) extends Generator {

  private lazy val numRows = children.head.eval().asInstanceOf[Int]
  private lazy val numFields = Math.ceil((children.length - 1.0) / numRows).toInt

  /**
   * Return true iff the first child exists and has a foldable IntegerType.
   */
  def hasFoldableNumRows: Boolean = {
    children.nonEmpty && children.head.dataType == IntegerType && children.head.foldable
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      throw QueryCompilationErrors.wrongNumArgsError(
        toSQLId(prettyName), Seq("> 1"), children.length
      )
    } else if (children.head.dataType != IntegerType) {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> toSQLType(IntegerType),
          "inputSql" -> toSQLExpr(children.head),
          "inputType" -> toSQLType(children.head.dataType))
      )
    } else if (!children.head.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("n"),
          "inputType" -> toSQLType(IntegerType),
          "inputExpr" -> toSQLExpr(children.head)
        )
      )
    } else if (numRows < 1) {
      DataTypeMismatch(
        errorSubClass = "VALUE_OUT_OF_RANGE",
        messageParameters = Map(
          "exprName" -> toSQLId("n"),
          "valueRange" -> s"(0, ${Int.MaxValue}]",
          "currentValue" -> toSQLValue(numRows, children.head.dataType)
        )
      )
    } else {
      for (i <- 1 until children.length) {
        val j = (i - 1) % numFields
        if (children(i).dataType != elementSchema.fields(j).dataType) {
          return DataTypeMismatch(
            errorSubClass = "STACK_COLUMN_DIFF_TYPES",
            messageParameters = Map(
              "columnIndex" -> j.toString,
              "leftParamIndex" -> (j + 1).toString,
              "leftType" -> toSQLType(elementSchema.fields(j).dataType),
              "rightParamIndex" -> i.toString,
              "rightType" -> toSQLType(children(i).dataType)
            )
          )
        }
      }
      TypeCheckResult.TypeCheckSuccess
    }
  }

  def findDataType(index: Int): DataType = {
    // Find the first data type except NullType.
    val firstDataIndex = ((index - 1) % numFields) + 1
    for (i <- firstDataIndex until children.length by numFields) {
      if (children(i).dataType != NullType) {
        return children(i).dataType
      }
    }
    // If all values of the column are NullType, use it.
    NullType
  }

  override def elementSchema: StructType =
    StructType(children.tail.take(numFields).zipWithIndex.map {
      case (e, index) => StructField(s"col$index", e.dataType)
    })

  override def eval(input: InternalRow): IterableOnce[InternalRow] = {
    val values = children.tail.map(_.eval(input)).toArray

    import org.apache.spark.util.ArrayImplicits._
    for (row <- 0 until numRows) yield {
      val fields = new Array[Any](numFields)
      for (col <- 0 until numFields) {
        val index = row * numFields + col
        fields.update(col, if (index < values.length) values(index) else null)
      }
      InternalRow(fields.toImmutableArraySeq: _*)
    }
  }

  /**
   * Only support code generation when stack produces 50 rows or less.
   */
  override def supportCodegen: Boolean = numRows <= 50

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Rows - we write these into an array.
    val rowData = ctx.addMutableState("InternalRow[]", "rows",
      v => s"$v = new InternalRow[$numRows];")
    val values = children.tail
    val dataTypes = values.take(numFields).map(_.dataType)
    val code = ctx.splitExpressionsWithCurrentInputs(Seq.tabulate(numRows) { row =>
      val fields = Seq.tabulate(numFields) { col =>
        val index = row * numFields + col
        if (index < values.length) values(index) else Literal(null, dataTypes(col))
      }
      val eval = CreateStruct(fields).genCode(ctx)
      s"${eval.code}\n$rowData[$row] = ${eval.value};"
    })

    // Create the collection.
    val wrapperClass = classOf[mutable.ArraySeq[_]].getName
    ev.copy(code =
      code"""
         |$code
         |$wrapperClass<InternalRow> ${ev.value} = $wrapperClass$$.MODULE$$.make($rowData);
       """.stripMargin, isNull = FalseLiteral)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Stack =
    copy(children = newChildren)
}

/**
 * Replicate the row N times. N is specified as the first argument to the function.
 * This is an internal function solely used by optimizer to rewrite EXCEPT ALL AND
 * INTERSECT ALL queries.
 */
case class ReplicateRows(children: Seq[Expression]) extends Generator with CodegenFallback {
  private lazy val numColumns = children.length - 1 // remove the multiplier value from output.

  override def elementSchema: StructType =
    StructType(children.tail.zipWithIndex.map {
      case (e, index) => StructField(s"col$index", e.dataType)
    })

  override def eval(input: InternalRow): IterableOnce[InternalRow] = {
    val numRows = children.head.eval(input).asInstanceOf[Long]
    val values = children.tail.map(_.eval(input)).toArray

    import org.apache.spark.util.ArrayImplicits._
    Range.Long(0, numRows, 1).map { _ =>
      val fields = new Array[Any](numColumns)
      for (col <- 0 until numColumns) {
        fields.update(col, values(col))
      }
      InternalRow(fields.toImmutableArraySeq: _*)
    }
  }

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): ReplicateRows = copy(children = newChildren)
}

/**
 * Wrapper around another generator to specify outer behavior. This is used to implement functions
 * such as explode_outer. This expression gets replaced during analysis.
 */
case class GeneratorOuter(child: Generator) extends UnaryExpression with Generator {
  final override def eval(input: InternalRow = null): IterableOnce[InternalRow] =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)

  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)

  override def elementSchema: StructType = child.elementSchema

  override lazy val resolved: Boolean = false

  override protected def withNewChildInternal(newChild: Expression): GeneratorOuter =
    copy(child = newChild.asInstanceOf[Generator])
}

/**
 * A base class for [[Explode]] and [[PosExplode]].
 */
abstract class ExplodeBase extends UnaryExpression with CollectionGenerator with Serializable {
  override val inline: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case _: ArrayType | _: MapType =>
      TypeCheckResult.TypeCheckSuccess
    case _ =>
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> toSQLType(TypeCollection(ArrayType, MapType)),
          "inputSql" -> toSQLExpr(child),
          "inputType" -> toSQLType(child.dataType))
      )
  }

  // hive-compatible default alias for explode function ("col" for array, "key", "value" for map)
  override def elementSchema: StructType = child.dataType match {
    case ArrayType(et, containsNull) =>
      if (position) {
        new StructType()
          .add("pos", IntegerType, nullable = false)
          .add("col", et, containsNull)
      } else {
        new StructType()
          .add("col", et, containsNull)
      }
    case MapType(kt, vt, valueContainsNull) =>
      if (position) {
        new StructType()
          .add("pos", IntegerType, nullable = false)
          .add("key", kt, nullable = false)
          .add("value", vt, valueContainsNull)
      } else {
        new StructType()
          .add("key", kt, nullable = false)
          .add("value", vt, valueContainsNull)
      }
  }

  override def eval(input: InternalRow): IterableOnce[InternalRow] = {
    child.dataType match {
      case ArrayType(et, _) =>
        val inputArray = child.eval(input).asInstanceOf[ArrayData]
        if (inputArray == null) {
          Nil
        } else {
          val rows = new Array[InternalRow](inputArray.numElements())
          inputArray.foreach(et, (i, e) => {
            rows(i) = if (position) InternalRow(i, e) else InternalRow(e)
          })
          rows
        }
      case MapType(kt, vt, _) =>
        val inputMap = child.eval(input).asInstanceOf[MapData]
        if (inputMap == null) {
          Nil
        } else {
          val rows = new Array[InternalRow](inputMap.numElements())
          var i = 0
          inputMap.foreach(kt, vt, (k, v) => {
            rows(i) = if (position) InternalRow(i, k, v) else InternalRow(k, v)
            i += 1
          })
          rows
        }
    }
  }

  override def collectionType: DataType = child.dataType

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.genCode(ctx)
  }
}

/**
 * Given an input array produces a sequence of rows for each value in the array.
 *
 * {{{
 *   SELECT explode(array(10,20)) ->
 *   10
 *   20
 * }}}
 */
case class Explode(child: Expression) extends ExplodeBase {
  override val position: Boolean = false
  override protected def withNewChildInternal(newChild: Expression): Explode =
    copy(child = newChild)
}

trait ExplodeGeneratorBuilderBase extends GeneratorBuilder {
  override def functionSignature: Option[FunctionSignature] =
    Some(FunctionSignature(Seq(InputParameter("collection"))))
  override def buildGenerator(funcName: String, expressions: Seq[Expression]): Generator = {
    assert(expressions.size == 1)
    Explode(expressions(0))
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr) - Separates the elements of array `expr` into multiple rows, or the elements of map `expr` into multiple rows and columns. Unless specified otherwise, uses the default column name `col` for elements of the array or `key` and `value` for the elements of the map.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(10, 20));
       10
       20
      > SELECT _FUNC_(collection => array(10, 20));
       10
       20
  """,
  since = "1.0.0",
  group = "generator_funcs")
// scalastyle:on line.size.limit
object ExplodeExpressionBuilder extends ExpressionBuilder {
  override def functionSignature: Option[FunctionSignature] =
    Some(FunctionSignature(Seq(InputParameter("collection"))))

  override def build(funcName: String, expressions: Seq[Expression]) : Expression =
    Explode(expressions(0))
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr) - Separates the elements of array `expr` into multiple rows, or the elements of map `expr` into multiple rows and columns. Unless specified otherwise, uses the default column name `col` for elements of the array or `key` and `value` for the elements of the map.",
  examples = """
    Examples:
      > SELECT * FROM _FUNC_(array(10, 20));
       10
       20
      > SELECT * FROM _FUNC_(collection => array(10, 20));
       10
       20
  """,
  since = "3.4.0",
  group = "generator_funcs")
// scalastyle:on line.size.limit
object ExplodeGeneratorBuilder extends ExplodeGeneratorBuilderBase {
  override def isOuter: Boolean = false
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr) - Separates the elements of array `expr` into multiple rows, or the elements of map `expr` into multiple rows and columns. Unless specified otherwise, uses the default column name `col` for elements of the array or `key` and `value` for the elements of the map.",
  examples = """
    Examples:
      > SELECT * FROM _FUNC_(array(10, 20));
       10
       20
      > SELECT * FROM _FUNC_(collection => array(10, 20));
       10
       20
  """,
  since = "3.4.0",
  group = "generator_funcs")
// scalastyle:on line.size.limit
object ExplodeOuterGeneratorBuilder extends ExplodeGeneratorBuilderBase {
  override def isOuter: Boolean = true
}

/**
 * Given an input array produces a sequence of rows for each position and value in the array.
 *
 * {{{
 *   SELECT posexplode(array(10,20)) ->
 *   0  10
 *   1  20
 * }}}
 */
case class PosExplode(child: Expression) extends ExplodeBase {
  override val position = true
  override protected def withNewChildInternal(newChild: Expression): PosExplode =
    copy(child = newChild)
}

trait PosExplodeGeneratorBuilderBase extends GeneratorBuilder {
  override def functionSignature: Option[FunctionSignature] =
    Some(FunctionSignature(Seq(InputParameter("collection"))))
  override def buildGenerator(funcName: String, expressions: Seq[Expression]): Generator = {
    assert(expressions.size == 1)
    PosExplode(expressions(0))
  }
}

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = "_FUNC_(expr) - Separates the elements of array `expr` into multiple rows with positions, or the elements of map `expr` into multiple rows and columns with positions. Unless specified otherwise, uses the column name `pos` for position, `col` for elements of the array or `key` and `value` for elements of the map.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(10,20));
       0	10
       1	20
      > SELECT _FUNC_(collection => array(10,20));
       0	10
       1	20
  """,
  since = "2.0.0",
  group = "generator_funcs")
// scalastyle:on line.size.limit line.contains.tab
object PosExplodeExpressionBuilder extends ExpressionBuilder {
  override def functionSignature: Option[FunctionSignature] =
    Some(FunctionSignature(Seq(InputParameter("collection"))))

  override def build(funcName: String, expressions: Seq[Expression]) : Expression =
    PosExplode(expressions(0))
}

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = "_FUNC_(expr) - Separates the elements of array `expr` into multiple rows with positions, or the elements of map `expr` into multiple rows and columns with positions. Unless specified otherwise, uses the column name `pos` for position, `col` for elements of the array or `key` and `value` for elements of the map.",
  examples = """
    Examples:
      > SELECT * FROM _FUNC_(array(10,20));
       0	10
       1	20
      > SELECT * FROM _FUNC_(collection => array(10,20));
       0	10
       1	20
  """,
  since = "3.5.0",
  group = "generator_funcs")
// scalastyle:on line.size.limit line.contains.tab
object PosExplodeGeneratorBuilder extends PosExplodeGeneratorBuilderBase {
  override def isOuter: Boolean = false
}

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = "_FUNC_(expr) - Separates the elements of array `expr` into multiple rows with positions, or the elements of map `expr` into multiple rows and columns with positions. Unless specified otherwise, uses the column name `pos` for position, `col` for elements of the array or `key` and `value` for elements of the map.",
  examples = """
    Examples:
      > SELECT * FROM _FUNC_(array(10,20));
       0	10
       1	20
      > SELECT * FROM _FUNC_(collection => array(10,20));
       0	10
       1	20
  """,
  since = "3.5.0",
  group = "generator_funcs")
// scalastyle:on line.size.limit line.contains.tab
object PosExplodeOuterGeneratorBuilder extends PosExplodeGeneratorBuilderBase {
  override def isOuter: Boolean = true
}

/**
 * Explodes an array of structs into a table.
 */
case class Inline(child: Expression) extends UnaryExpression with CollectionGenerator {
  override val inline: Boolean = true
  override val position: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case ArrayType(st: StructType, _) =>
      TypeCheckResult.TypeCheckSuccess
    case _ =>
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> toSQLType("ARRAY<STRUCT>"),
          "inputSql" -> toSQLExpr(child),
          "inputType" -> toSQLType(child.dataType))
      )
  }

  override def elementSchema: StructType = child.dataType match {
    case ArrayType(st: StructType, false) => st
    case ArrayType(st: StructType, true) => st.asNullable
  }

  override def collectionType: DataType = child.dataType

  private lazy val numFields = elementSchema.fields.length

  private lazy val generatorNullRow = new GenericInternalRow(elementSchema.length)

  override def eval(input: InternalRow): IterableOnce[InternalRow] = {
    val inputArray = child.eval(input).asInstanceOf[ArrayData]
    if (inputArray == null) {
      Nil
    } else {
      for (i <- 0 until inputArray.numElements()) yield {
        val s = inputArray.getStruct(i, numFields)
        if (s == null) generatorNullRow else s
      }
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.genCode(ctx)
  }

  override protected def withNewChildInternal(newChild: Expression): Inline = copy(child = newChild)
}

trait InlineGeneratorBuilderBase extends GeneratorBuilder {
  override def functionSignature: Option[FunctionSignature] =
    Some(FunctionSignature(Seq(InputParameter("input"))))
  override def buildGenerator(funcName: String, expressions: Seq[Expression]): Generator = {
    assert(expressions.size == 1)
    Inline(expressions(0))
  }
}

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = "_FUNC_(expr) - Explodes an array of structs into a table. Uses column names col1, col2, etc. by default unless specified otherwise.",
  examples = """
    Examples:
      > SELECT _FUNC_(array(struct(1, 'a'), struct(2, 'b')));
       1	a
       2	b
      > SELECT _FUNC_(input => array(struct(1, 'a'), struct(2, 'b')));
       1	a
       2	b
  """,
  since = "2.0.0",
  group = "generator_funcs")
// scalastyle:on line.size.limit line.contains.tab
object InlineExpressionBuilder extends ExpressionBuilder {
  override def functionSignature: Option[FunctionSignature] =
    Some(FunctionSignature(Seq(InputParameter("input"))))

  override def build(funcName: String, expressions: Seq[Expression]) : Expression =
    Inline(expressions(0))
}

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = "_FUNC_(expr) - Explodes an array of structs into a table. Uses column names col1, col2, etc. by default unless specified otherwise.",
  examples = """
    Examples:
      > SELECT * FROM _FUNC_(array(struct(1, 'a'), struct(2, 'b')));
       1	a
       2	b
      > SELECT * FROM _FUNC_(input => array(struct(1, 'a'), struct(2, 'b')));
       1	a
       2	b
  """,
  since = "3.4.0",
  group = "generator_funcs")
// scalastyle:on line.size.limit line.contains.tab
object InlineGeneratorBuilder extends InlineGeneratorBuilderBase {
  override def isOuter: Boolean = false
}

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = "_FUNC_(expr) - Explodes an array of structs into a table. Uses column names col1, col2, etc. by default unless specified otherwise.",
  examples = """
    Examples:
      > SELECT * FROM _FUNC_(array(struct(1, 'a'), struct(2, 'b')));
       1	a
       2	b
      > SELECT * FROM _FUNC_(input => array(struct(1, 'a'), struct(2, 'b')));
       1	a
       2	b
  """,
  since = "3.4.0",
  group = "generator_funcs")
// scalastyle:on line.size.limit line.contains.tab
object InlineOuterGeneratorBuilder extends InlineGeneratorBuilderBase {
  override def isOuter: Boolean = true
}

@ExpressionDescription(
  usage = """_FUNC_() - Get Spark SQL keywords""",
  examples = """
    Examples:
      > SELECT * FROM _FUNC_() LIMIT 2;
       ADD  false
       AFTER  false
  """,
  since = "3.5.0",
  group = "generator_funcs")
case class SQLKeywords() extends LeafExpression with Generator with CodegenFallback {
  override def elementSchema: StructType = new StructType()
    .add("keyword", StringType, nullable = false)
    .add("reserved", BooleanType, nullable = false)

  override def eval(input: InternalRow): IterableOnce[InternalRow] = {
    val reservedList = getReservedList()
    keywords.zip(reservedList).map { case (keyword, isReserved) =>
      InternalRow(UTF8String.fromString(keyword), isReserved)
    }
  }

  override def prettyName: String = "sql_keywords"
}

@ExpressionDescription(
  usage = """_FUNC_() - Get all of the Spark SQL string collations""",
  examples = """
    Examples:
      > SELECT * FROM _FUNC_() WHERE NAME = 'UTF8_BINARY';
       SYSTEM  BUILTIN  UTF8_BINARY NULL  NULL  ACCENT_SENSITIVE  CASE_SENSITIVE  NO_PAD  NULL
  """,
  since = "4.0.0",
  group = "generator_funcs")
case class Collations() extends LeafExpression with Generator with CodegenFallback {
  override def elementSchema: StructType = new StructType()
    .add("CATALOG", StringType, nullable = false)
    .add("SCHEMA", StringType, nullable = false)
    .add("NAME", StringType, nullable = false)
    .add("LANGUAGE", StringType)
    .add("COUNTRY", StringType)
    .add("ACCENT_SENSITIVITY", StringType, nullable = false)
    .add("CASE_SENSITIVITY", StringType, nullable = false)
    .add("PAD_ATTRIBUTE", StringType, nullable = false)
    .add("ICU_VERSION", StringType)

  override def eval(input: InternalRow): IterableOnce[InternalRow] = {
    CollationFactory.listCollations().asScala.map(CollationFactory.loadCollationMeta).map { m =>
      InternalRow(
        UTF8String.fromString(m.catalog),
        UTF8String.fromString(m.schema),
        UTF8String.fromString(m.collationName),
        UTF8String.fromString(m.language),
        UTF8String.fromString(m.country),
        UTF8String.fromString(
          if (m.accentSensitivity) "ACCENT_SENSITIVE" else "ACCENT_INSENSITIVE"),
        UTF8String.fromString(
          if (m.caseSensitivity) "CASE_SENSITIVE" else "CASE_INSENSITIVE"),
        UTF8String.fromString(m.padAttribute),
        UTF8String.fromString(m.icuVersion))
    }
  }

  override def prettyName: String = "collations"
}
