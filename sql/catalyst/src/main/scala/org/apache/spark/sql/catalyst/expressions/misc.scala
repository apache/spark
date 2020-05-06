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

import java.io.UnsupportedEncodingException
import java.time.DateTimeException

import org.apache.spark.{SPARK_REVISION, SPARK_VERSION_SHORT}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.RandomUUIDGenerator
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Print the result of an expression to stderr (used for debugging codegen).
 */
case class PrintToStderr(child: Expression) extends UnaryExpression {

  override def dataType: DataType = child.dataType

  protected override def nullSafeEval(input: Any): Any = {
    // scalastyle:off println
    System.err.println(outputPrefix + input)
    // scalastyle:on println
    input
  }

  private val outputPrefix = s"Result of ${child.simpleString(SQLConf.get.maxToStringFields)} is "

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val outputPrefixField = ctx.addReferenceObj("outputPrefix", outputPrefix)
    nullSafeCodeGen(ctx, ev, c =>
      s"""
         | System.err.println($outputPrefixField + $c);
         | ${ev.value} = $c;
       """.stripMargin)
  }
}

/**
 * A function throws an exception if 'condition' is not true.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Throws an exception if `expr` is not true.",
  examples = """
    Examples:
      > SELECT _FUNC_(0 < 1);
       NULL
  """)
case class AssertTrue(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[DataType] = Seq(BooleanType)

  override def dataType: DataType = NullType

  override def prettyName: String = "assert_true"

  private val errMsg = s"'${child.simpleString(SQLConf.get.maxToStringFields)}' is not true!"

  override def eval(input: InternalRow) : Any = {
    val v = child.eval(input)
    if (v == null || java.lang.Boolean.FALSE.equals(v)) {
      throw new RuntimeException(errMsg)
    } else {
      null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)

    // Use unnamed reference that doesn't create a local field here to reduce the number of fields
    // because errMsgField is used only when the value is null or false.
    val errMsgField = ctx.addReferenceObj("errMsg", errMsg)
    ExprCode(code = code"""${eval.code}
       |if (${eval.isNull} || !${eval.value}) {
       |  throw new RuntimeException($errMsgField);
       |}""".stripMargin, isNull = TrueLiteral,
      value = JavaCode.defaultLiteral(dataType))
  }

  override def sql: String = s"assert_true(${child.sql})"
}

/**
 * Returns the current database of the SessionCatalog.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current database.",
  examples = """
    Examples:
      > SELECT _FUNC_();
       default
  """)
case class CurrentDatabase() extends LeafExpression with Unevaluable {
  override def dataType: DataType = StringType
  override def foldable: Boolean = true
  override def nullable: Boolean = false
  override def prettyName: String = "current_database"
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_() - Returns an universally unique identifier (UUID) string. The value is returned as a canonical UUID 36-character string.""",
  examples = """
    Examples:
      > SELECT _FUNC_();
       46707d92-02f4-4817-8116-a4c3b23e6266
  """,
  note = """
    The function is non-deterministic.
  """)
// scalastyle:on line.size.limit
case class Uuid(randomSeed: Option[Long] = None) extends LeafExpression with Stateful
    with ExpressionWithRandomSeed {

  def this() = this(None)

  override def withNewSeed(seed: Long): Uuid = Uuid(Some(seed))

  override lazy val resolved: Boolean = randomSeed.isDefined

  override def nullable: Boolean = false

  override def dataType: DataType = StringType

  @transient private[this] var randomGenerator: RandomUUIDGenerator = _

  override protected def initializeInternal(partitionIndex: Int): Unit =
    randomGenerator = RandomUUIDGenerator(randomSeed.get + partitionIndex)

  override protected def evalInternal(input: InternalRow): Any =
    randomGenerator.getNextUUIDUTF8String()

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val randomGen = ctx.freshName("randomGen")
    ctx.addMutableState("org.apache.spark.sql.catalyst.util.RandomUUIDGenerator", randomGen,
      forceInline = true,
      useFreshName = false)
    ctx.addPartitionInitializationStatement(s"$randomGen = " +
      "new org.apache.spark.sql.catalyst.util.RandomUUIDGenerator(" +
      s"${randomSeed.get}L + partitionIndex);")
    ev.copy(code = code"final UTF8String ${ev.value} = $randomGen.getNextUUIDUTF8String();",
      isNull = FalseLiteral)
  }

  override def freshCopy(): Uuid = Uuid(randomSeed)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_() - Returns the Spark version. The string contains 2 fields, the first being a release version and the second being a git revision.""",
  since = "3.0.0")
// scalastyle:on line.size.limit
case class SparkVersion() extends LeafExpression with CodegenFallback {
  override def nullable: Boolean = false
  override def foldable: Boolean = true
  override def dataType: DataType = StringType
  override def prettyName: String = "version"
  override def eval(input: InternalRow): Any = {
    UTF8String.fromString(SPARK_VERSION_SHORT + " " + SPARK_REVISION)
  }
}

@ExpressionDescription(
  usage = """_FUNC_(expr) - Return DDL-formatted type string for the data type of the input.""",
  examples = """
      Examples:
      > SELECT _FUNC_(1);
       int
      > SELECT _FUNC_(array(1));
       array<int>
  """,
  since = "3.0.0")
case class TypeOf(child: Expression) extends UnaryExpression {
  override def nullable: Boolean = false
  override def foldable: Boolean = true
  override def dataType: DataType = StringType
  override def eval(input: InternalRow): Any = UTF8String.fromString(child.dataType.catalogString)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, _ => s"""UTF8String.fromString(${child.dataType.catalogString})""")
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Evaluate an expression and handle certain types of runtime exceptions by returning NULL.
    In cases where it is preferable that queries produce NULL instead of failing when corrupt or invalid data is encountered, the TRY function may be useful, especially when ANSI mode is on and the users need null-tolerant on certain columns or outputs.
    AnalysisExceptions will not be handled by this, typically runtime exceptions handled by _FUNC_ function are:

      * ArightmeticException - e.g. division by zero, numeric value out of range,
      * NumberFormatException - e.g. invalid casting,
      * IllegalArgumentException - e.g. invalid datetime pattern, missing format argument for string formatting,
      * DateTimeException - e.g. invalid datetime values
      * UnsupportedEncodingException - e.g. encode or decode string with invalid charset
  """,
  examples = """
      Examples:
      > SELECT _FUNC_(1 / 0);
       NULL
      > SELECT _FUNC_(date_format(timestamp '2019-10-06', 'yyyy-MM-dd uucc'));
       NULL
      > SELECT _FUNC_((5e36BD + 0.1) + 5e36BD);
       NULL
      > SELECT _FUNC_(regexp_extract('1a 2b 14m', '\\d+', 1));
       NULL
      > SELECT _FUNC_(encode('abc', 'utf-88'));
       NULL
  """,
  since = "3.1.0")
// scalastyle:on line.size.limit
case class TryExpression(child: Expression) extends UnaryExpression {

  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def prettyName: String = "try"

  override def eval(input: InternalRow): Any = {
    try {
      child.eval(input)
    } catch {
      case e if TryExpression.canSuppress(e) => null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val javaType = CodeGenerator.javaType(dataType)
    ev.copy(code =
      code"""
         |boolean ${ev.isNull} = false;
         |$javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |try {
         |  ${eval.code}
         |  ${ev.isNull} = ${eval.isNull};
         |  ${ev.value} = ${eval.value};
         |} catch (java.lang.Exception e) {
         |  if (org.apache.spark.sql.catalyst.expressions.TryExpression.canSuppress(e)) {
         |    ${ev.isNull} = true;
         |  } else {
         |    throw e;
         |  }
         |}
         |""".stripMargin)
  }
}

object TryExpression {

  /**
   * Certain runtime exceptions that can be suppressed by [[TryExpression]], those not listed here
   * are not handled by try function, e.g. exceptions that related access controls or critical ones
   * like [[InterruptedException]] etc, or superclass like [[RuntimeException]] that can suppress
   * all of its subclasses.
   */
  def canSuppress(e: Throwable): Boolean = e match {
    case _: IllegalArgumentException |
         _: ArithmeticException |
         _: DateTimeException |
         _: NumberFormatException |
         _: UnsupportedEncodingException => true
    case _ => false
  }
}
