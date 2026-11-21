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

import java.lang.{Boolean => JBoolean, Long => JLong}

import org.apache.spark.{SparkArithmeticException, SparkRuntimeException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{MathUtils, QuotingUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, LongType}

/**
 * A Python UDF lowered to the body of a Java method (see `pyspark.sql.transpile_java`).
 *
 * The method takes one boxed parameter per child, named `_udf_arg_0`, `_udf_arg_1`, ..., and
 * returns a boxed value; Java `null` is both SQL NULL and Python `None`. Only `Long` and
 * `Boolean` are supported, for both arguments and the result.
 *
 * `body` is source rather than bytecode because a class compiled on the driver cannot be shipped
 * to executors. [[doGenCode]] splices it into the generated class; [[eval]] compiles the same
 * string on its own, so the two paths cannot disagree.
 *
 * @param name the UDF's name, for display and to seed the generated method's name
 * @param body Java statements ending in a `return` on every path
 * @param children the arguments, each already `LongType` or `BooleanType`
 * @param dataType the result type, `LongType` or `BooleanType`
 */
case class TranspiledJavaUDF(
    name: String,
    body: String,
    children: Seq[Expression],
    dataType: DataType)
  extends Expression with NonSQLExpression with UserDefinedExpression {

  // Like ScalaUDF: keeps rules from duplicating the call (and any `rand()` child with it).
  override def expensive: Boolean = true

  override def nullable: Boolean = true

  override def prettyName: String = name

  // The default would print `body` into EXPLAIN.
  override def toString: String = s"TranspiledJavaUDF($name, ${children.mkString(", ")})"

  override def checkInputDataTypes(): TypeCheckResult = {
    (children.map(_.dataType) :+ dataType).find(dt => dt != LongType && dt != BooleanType) match {
      case Some(dt) => TypeCheckResult.TypeCheckFailure(s"unsupported type ${dt.simpleString}")
      case None => TypeCheckResult.TypeCheckSuccess
    }
  }

  private def boxed(dt: DataType): String = CodeGenerator.boxedType(dt)

  private def methodSource(methodName: String): String = {
    val params = children.zipWithIndex.map { case (c, i) => s"${boxed(c.dataType)} _udf_arg_$i" }
    s"""
       |private ${boxed(dataType)} $methodName(${params.mkString(", ")}) {
       |$body
       |}
     """.stripMargin
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // A user-chosen name is not necessarily a Java identifier.
    val methodName = ctx.freshName("udf_" + name.map(c => if (c.isLetterOrDigit) c else '_'))
    val callable = ctx.addNewFunction(methodName, methodSource(methodName))
    val args = children.map { child =>
      val eval = child.genCode(ctx)
      val term = ctx.freshName("arg")
      (term, code"""
        |${eval.code}
        |${boxed(child.dataType)} $term = null;
        |if (!${eval.isNull}) {
        |  $term = ${eval.value};
        |}
        """.stripMargin)
    }
    val result = ctx.freshName("result")
    ev.copy(code = code"""
      |${args.map(_._2).foldLeft[Block](EmptyBlock)(_ + _)}
      |${boxed(dataType)} $result = $callable(${args.map(_._1).mkString(", ")});
      |boolean ${ev.isNull} = $result == null;
      |${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      |if (!${ev.isNull}) {
      |  ${ev.value} = $result;
      |}
      """.stripMargin)
  }

  // The same body compiled for the interpreted path; `CodeGenerator.compile` caches on source.
  @transient private lazy val invoker: TranspiledJavaUDFInvoker = {
    val args = children.indices.map(i => s"(${boxed(children(i).dataType)}) args[$i]")
    val source =
      s"""
         |public Object generate(Object[] references) {
         |  return new SpecificTranspiledJavaUDF();
         |}
         |
         |class SpecificTranspiledJavaUDF extends ${classOf[TranspiledJavaUDFInvoker].getName} {
         |  public Object call(Object[] args) {
         |    return udf(${args.mkString(", ")});
         |  }
         |${methodSource("udf")}
         |}
       """.stripMargin
    val code = CodeFormatter.stripOverlappingComments(new CodeAndComment(source, Map.empty))
    CodeGenerator.compile(code)._1.generate(Array.empty).asInstanceOf[TranspiledJavaUDFInvoker]
  }

  override def eval(input: InternalRow): Any = invoker.call(children.map(_.eval(input)).toArray)

  /** Compiles the body now, so a lowering bug skips this option instead of failing the query. */
  def validate(): Unit = invoker

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): TranspiledJavaUDF = copy(children = newChildren)
}

/** What [[TranspiledJavaUDF.eval]] calls; a compiled class can only extend a type it can see. */
abstract class TranspiledJavaUDFInvoker {
  def call(args: Array[Any]): Any
}

/**
 * The operators generated bodies call. They hold the differences between Python and Java:
 *
 *  - an operand of `None` raises, as Python's TypeError would, except in `==` / `!=`, where
 *    `None == None` is True and `None == 1` is False;
 *  - `//` and `%` round towards negative infinity, not towards zero;
 *  - overflow and division by zero raise with Spark's error classes, as they do in the Catalyst
 *    target under ANSI (Python ints are unbounded, so overflow is a documented divergence).
 */
object TranspiledJavaUDFHelpers {

  def add(a: JLong, b: JLong): JLong = MathUtils.addExact(operand(a, "+"), operand(b, "+"))

  def subtract(a: JLong, b: JLong): JLong =
    MathUtils.subtractExact(operand(a, "-"), operand(b, "-"))

  def multiply(a: JLong, b: JLong): JLong =
    MathUtils.multiplyExact(operand(a, "*"), operand(b, "*"))

  def negate(a: JLong): JLong = MathUtils.negateExact(operand(a, "unary -"))

  def floorDivide(a: JLong, b: JLong): JLong = {
    val (x, y) = (operand(a, "//"), operand(b, "//"))
    if (y == 0L) throw zeroDivisor("DIVIDE_BY_ZERO")
    // `Math.floorDiv(Long.MinValue, -1)` wraps instead of throwing.
    if (y == -1L) MathUtils.negateExact(x) else MathUtils.floorDiv(x, y)
  }

  def mod(a: JLong, b: JLong): JLong = {
    val (x, y) = (operand(a, "%"), operand(b, "%"))
    if (y == 0L) throw zeroDivisor("REMAINDER_BY_ZERO")
    MathUtils.floorMod(x, y)
  }

  def equalsLong(a: JLong, b: JLong): JBoolean = java.util.Objects.equals(a, b)

  def equalsBoolean(a: JBoolean, b: JBoolean): JBoolean = java.util.Objects.equals(a, b)

  def lessThan(a: JLong, b: JLong): JBoolean = operand(a, "<") < operand(b, "<")

  def lessThanOrEqual(a: JLong, b: JLong): JBoolean = operand(a, "<=") <= operand(b, "<=")

  def greaterThan(a: JLong, b: JLong): JBoolean = operand(a, ">") > operand(b, ">")

  def greaterThanOrEqual(a: JLong, b: JLong): JBoolean = operand(a, ">=") >= operand(b, ">=")

  /** Python's `not`: `not None` is True, unlike SQL's `NOT NULL`. */
  def not(a: JBoolean): JBoolean = a == null || !a

  /** Whether an `if` takes its branch: Python treats `None` as false. */
  def isTrue(a: JBoolean): Boolean = a != null && a

  private def operand(a: JLong, op: String): Long = {
    if (a == null) {
      throw new SparkRuntimeException(
        errorClass = "USER_RAISED_EXCEPTION",
        messageParameters = Map("errorMessage" ->
          (s"Python UDF transpiler: cannot use NULL with operator `$op`; " +
            "Python would raise TypeError here.")))
    }
    a
  }

  // Built directly: `QueryExecutionErrors.divideByZeroError` wants a query context, and there is
  // none inside a generated body.
  private def zeroDivisor(errorClass: String): ArithmeticException = {
    new SparkArithmeticException(
      errorClass = errorClass,
      messageParameters = Map("config" -> QuotingUtils.toSQLConf(SQLConf.ANSI_ENABLED.key)),
      context = Array.empty,
      summary = "")
  }
}
