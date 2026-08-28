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

import org.apache.spark.{SparkArithmeticException, SparkRuntimeException}
import org.apache.spark.SparkException.internalError
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodegenContext, CodeGenerator, EmptyBlock, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, DoubleType, LongType, StringType}

/** The handle [[TranspiledJavaUDF.eval]] uses; a compiled class can only implement a type
 * that was on the classpath when it was compiled. */
abstract class TranspiledJavaUDFInvoker {
  def call(args: Array[Any]): Any
}

/** Errors the generated body raises. Lives here because the constructors take Scala maps. */
object TranspiledJavaUDFErrors {

  /** Same class as Catalyst's `raise_error` so a NULL comparison does not name the target. */
  def nullComparison(op: String): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "USER_RAISED_EXCEPTION",
      messageParameters = Map(
        "errorMessage" ->
          ("Python UDF transpiler: cannot compare NULL with operator " +
            s"`$op`; Python would raise TypeError here. Add an " +
            "`is not None` guard or filter NULLs upstream.")))
  }

  // Empty context: `QueryExecutionErrors.divideByZeroError` wraps `Array(context)`,
  // which is `Array(null)` here and later dies as `NoneType has no attribute contextType`.
  def divideByZero(): ArithmeticException = zeroDivisor("DIVIDE_BY_ZERO")

  def remainderByZero(): ArithmeticException = zeroDivisor("REMAINDER_BY_ZERO")

  def arithmeticOverflow(message: String): ArithmeticException = {
    QueryExecutionErrors.arithmeticOverflowError(message)
  }

  private def zeroDivisor(errorClass: String): ArithmeticException = {
    new SparkArithmeticException(
      errorClass = errorClass,
      messageParameters = Map("config" -> QuotingUtils.toSQLConf(SQLConf.ANSI_ENABLED.key)),
      context = Array.empty,
      summary = "")
  }
}

/**
 * A Python UDF lowered to Java source (see `pyspark.sql.transpile_java`).
 *
 * `body` is source, not bytecode: a class compiled on the driver cannot be
 * deserialized on an executor. [[doGenCode]] splices the same string into
 * whole-stage codegen; [[eval]] compiles it for the interpreted path. Two
 * generators would be two chances to disagree.
 *
 * Boxed ABI (`Long`, `Double`, `UTF8String`, `Boolean`, `byte[]`); Java `null`
 * is both SQL NULL and Python `None`. Every argument is a child, so unread
 * arguments are still evaluated -- matching the interpreted UDF.
 *
 * @param name the UDF's name, for display and to seed the generated method's name
 * @param body Java statements ending in a `return`, over the parameters named by `argNames`
 * @param argNames the generated method's parameter names, parallel to `children`
 * @param children the call's bound arguments, already cast to `inputTypes` by the transpiler
 * @param inputTypes one per child, each drawn from the boxed ABI above
 * @param dataType the method's return type, also from the boxed ABI
 */
case class TranspiledJavaUDF(
    name: String,
    body: String,
    argNames: Seq[String],
    children: Seq[Expression],
    inputTypes: Seq[DataType],
    dataType: DataType)
  extends Expression with ExpectsInputTypes with NonSQLExpression with UserDefinedExpression {
  // Both mixins: `PartitionPruning` / `EliminateSorts` match these traits, not
  // `expensive`, and by then `ConvertToCatalyst` has already replaced the PythonUDF.

  require(
    argNames.length == children.length && inputTypes.length == children.length,
    s"argNames (${argNames.length}), inputTypes (${inputTypes.length}) and children " +
      s"(${children.length}) must be parallel")

  // Same as ScalaUDF: duplicating this also duplicates a `rand()` child.
  override def expensive: Boolean = true

  // Not overridden: `deterministic` derives from the children. Pinning it true
  // would reintroduce the duplicate-draw problem this target avoids.

  override def nullable: Boolean = true

  override def prettyName: String = name

  /**
   * Not the inherited one: dumping `body` mangles EXPLAIN. Spelled as
   * `TranspiledJavaUDF` so a plan says which target fired; `prettyName` stays
   * the UDF name so column names match the interpreted UDF.
   */
  override def toString: String = s"TranspiledJavaUDF($name, ${children.mkString(", ")})"

  /**
   * `CodeGenerator.boxedType`, not a mapping of our own -- `doGenCode` uses
   * both in one block. The ABI check is a transpiler-bug tripwire.
   */
  private def boxedType(dt: DataType): String = {
    dt match {
      case LongType | DoubleType | BooleanType | BinaryType | _: StringType =>
        CodeGenerator.boxedType(dt)
      case other =>
        throw internalError(s"$prettyName was transpiled with unsupported Java UDF type $other")
    }
  }

  /** The transpiled function as a Java method declaration named `methodName`. */
  private def methodSource(methodName: String): String = {
    val params = argNames.zip(inputTypes).map { case (argName, dt) =>
      s"${boxedType(dt)} $argName"
    }.mkString(", ")
    s"""
       |private ${boxedType(dataType)} $methodName($params) {
       |$body
       |}
     """.stripMargin
  }

  // Java identifiers, from a name a user chose. `freshName` only appends a counter, so anything
  // that is not identifier-safe has to be replaced before it reaches the generated source.
  private def javaSafeName: String = {
    val cleaned = name.map(c => if (c.isLetterOrDigit || c == '_') c else '_')
    if (cleaned.headOption.exists(c => c.isLetter || c == '_')) cleaned else s"udf_$cleaned"
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evals = children.map(_.genCode(ctx))
    // Fresh per call site: `addNewFunction` keys on the name.
    val methodName = ctx.freshName(javaSafeName)
    // Not always `methodName`: a large class moves the method onto a nested instance.
    val callable = ctx.addNewFunction(methodName, methodSource(methodName))
    val argTerms = evals.zip(inputTypes).map { case (eval, dt) =>
      val term = ctx.freshName("arg")
      (term,
        code"""
              |${boxedType(dt)} $term = null;
              |${eval.code}
              |if (!${eval.isNull}) {
              |  $term = ${eval.value};
              |}
            """.stripMargin)
    }
    val resultTerm = ctx.freshName("result")
    ev.copy(code =
      code"""
            |${argTerms.map(_._2).reduceOption(_ + _).getOrElse(EmptyBlock)}
            |${boxedType(dataType)} $resultTerm = $callable(${argTerms.map(_._1).mkString(", ")});
            |boolean ${ev.isNull} = $resultTerm == null;
            |${CodeGenerator.javaType(dataType)} ${ev.value} =
            |  ${CodeGenerator.defaultValue(dataType)};
            |if (!${ev.isNull}) {
            |  ${ev.value} = $resultTerm;
            |}
          """.stripMargin)
  }

  /**
   * The same `body`, compiled for the interpreted path. Per JVM; `CodeGenerator.compile`
   * caches on the source.
   */
  @transient private lazy val invoker: TranspiledJavaUDFInvoker = {
    val methodName = "transpiledUDF"
    val casts = argNames.indices.zip(inputTypes).map { case (i, dt) =>
      s"(${boxedType(dt)}) args[$i]"
    }.mkString(", ")
    val codeBody =
      s"""
         |public SpecificTranspiledJavaUDF generate(Object[] references) {
         |  return new SpecificTranspiledJavaUDF();
         |}
         |
         |class SpecificTranspiledJavaUDF extends ${classOf[TranspiledJavaUDFInvoker].getName} {
         |  public Object call(Object[] args) {
         |    return $methodName($casts);
         |  }
         |${methodSource(methodName)}
         |}
       """.stripMargin
    val code = CodeFormatter.stripOverlappingComments(new CodeAndComment(codeBody, Map.empty))
    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(Array.empty).asInstanceOf[TranspiledJavaUDFInvoker]
  }

  override def eval(input: InternalRow): Any = {
    val args = new Array[Any](children.length)
    var i = 0
    while (i < children.length) {
      // Per row, never cached: a nondeterministic child owes each row its own value.
      args(i) = children(i).eval(input)
      i += 1
    }
    invoker.call(args)
  }

  /**
   * Compile now, while a failure can still be a skipped option. After
   * `ConvertToCatalyst` both paths compile this same source and there is no
   * fallback left.
   */
  def validate(): Unit = invoker

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): TranspiledJavaUDF = copy(children = newChildren)
}
