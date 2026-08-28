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

/**
 * The interface generated code implements so [[TranspiledJavaUDF.eval]] can call it. The
 * interpreted path needs a handle on the transpiled body, and a compiled class can only be reached
 * through a type that was on the classpath when it was compiled.
 */
abstract class TranspiledJavaUDFInvoker {
  def call(args: Array[Any]): Any
}

/**
 * Errors the generated body can raise, built here rather than in `TranspiledJavaUDFHelpers` because
 * the constructors take Scala collections that Java cannot supply readably.
 */
object TranspiledJavaUDFErrors {

  /**
   * The same error the Catalyst target raises for the same comparison. That target wraps every
   * ordering comparison in `raise_error(...)`, which yields `USER_RAISED_EXCEPTION`, so matching
   * the error class here keeps a NULL comparison from telling you which target lowered the UDF.
   */
  def nullComparison(op: String): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "USER_RAISED_EXCEPTION",
      messageParameters = Map(
        "errorMessage" ->
          ("Python UDF transpiler: cannot compare NULL with operator " +
            s"`$op`; Python would raise TypeError here. Add an " +
            "`is not None` guard or filter NULLs upstream.")))
  }

  // The same error classes `QueryExecutionErrors.divideByZeroError` / `remainderByZeroError`
  // produce, but built with an EMPTY context array rather than theirs, which is `Array(context)`
  // and so becomes `Array(null)` for a caller with no context to give. A null inside that array
  // fails later with `NoneType object has no attribute contextType` instead of reporting the
  // arithmetic error -- the generated body is not a SQL fragment, so it has no QueryContext.
  def divideByZero(): ArithmeticException = zeroDivisor("DIVIDE_BY_ZERO")

  def remainderByZero(): ArithmeticException = zeroDivisor("REMAINDER_BY_ZERO")

  /**
   * [[ARITHMETIC_OVERFLOW]] for an overflow the helpers detect themselves rather than catching from
   * `Math.*Exact`. Unlike the zero-divisor errors above, this one takes a null context by design --
   * it is its own default -- so it can simply be delegated to.
   */
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
 * A Python UDF lowered to Java source by the `java` transpiler (see `pyspark.sql.transpile_java`).
 *
 * `body` is the statements of a single Java method -- the transpiled function -- which reads its
 * arguments through `argNames` and returns the result. It is source, not bytecode: a class compiled
 * on the driver could not be deserialized on an executor, whose class loader never defined it, so
 * what travels with the plan is the text, compiled once per JVM the way the rest of codegen is.
 *
 * That text is used twice, and deliberately only written once. [[doGenCode]] splices it into
 * whole-stage codegen as a method of the generated class, and [[eval]] compiles it into a small
 * wrapper for the interpreted path -- reached under `NO_CODEGEN`, and whenever a projection is
 * interpreted rather than generated. Not by `ConstantFolding`, which needs `foldable`, deliberately
 * not overridden here (see `deterministic` below for why deriving it from the children is a trap).
 * Two call sites over one string cannot disagree about what the UDF computes; two generators
 * would.
 *
 * The ABI is boxed -- `Long`, `Double`, `UTF8String`, `Boolean`, `byte[]` -- with Java `null`
 * standing for both SQL NULL and Python `None`, which is what makes the null mapping exact rather
 * than a translation. Unboxing the hot path is left for later; the win being claimed here is
 * against a Python worker, not against hand-written codegen.
 *
 * Unlike a Catalyst lowering, every argument is a child and so every argument is evaluated, before
 * the call and whether or not the body reads it. That is what the interpreted UDF does too, which
 * evaluates its arguments in a projection feeding the worker, and it is why a body reading a
 * parameter many times still evaluates it once: the repeats are reads of a Java local, inside
 * `body`, and nothing in the plan sees them.
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
  // Both mixins are load-bearing, not decoration. Rules that mean "an opaque user function" test
  // for these traits rather than for `expensive`, which only `PushPredicateThroughNonJoin` reads:
  // `PartitionPruning.isScanCostBoundExpression` and `EliminateSorts.isOrderIrrelevantAggs` both
  // match on `NonSQLExpression | UserDefinedExpression`, and `ConvertToCatalyst` runs in the first
  // optimizer batch, so by the time those rules look, the PythonUDF they would have matched is gone
  // and this node is what is there.

  require(
    argNames.length == children.length && inputTypes.length == children.length,
    s"argNames (${argNames.length}), inputTypes (${inputTypes.length}) and children " +
      s"(${children.length}) must be parallel")

  // A transpiled body can compute anything a Python function can, so it is worth no more to the
  // optimizer than a ScalaUDF is. Rules read this to decide whether duplicating an expression is
  // free; duplicating this one would also duplicate its arguments, and a second copy of a `rand()`
  // argument is a second draw the body was never meant to see.
  override def expensive: Boolean = true

  // Deliberately NOT overridden: `deterministic` derives from the children, which is what stops a
  // rule from copying a call that carries a draw. Pinning it true here would reintroduce exactly
  // the duplicate-evaluation problem this target avoids by construction.

  // Python can return None from any path, and the boxed ABI has no way to promise otherwise.
  override def nullable: Boolean = true

  override def prettyName: String = name

  /**
   * Deliberately not the inherited one, which prints every case-class field. `body` is a
   * multi-line Java method, and dumping it here puts the whole of it -- newlines included --
   * into every plan string, which mangles `EXPLAIN`'s line structure and buries the rest of the
   * plan. The generated source is still reachable when it is what you want, through the codegen
   * log (`spark.sql.codegen.logLevel`).
   *
   * The node name is spelled out rather than left to `prettyName` so a plan says which target
   * lowered the UDF: a Catalyst lowering shows up as ordinary expressions, and someone reading
   * `EXPLAIN` after enabling `catalyst,java` wants to see which one they got. `prettyName` stays
   * the bare UDF name so `sql` and generated column names match the interpreted UDF's.
   */
  override def toString: String = s"TranspiledJavaUDF($name, ${children.mkString(", ")})"

  /**
   * The boxed Java type for an ABI type, from `CodeGenerator` rather than a mapping of its own.
   *
   * Delegating matters because `doGenCode` uses this for the result variable and
   * `CodeGenerator.javaType` for `ev.value` in the same block, so the two have to agree; and
   * because Spark does evolve that mapping (`javaType` now answers `BinaryView` for some types),
   * and a hand-rolled copy would drift silently into generated Java the helpers cannot accept. The
   * ABI check stays: anything outside the five types the transpiler casts into is a transpiler bug,
   * and worth an error here rather than a puzzle in the generated source.
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
    // A fresh name per call site: `addNewFunction` keys on the name, so two UDFs that happen to
    // share one would otherwise have the first's body silently replaced by the second's.
    val methodName = ctx.freshName(javaSafeName)
    // The returned string is what to call, and is not always `methodName`: once the generated class
    // outgrows GENERATED_CLASS_SIZE_THRESHOLD the method is moved to a nested class and this comes
    // back as `nestedClassInstance.methodName`.
    val callable = ctx.addNewFunction(methodName, methodSource(methodName))

    // Box each argument, spelled out rather than done with a conditional expression: mixing `null`
    // and a primitive in a ternary leans on the JLS's boxing rules for its result type, and being
    // explicit costs nothing here.
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
   * The same `body`, compiled for the interpreted path. Per JVM and lazily, so the source is what
   * crosses the wire; `CodeGenerator.compile` caches on the source, so repeated calls and repeated
   * copies of this expression share one class.
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
   * Compile the body now, so a body that does not compile is a fallback rather than an outage.
   *
   * Called while the option is still being built, which is the last moment a failure can be turned
   * into "decline this option". Once `ConvertToCatalyst` has substituted the option it discards
   * `pythonUDFExpr`, and after that a compile failure has nowhere to go: whole-stage codegen falls
   * back to the interpreted path, which compiles the SAME source through `invoker` and fails
   * identically. Forcing `invoker` here also warms `CodeGenerator`'s cache, which is keyed on the
   * source, so the executors' compile is not duplicated work on the driver.
   */
  def validate(): Unit = invoker

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): TranspiledJavaUDF = copy(children = newChildren)
}
